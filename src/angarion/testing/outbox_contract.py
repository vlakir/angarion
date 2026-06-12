"""Контрактный набор ``OutboxPort`` (C-9 спеки T002; FR-6, SC-5)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from angarion.domain.models import DeliveryReceipt, OutboxStatus
from angarion.testing.factories import FAR_FUTURE, LONG_AGO, NOW, make_outbound

if TYPE_CHECKING:
    from angarion.domain.ports import OutboxPort


class OutboxContract:
    """
    Поведенческая спецификация outbox исходящих (C-9): журнал с
    insert-if-absent по ``idempotency_key`` (идемпотентность выхода),
    выборка due-записей FIFO, переходы pending → sent / failed,
    ретеншн-очистка терминальных записей ``prune()`` (A-7).

    Реализация подключается переопределением фикстуры ``outbox``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def outbox(self) -> OutboxPort:
        raise NotImplementedError

    async def test_put_new_key_pending(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        assert await outbox.put(msg) is True
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.msg == msg
        assert record.status is OutboxStatus.PENDING
        assert record.attempts == 0
        assert record.created_at.tzinfo is not None
        assert record.next_attempt_at.tzinfo is not None
        assert record.finished_at is None
        assert record.receipt is None
        assert record.last_error is None

    async def test_put_duplicate_false_and_unchanged(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        await outbox.put(msg)
        await outbox.reschedule(
            msg.idempotency_key, not_before=NOW, error='первая попытка'
        )
        assert await outbox.put(msg) is False
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.attempts == 1  # повторный put не сбросил состояние

    async def test_put_stores_observability_context(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        event_uid = uuid4()
        await outbox.put(msg, pipeline='digest', event_uid=event_uid)
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.pipeline == 'digest'
        assert record.event_uid == event_uid

    async def test_get_unknown_none(self, outbox: OutboxPort) -> None:
        assert await outbox.get('no-such-key') is None

    async def test_due_fifo_with_limit(self, outbox: OutboxPort) -> None:
        first = make_outbound(idempotency_key='k1')
        second = make_outbound(idempotency_key='k2')
        await outbox.put(first)
        await outbox.put(second)
        due = await outbox.due()
        assert [r.msg.idempotency_key for r in due] == ['k1', 'k2']
        limited = await outbox.due(limit=1)
        assert [r.msg.idempotency_key for r in limited] == ['k1']

    async def test_due_excludes_future_rescheduled(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        await outbox.put(msg)
        await outbox.reschedule(
            msg.idempotency_key, not_before=FAR_FUTURE, error='позже'
        )
        assert await outbox.due() == []

    async def test_reschedule_increments_attempts(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        await outbox.put(msg)
        await outbox.reschedule(msg.idempotency_key, not_before=LONG_AGO, error='boom')
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.PENDING
        assert record.attempts == 1
        assert record.last_error == 'boom'
        assert [r.msg.idempotency_key for r in await outbox.due()] == [
            msg.idempotency_key
        ]

    async def test_mark_sent_terminal_and_idempotent(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        await outbox.put(msg)
        receipt = DeliveryReceipt(external_id='777', delivered_at=NOW)
        await outbox.mark_sent(msg.idempotency_key, receipt)
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.SENT
        assert record.receipt == receipt
        assert record.finished_at is not None
        assert await outbox.due() == []
        # повторный mark_sent и reschedule после sent — no-op
        await outbox.mark_sent(
            msg.idempotency_key,
            DeliveryReceipt(external_id='888', delivered_at=NOW),
        )
        await outbox.reschedule(
            msg.idempotency_key, not_before=LONG_AGO, error='поздно'
        )
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.receipt == receipt
        assert record.attempts == 0

    async def test_mark_failed_terminal(self, outbox: OutboxPort) -> None:
        msg = make_outbound()
        await outbox.put(msg)
        await outbox.mark_failed(msg.idempotency_key, 'исчерпаны ретраи')
        record = await outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.FAILED
        assert record.last_error == 'исчерпаны ретраи'
        assert record.finished_at is not None
        assert await outbox.due() == []

    async def test_transitions_on_unknown_key_are_noop(
        self, outbox: OutboxPort
    ) -> None:
        await outbox.mark_sent(
            'ghost', DeliveryReceipt(external_id=None, delivered_at=NOW)
        )
        await outbox.reschedule('ghost', not_before=NOW, error='x')
        await outbox.mark_failed('ghost', 'x')
        assert await outbox.get('ghost') is None

    async def test_prune_removes_only_finished(self, outbox: OutboxPort) -> None:
        sent = make_outbound(idempotency_key='sent')
        failed = make_outbound(idempotency_key='failed')
        pending = make_outbound(idempotency_key='pending')
        for msg in (sent, failed, pending):
            await outbox.put(msg)
        await outbox.mark_sent(
            'sent', DeliveryReceipt(external_id='1', delivered_at=NOW)
        )
        await outbox.mark_failed('failed', 'boom')
        terminal_count = 2  # sent + failed; pending не прунится
        assert await outbox.prune(older_than=LONG_AGO) == 0
        assert await outbox.prune(older_than=FAR_FUTURE) == terminal_count
        assert await outbox.get('sent') is None
        assert await outbox.get('failed') is None
        assert await outbox.get('pending') is not None
