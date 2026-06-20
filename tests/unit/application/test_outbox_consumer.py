"""
OutboxConsumer (§12.9, FR-5; T024, фаза 4): диспетчеризация
notify/catchup/restart_pipeline, пометка done/failed, аналитика сбоя.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from angarion.adapters.memory.storage import MemoryAnalytics, MemoryCommandOutbox
from angarion.application.outbox_consumer import (
    COMMAND_FAILED,
    NOTIFY_FAILED,
    OutboxConsumer,
)
from angarion.domain.models import (
    CommandKind,
    CommandStatus,
    DeliveryReceipt,
    OutboundRecord,
)
from angarion.testing.factories import make_outbound

pytestmark = pytest.mark.asyncio


class FakeSink:
    """``SinkPort`` для тестов: запоминает отправки или падает."""

    def __init__(self, *, fail: bool = False) -> None:
        self.sent: list[OutboundRecord] = []
        self._fail = fail

    async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
        if self._fail:
            msg_text = 'sink boom'
            raise RuntimeError(msg_text)
        self.sent.append(msg)
        return DeliveryReceipt(external_id='ext-1', delivered_at=datetime.now(UTC))


def _make_consumer(
    outbox: MemoryCommandOutbox,
    analytics: MemoryAnalytics,
    *,
    sink: FakeSink | None = None,
    catchups: list[str] | None = None,
    catchup_fail: bool = False,
    restarts: list[bool] | None = None,
) -> OutboxConsumer:
    async def catchup(source_key: str) -> None:
        if catchup_fail:
            raise KeyError(source_key)
        if catchups is not None:
            catchups.append(source_key)

    def request_restart() -> None:
        if restarts is not None:
            restarts.append(True)

    return OutboxConsumer(
        command_outbox=outbox,
        sink=sink or FakeSink(),
        analytics=analytics,
        catchup=catchup,
        request_restart=request_restart,
        poll_seconds=0.0,
    )


async def test_notify_dispatch_sends_and_marks_done() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    sink = FakeSink()
    message = make_outbound()
    command = await outbox.put(
        CommandKind.NOTIFY, payload={'record': message.model_dump(mode='json')}
    )
    consumer = _make_consumer(outbox, analytics, sink=sink)
    processed = await consumer.poll_once()
    assert processed == 1
    assert sink.sent[0].idempotency_key == message.idempotency_key
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.DONE
    assert stored.result == 'sent:ext-1'


async def test_notify_failure_marks_failed_and_records_notify_failed() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await outbox.put(
        CommandKind.NOTIFY,
        payload={'record': make_outbound().model_dump(mode='json')},
    )
    consumer = _make_consumer(outbox, analytics, sink=FakeSink(fail=True))
    await consumer.poll_once()
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.FAILED
    assert 'sink boom' in (stored.error or '')
    failures = await analytics.recent(kind=NOTIFY_FAILED)
    assert failures
    assert failures[0].payload['command_uid'] == str(command.uid)


async def test_catchup_dispatch_invokes_callback() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    catchups: list[str] = []
    await outbox.put(CommandKind.CATCHUP, payload={'source_key': 'memory:a:-1'})
    consumer = _make_consumer(outbox, analytics, catchups=catchups)
    await consumer.poll_once()
    assert catchups == ['memory:a:-1']


async def test_catchup_failure_records_command_failed() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await outbox.put(
        CommandKind.CATCHUP, payload={'source_key': 'memory:a:-1'}
    )
    consumer = _make_consumer(outbox, analytics, catchup_fail=True)
    await consumer.poll_once()
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.FAILED
    assert (await analytics.recent(kind=COMMAND_FAILED))[0].payload[
        'command_kind'
    ] == 'catchup'


async def test_restart_dispatch_requests_restart_and_marks_done() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    restarts: list[bool] = []
    command = await outbox.put(CommandKind.RESTART_PIPELINE)
    consumer = _make_consumer(outbox, analytics, restarts=restarts)
    await consumer.poll_once()
    assert restarts == [True]
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.DONE


async def test_poll_once_empty_returns_zero() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    consumer = _make_consumer(outbox, analytics)
    assert await consumer.poll_once() == 0
