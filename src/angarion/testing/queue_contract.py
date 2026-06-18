"""Контрактный набор ``EventQueuePort`` (§5 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from angarion.domain.models import QueueDepth
from angarion.testing.factories import NOW, make_envelope

if TYPE_CHECKING:
    from angarion.domain.ports import EventQueuePort


class EventQueueContract:
    """
    Поведенческая спецификация очереди: FIFO, at-least-once,
    ack/nack/recover, прозрачный round-trip envelope (включая
    ``not_before`` — C-8).

    Реализация подключается переопределением фикстуры ``queue``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def queue(self) -> EventQueuePort:
        raise NotImplementedError

    async def test_put_get_fifo(self, queue: EventQueuePort) -> None:
        first = make_envelope(pipeline='first')
        second = make_envelope(pipeline='second')
        await queue.put(first)
        await queue.put(second)
        assert (await queue.get()).envelope == first
        assert (await queue.get()).envelope == second

    async def test_roundtrip_preserves_attempt_and_not_before(
        self, queue: EventQueuePort
    ) -> None:
        not_before = NOW + timedelta(seconds=8)
        attempt = 3
        envelope = make_envelope(attempt=attempt, not_before=not_before)
        await queue.put(envelope)
        item = await queue.get()
        assert item.envelope == envelope
        assert item.envelope.attempt == attempt
        assert item.envelope.not_before == not_before

    async def test_get_waits_for_put(self, queue: EventQueuePort) -> None:
        task = asyncio.create_task(queue.get())
        await asyncio.sleep(0.01)
        assert not task.done()
        envelope = make_envelope()
        await queue.put(envelope)
        item = await asyncio.wait_for(task, timeout=1.0)
        assert item.envelope == envelope

    async def test_depth_tracks_pending_and_unacked(
        self, queue: EventQueuePort
    ) -> None:
        assert await queue.depth() == QueueDepth(pending=0, unacked=0)
        await queue.put(make_envelope())
        assert await queue.depth() == QueueDepth(pending=1, unacked=0)
        item = await queue.get()
        assert await queue.depth() == QueueDepth(pending=0, unacked=1)
        await queue.ack(item)
        assert await queue.depth() == QueueDepth(pending=0, unacked=0)

    async def test_ack_is_idempotent(self, queue: EventQueuePort) -> None:
        await queue.put(make_envelope())
        item = await queue.get()
        await queue.ack(item)
        await queue.ack(item)
        assert await queue.depth() == QueueDepth(pending=0, unacked=0)

    async def test_nack_returns_item_to_queue(self, queue: EventQueuePort) -> None:
        envelope = make_envelope()
        await queue.put(envelope)
        item = await queue.get()
        await queue.nack(item)
        assert await queue.depth() == QueueDepth(pending=1, unacked=0)
        assert (await queue.get()).envelope == envelope

    async def test_nack_after_ack_is_noop(self, queue: EventQueuePort) -> None:
        await queue.put(make_envelope())
        item = await queue.get()
        await queue.ack(item)
        await queue.nack(item)
        assert await queue.depth() == QueueDepth(pending=0, unacked=0)

    async def test_recover_requeues_unacked(self, queue: EventQueuePort) -> None:
        first = make_envelope(pipeline='first')
        second = make_envelope(pipeline='second')
        await queue.put(first)
        await queue.put(second)
        unacked_count = 2
        await queue.get()
        await queue.get()
        assert await queue.recover() == unacked_count
        assert await queue.depth() == QueueDepth(pending=2, unacked=0)
        recovered = [(await queue.get()).envelope for _ in range(2)]
        assert sorted(recovered, key=lambda e: e.pipeline) == [first, second]

    async def test_recover_with_no_unacked_returns_zero(
        self, queue: EventQueuePort
    ) -> None:
        assert await queue.recover() == 0

    async def test_purge_acked_keeps_pending_and_unacked_intact(
        self, queue: EventQueuePort
    ) -> None:
        """
        Ретеншн acked-строк (§17.3, T016): ``purge_acked`` чистит только
        подтверждённые записи и НЕ трогает pending/unacked — depth не
        меняется, unacked не теряется и не выдаётся повторно.
        """
        acked = make_envelope(pipeline='acked')
        await queue.put(acked)
        await queue.ack(await queue.get())  # подтверждена → кандидат на чистку
        await queue.put(make_envelope(pipeline='pending'))  # ждёт в очереди
        inflight = await queue.get()  # взята, не подтверждена → unacked
        await queue.put(make_envelope(pipeline='tail'))

        deleted = await queue.purge_acked(keep_latest=0)

        assert deleted >= 0
        assert await queue.depth() == QueueDepth(pending=1, unacked=1)
        await queue.ack(inflight)
        assert await queue.depth() == QueueDepth(pending=1, unacked=0)

    async def test_purge_acked_on_empty_queue_is_zero(
        self, queue: EventQueuePort
    ) -> None:
        assert await queue.purge_acked(keep_latest=0) == 0
