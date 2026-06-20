"""
DeliveryWorker (C-9 спеки T002): доставка исходящих из outbox —
send → mark_sent, retry с backoff, терминальный failed.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from app_factories import make_outbound

from angarion.adapters.memory.sink import MemorySink
from angarion.adapters.memory.storage import MemoryAnalytics, MemoryOutbox
from angarion.application.delivery import DeliveryWorker
from angarion.domain.errors import DeliveryError
from angarion.domain.models import DeliveryReceipt, OutboundRecord, OutboxStatus


class FlakySink(MemorySink):
    """Sink, падающий на заданных по счёту вызовах send."""

    def __init__(self, fail_on: set[int]) -> None:
        super().__init__()
        self._calls = 0
        self._fail_on = fail_on

    async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
        self._calls += 1
        if self._calls in self._fail_on:
            error = 'сеть моргнула'
            raise DeliveryError(error)
        return await super().send(msg)


class DeliveryHarness:
    def __init__(
        self,
        *,
        sink: MemorySink | None = None,
        max_retries: int = 5,
        backoff_base: float = 0.0,
        backoff_cap: float = 60.0,
        poll_interval: float = 0.001,
        shutdown_drain_seconds: float = 5.0,
    ) -> None:
        self.outbox = MemoryOutbox()
        self.sink = sink if sink is not None else MemorySink()
        self.analytics = MemoryAnalytics()
        self.worker = DeliveryWorker(
            outbox=self.outbox,
            sink=self.sink,
            analytics=self.analytics,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
            poll_interval=poll_interval,
            shutdown_drain_seconds=shutdown_drain_seconds,
        )

    async def kinds(self) -> list[str]:
        return [event.kind for event in await self.analytics.recent(limit=100)]


class TestDelivery:
    async def test_delivers_pending_and_marks_sent(self) -> None:
        harness = DeliveryHarness()
        msg = make_outbound()
        record_uid = uuid4()
        await harness.outbox.put(msg, pipeline='digest', record_uid=record_uid)
        assert await harness.worker.drain_due() == 1
        assert harness.sink.sent == [msg]
        record = await harness.outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.SENT
        assert record.receipt is not None
        delivered = await harness.analytics.recent(kind='delivered')
        assert len(delivered) == 1
        assert delivered[0].pipeline == 'digest'
        assert delivered[0].record_uid == record_uid

    async def test_nothing_due(self) -> None:
        harness = DeliveryHarness()
        assert await harness.worker.deliver_next() is False
        assert await harness.worker.drain_due() == 0

    async def test_failed_send_retried_until_delivered(self) -> None:
        """C-9: сбой доставки больше не теряет сообщение — доезжает ретраем."""
        harness = DeliveryHarness(sink=FlakySink(fail_on={1}))
        msg = make_outbound()
        await harness.outbox.put(msg)
        assert await harness.worker.drain_due() == 2  # сбой + успешный ретрай
        assert harness.sink.sent == [msg]
        record = await harness.outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.SENT
        assert record.attempts == 1
        assert (await harness.kinds()).count('delivered') == 1

    async def test_failure_schedules_backoff(self) -> None:
        harness = DeliveryHarness(sink=FlakySink(fail_on={1, 2}), backoff_base=4.0)
        msg = make_outbound()
        await harness.outbox.put(msg)
        before = datetime.now(UTC)
        assert await harness.worker.deliver_next() is True
        record = await harness.outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.PENDING
        assert record.attempts == 1
        assert record.last_error is not None
        assert 'DeliveryError' in record.last_error
        delay = (record.next_attempt_at - before).total_seconds()
        assert 4.0 <= delay < 5.0
        assert await harness.worker.deliver_next() is False  # ещё не срок

    async def test_exhausted_attempts_mark_failed(self) -> None:
        harness = DeliveryHarness(sink=FlakySink(fail_on={1, 2, 3, 4}), max_retries=2)
        msg = make_outbound()
        await harness.outbox.put(msg)
        assert await harness.worker.drain_due() == 3  # 1 попытка + 2 ретрая
        record = await harness.outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.FAILED
        assert record.last_error is not None
        assert record.finished_at is not None
        assert harness.sink.sent == []
        kinds = await harness.kinds()
        assert 'delivery_failed' in kinds
        assert 'delivered' not in kinds


class TestRunLifecycle:
    async def test_run_delivers_then_idles(self) -> None:
        harness = DeliveryHarness()
        await harness.outbox.put(make_outbound())
        task = asyncio.create_task(harness.worker.run())
        while not harness.sink.sent:
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(harness.sink.sent) == 1

    async def test_run_completes_current_delivery_on_cancel(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class SlowSink(MemorySink):
            async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
                started.set()
                await release.wait()
                return await super().send(msg)

        harness = DeliveryHarness(sink=SlowSink())
        msg = make_outbound()
        await harness.outbox.put(msg)
        task = asyncio.create_task(harness.worker.run())
        await started.wait()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        record = await harness.outbox.get(msg.idempotency_key)
        assert record is not None
        assert record.status is OutboxStatus.SENT  # дообработано

    async def test_run_bounded_stop_when_sink_hangs(self) -> None:
        """T031: залипший в send (throttle/FloodWait) sink не виснет стоп."""
        started = asyncio.Event()

        class HungSink(MemorySink):
            async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
                started.set()
                await asyncio.sleep(100)  # «залип» в долгом ожидании
                return await super().send(msg)

        harness = DeliveryHarness(sink=HungSink(), shutdown_drain_seconds=0.05)
        await harness.outbox.put(make_outbound())
        task = asyncio.create_task(harness.worker.run())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1.0)  # не 100 c

    async def test_run_cancel_while_idle(self) -> None:
        harness = DeliveryHarness(poll_interval=60.0)
        task = asyncio.create_task(harness.worker.run())
        await asyncio.sleep(0.01)  # worker ушёл в poll-sleep
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
