"""
PipelineWorker (§6.3 в редакции C-9, §8 ТЗ; FR-9–11): инвариант
«outbound зафиксированы в outbox до ack», DROP, retry/defer-to-tail
(C-8), DLQ, ScopedStateStore. Доставкой занимается DeliveryWorker
(test_delivery.py).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from app_factories import make_context, make_envelope, make_event, make_target

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryDeadLetters,
    MemoryOutbox,
    MemoryStateStore,
)
from angarion.application import worker as worker_module
from angarion.application.processors import (
    FunctionProcessor,
    ProcessorFn,
    passthrough,
)
from angarion.application.worker import (
    PipelineBinding,
    PipelineWorker,
    ScopedStateStore,
)
from angarion.domain.errors import ProcessingError
from angarion.domain.keys import make_idempotency_key
from angarion.domain.models import (
    AnalyticsEvent,
    InboundEvent,
    OutboxStatus,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    QueueEnvelope,
    QueueItem,
    TargetSpec,
    Verdict,
)


class RecordingQueue(MemoryQueue):
    """MemoryQueue, журналирующая порядок put/ack для проверки инвариантов."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[str] = []

    async def put(self, item: QueueEnvelope) -> None:
        self.calls.append('put')
        await super().put(item)

    async def ack(self, item: QueueItem) -> None:
        self.calls.append('ack')
        await super().ack(item)


async def boom(
    event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    msg = 'boom'
    raise ProcessingError(msg)


async def drop(
    event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    return ProcessingResult(verdict=Verdict.DROP, note='подавлено')


class WorkerHarness:
    """Worker на InMemory-портах с одним пайплайном digest."""

    def __init__(
        self,
        fn: ProcessorFn,
        *,
        targets: list[TargetSpec] | None = None,
        max_retries: int = 5,
        backoff_base: float = 0.0,
        backoff_cap: float = 60.0,
    ) -> None:
        self.queue = RecordingQueue()
        self.outbox = MemoryOutbox()
        self.analytics = MemoryAnalytics()
        self.dead_letters = MemoryDeadLetters()
        self.state = MemoryStateStore()
        binding = PipelineBinding(
            processor=FunctionProcessor(name='test', fn=fn),
            ctx=make_context(targets=targets),
        )
        self.worker = PipelineWorker(
            queue=self.queue,
            outbox=self.outbox,
            analytics=self.analytics,
            dead_letters=self.dead_letters,
            state=self.state,
            pipelines={'digest': binding},
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_cap=backoff_cap,
        )

    async def kinds(self) -> list[str]:
        return [event.kind for event in await self.analytics.recent(limit=100)]


class TestStaging:
    async def test_deliver_stages_outbound_before_ack(self) -> None:
        """C-9: outbound фиксируется в outbox строго до ack."""
        harness = WorkerHarness(passthrough)
        envelope = make_envelope()
        await harness.queue.put(envelope)
        await harness.worker.process_one()
        target = make_target().target
        key = make_idempotency_key('digest', envelope.event, target, 0)
        record = await harness.outbox.get(key)
        assert record is not None
        assert record.status is OutboxStatus.PENDING
        assert record.msg.text == 'hello'
        assert record.pipeline == 'digest'
        assert record.event_uid == envelope.event.uid
        depth = await harness.queue.depth()
        assert (depth.pending, depth.unacked) == (0, 0)
        kinds = await harness.kinds()
        assert 'processed' in kinds
        assert 'delivered' not in kinds  # доставка — DeliveryWorker

    async def test_drop_stages_nothing(self) -> None:
        harness = WorkerHarness(drop)
        await harness.queue.put(make_envelope())
        await harness.worker.process_one()
        assert await harness.outbox.due() == []
        kinds = await harness.kinds()
        assert 'dropped' in kinds
        assert (await harness.queue.depth()).unacked == 0

    async def test_reprocessing_does_not_duplicate_outbox(self) -> None:
        """At-least-once: повторная обработка гасится insert-if-absent."""
        harness = WorkerHarness(passthrough)
        await harness.queue.put(make_envelope())
        await harness.queue.put(make_envelope())
        await harness.worker.process_one()
        await harness.worker.process_one()
        assert len(await harness.outbox.due()) == 1
        assert (await harness.kinds()).count('processed') == 2

    async def test_processor_events_recorded(self) -> None:
        extra = AnalyticsEvent(
            uid=make_envelope().event.uid, kind='custom_metric', at=datetime.now(UTC)
        )

        async def with_events(
            event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
        ) -> ProcessingResult:
            return ProcessingResult(verdict=Verdict.DROP, events=[extra])

        harness = WorkerHarness(with_events)
        await harness.queue.put(make_envelope())
        await harness.worker.process_one()
        assert 'custom_metric' in await harness.kinds()


class TestRetry:
    async def test_retry_put_strictly_before_ack(self) -> None:
        """C-8: падение между put и ack даёт дубль, не потерю."""
        harness = WorkerHarness(boom)
        await harness.queue.put(make_envelope())
        await harness.worker.process_one()
        assert harness.queue.calls == ['put', 'put', 'ack']

    @pytest.mark.parametrize(
        ('attempt', 'base', 'cap', 'expected_delay'),
        [(0, 4.0, 60.0, 4.0), (3, 1.0, 60.0, 8.0), (10, 1.0, 60.0, 60.0)],
    )
    async def test_backoff_exponential_with_cap(
        self, attempt: int, base: float, cap: float, expected_delay: float
    ) -> None:
        harness = WorkerHarness(boom, backoff_base=base, backoff_cap=cap, max_retries=99)
        await harness.queue.put(make_envelope(attempt=attempt))
        before = datetime.now(UTC)
        await harness.worker.process_one()
        item = await harness.queue.get()
        retried = item.envelope
        assert retried.attempt == attempt + 1
        assert retried.not_before is not None
        delay = (retried.not_before - before).total_seconds()
        assert expected_delay <= delay < expected_delay + 1.0

    async def test_exhausted_retries_go_to_dlq(self) -> None:
        """§8: после max_retries — ack + failed + полный дамп envelope в DLQ."""
        harness = WorkerHarness(boom, max_retries=2)
        await harness.queue.put(make_envelope())
        for _ in range(3):
            await harness.worker.process_one()
        letters = await harness.dead_letters.list()
        assert len(letters) == 1
        assert letters[0].envelope.attempt == 2
        assert 'ProcessingError' in letters[0].error
        assert 'boom' in letters[0].error
        depth = await harness.queue.depth()
        assert (depth.pending, depth.unacked) == (0, 0)
        assert 'failed' in await harness.kinds()

    async def test_unknown_pipeline_goes_to_dlq(self) -> None:
        harness = WorkerHarness(passthrough)
        await harness.queue.put(make_envelope(pipeline='ghost'))
        await harness.worker.process_one()
        letters = await harness.dead_letters.list()
        assert len(letters) == 1
        assert 'ghost' in letters[0].error
        assert await harness.outbox.due() == []


class TestDefer:
    async def test_early_envelope_returned_to_tail(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """C-8: envelope с not_before в будущем — put в хвост + ack + sleep."""
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr(worker_module.asyncio, 'sleep', fake_sleep)
        harness = WorkerHarness(passthrough)
        not_before = datetime.now(UTC) + timedelta(seconds=0.5)
        await harness.queue.put(make_envelope(not_before=not_before))
        await harness.worker.process_one()
        assert await harness.outbox.due() == []  # процессор не вызывался
        assert harness.queue.calls == ['put', 'put', 'ack']
        assert (await harness.queue.depth()).pending == 1
        assert len(sleeps) == 1
        assert 0.0 < sleeps[0] <= 0.5

    async def test_defer_sleep_capped_at_one_second(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr(worker_module.asyncio, 'sleep', fake_sleep)
        harness = WorkerHarness(passthrough)
        not_before = datetime.now(UTC) + timedelta(hours=1)
        await harness.queue.put(make_envelope(not_before=not_before))
        await harness.worker.process_one()
        assert sleeps == [1.0]

    async def test_ripe_envelope_processed_normally(self) -> None:
        """not_before в прошлом — обычная обработка без defer."""
        harness = WorkerHarness(passthrough)
        not_before = datetime.now(UTC) - timedelta(seconds=5)
        await harness.queue.put(make_envelope(not_before=not_before))
        await harness.worker.process_one()
        assert len(await harness.outbox.due()) == 1


class TestProcessorServicesWiring:
    async def test_idempotency_key_bound_to_pipeline(self) -> None:
        captured: list[ProcessorServices] = []

        async def capture(
            event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
        ) -> ProcessingResult:
            captured.append(svc)
            return ProcessingResult(verdict=Verdict.DROP)

        harness = WorkerHarness(capture)
        envelope = make_envelope()
        await harness.queue.put(envelope)
        await harness.worker.process_one()
        svc = captured[0]
        target = make_target().target
        expected = make_idempotency_key('digest', envelope.event, target, 0)
        assert svc.make_idempotency_key(envelope.event, target, 0) == expected

    async def test_state_scoped_to_pipeline(self) -> None:
        async def stateful(
            event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
        ) -> ProcessingResult:
            await svc.state.set('seen', event.dedup_key)
            return ProcessingResult(verdict=Verdict.DROP)

        harness = WorkerHarness(stateful)
        envelope = make_envelope()
        await harness.queue.put(envelope)
        await harness.worker.process_one()
        assert await harness.state.get('digest', 'seen') == envelope.event.dedup_key
        assert await harness.state.get('other', 'seen') is None


class TestScopedStateStore:
    async def test_namespaced_roundtrip(self) -> None:
        store = MemoryStateStore()
        digest = ScopedStateStore(store, 'digest')
        audit = ScopedStateStore(store, 'audit')
        await digest.set('k', 'v')
        assert await digest.get('k') == 'v'
        assert await audit.get('k') is None
        assert await store.get('digest', 'k') == 'v'

    async def test_keys_and_delete(self) -> None:
        store = MemoryStateStore()
        scoped = ScopedStateStore(store, 'digest')
        await scoped.set('a:1', 'x')
        await scoped.set('a:2', 'y')
        await scoped.set('b:1', 'z')
        assert await scoped.keys(prefix='a:') == ['a:1', 'a:2']
        assert await scoped.keys() == ['a:1', 'a:2', 'b:1']
        await scoped.delete('a:1')
        await scoped.delete('a:1')  # повторное удаление — no-op
        assert await scoped.keys(prefix='a:') == ['a:2']


class TestRunLifecycle:
    async def test_run_completes_current_item_on_cancel(self) -> None:
        """Graceful-остановка (plan 2.9): текущий item дообрабатывается."""
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow(
            event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
        ) -> ProcessingResult:
            started.set()
            await release.wait()
            return ProcessingResult(verdict=Verdict.DROP)

        harness = WorkerHarness(slow)
        await harness.queue.put(make_envelope())
        task = asyncio.create_task(harness.worker.run())
        await started.wait()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        depth = await harness.queue.depth()
        assert (depth.pending, depth.unacked) == (0, 0)
        assert 'dropped' in await harness.kinds()

    async def test_run_cancel_while_idle(self) -> None:
        harness = WorkerHarness(passthrough)
        task = asyncio.create_task(harness.worker.run())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_run_processes_sequentially(self) -> None:
        harness = WorkerHarness(passthrough)
        await harness.queue.put(make_envelope())
        await harness.queue.put(make_envelope(event=make_event(external_id='43')))
        task = asyncio.create_task(harness.worker.run())
        while len(await harness.outbox.due(limit=10)) < 2:
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(await harness.outbox.due(limit=10)) == 2
