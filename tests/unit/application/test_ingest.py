"""IngestService (§6.1 ТЗ, FR-7): дедуп → реестр + обогащение → router → fan-out."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from app_factories import NOW, SOURCE_KEY, make_address, make_event

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.domain.models import EventKind, QueueEnvelope


@pytest.fixture
def queue() -> MemoryQueue:
    return MemoryQueue()


@pytest.fixture
def dedup() -> MemoryDedupStore:
    return MemoryDedupStore()


@pytest.fixture
def registry() -> MemoryMessageRegistry:
    return MemoryMessageRegistry()


@pytest.fixture
def analytics() -> MemoryAnalytics:
    return MemoryAnalytics()


@pytest.fixture
def service(
    queue: MemoryQueue,
    dedup: MemoryDedupStore,
    registry: MemoryMessageRegistry,
    analytics: MemoryAnalytics,
) -> IngestService:
    router = Router(
        [
            RouteSpec(
                pipeline='digest',
                events=frozenset(EventKind),
                sources=(make_address(),),
            ),
            RouteSpec(
                pipeline='audit',
                events=frozenset(EventKind),
                sources=(make_address(),),
            ),
        ]
    )
    return IngestService(
        dedup=dedup,
        registry=registry,
        router=router,
        queue=queue,
        analytics=analytics,
    )


async def drain(queue: MemoryQueue) -> list[QueueEnvelope]:
    envelopes: list[QueueEnvelope] = []
    while (await queue.depth()).pending:
        item = await queue.get()
        envelopes.append(item.envelope)
        await queue.ack(item)
    return envelopes


async def kinds(analytics: MemoryAnalytics) -> list[str]:
    return [event.kind for event in await analytics.recent(limit=100)]


class TestDedup:
    async def test_duplicate_exits_before_registry(
        self,
        service: IngestService,
        dedup: MemoryDedupStore,
        registry: MemoryMessageRegistry,
        queue: MemoryQueue,
        analytics: MemoryAnalytics,
    ) -> None:
        """§6.1 шаг 1: дубль выходит до любых записей в реестр."""
        event = make_event()
        await dedup.mark_inbound(event.dedup_key)
        await service.ingest(event)
        assert await registry.get(SOURCE_KEY, event.external_id) is None
        assert (await queue.depth()).pending == 0
        assert await kinds(analytics) == ['duplicate']

    async def test_second_submission_is_duplicate(
        self,
        service: IngestService,
        queue: MemoryQueue,
        analytics: MemoryAnalytics,
    ) -> None:
        await service.ingest(make_event())
        await service.ingest(make_event(uid=uuid4()))
        assert (await queue.depth()).pending == 2  # fan-out только первого
        assert 'duplicate' in await kinds(analytics)


class TestRegistryMaintenance:
    async def test_new_writes_record(
        self, service: IngestService, registry: MemoryMessageRegistry
    ) -> None:
        await service.ingest(make_event(sender_id='u1', sender_name='Алиса'))
        record = await registry.get(SOURCE_KEY, '42')
        assert record is not None
        assert record.text == 'hello'
        assert record.sender_name == 'Алиса'
        assert record.deleted_at is None

    async def test_edited_enriched_with_previous_text(
        self, service: IngestService, queue: MemoryQueue
    ) -> None:
        """§6.1 шаг 2: EDITED получает previous_text из реестра."""
        await service.ingest(make_event())
        await drain(queue)
        edited = make_event(
            kind=EventKind.MESSAGE_EDITED,
            text='hello v2',
            event_at=NOW + timedelta(seconds=1),
        )
        await service.ingest(edited)
        envelopes = await drain(queue)
        assert len(envelopes) == 2
        assert all(env.event.previous_text == 'hello' for env in envelopes)
        assert all(env.event.text == 'hello v2' for env in envelopes)

    async def test_stale_edit_routed_without_enrichment(
        self,
        service: IngestService,
        registry: MemoryMessageRegistry,
        queue: MemoryQueue,
    ) -> None:
        """Staleness-guard: устаревшая правка не трогает реестр, но маршрутизируется."""
        await service.ingest(make_event())
        await service.ingest(
            make_event(
                kind=EventKind.MESSAGE_EDITED,
                text='v2',
                event_at=NOW + timedelta(seconds=10),
            )
        )
        await drain(queue)
        await service.ingest(
            make_event(
                kind=EventKind.MESSAGE_EDITED,
                text='v3-late',
                event_at=NOW + timedelta(seconds=5),
            )
        )
        record = await registry.get(SOURCE_KEY, '42')
        assert record is not None
        assert record.text == 'v2'
        envelopes = await drain(queue)
        assert len(envelopes) == 2
        assert all(env.event.previous_text is None for env in envelopes)

    async def test_deleted_enriched_from_registry(
        self,
        service: IngestService,
        registry: MemoryMessageRegistry,
        queue: MemoryQueue,
    ) -> None:
        """§6.1 шаг 2: DELETED получает текст и метаданные удалённого."""
        await service.ingest(make_event(sender_id='u1', sender_name='Алиса'))
        await drain(queue)
        await service.ingest(make_event(kind=EventKind.MESSAGE_DELETED, text=None))
        envelopes = await drain(queue)
        assert len(envelopes) == 2
        enriched = envelopes[0].event
        assert enriched.text == 'hello'
        assert enriched.content_hash is not None
        assert enriched.sender_id == 'u1'
        assert enriched.sender_name == 'Алиса'
        record = await registry.get(SOURCE_KEY, '42')
        assert record is not None
        assert record.deleted_at is not None

    async def test_deleted_unknown_message_routed_as_is(
        self, service: IngestService, queue: MemoryQueue
    ) -> None:
        await service.ingest(make_event(kind=EventKind.MESSAGE_DELETED, text=None))
        envelopes = await drain(queue)
        assert len(envelopes) == 2
        assert envelopes[0].event.text is None

    async def test_deleted_own_fields_not_overwritten(
        self, service: IngestService, queue: MemoryQueue
    ) -> None:
        """Обогащение заполняет только отсутствующие поля события."""
        await service.ingest(make_event(sender_id='u1', sender_name='Алиса'))
        await drain(queue)
        deleted = make_event(
            kind=EventKind.MESSAGE_DELETED,
            text='снимок адаптера',
            sender_id='u1-from-event',
            sender_name='Алиса (из события)',
        )
        await service.ingest(deleted)
        envelopes = await drain(queue)
        enriched = envelopes[0].event
        assert enriched.text == 'снимок адаптера'
        assert enriched.content_hash == deleted.content_hash
        assert enriched.sender_id == 'u1-from-event'
        assert enriched.sender_name == 'Алиса (из события)'


class TestRoutingAndFanOut:
    async def test_unrouted(
        self,
        service: IngestService,
        queue: MemoryQueue,
        analytics: MemoryAnalytics,
    ) -> None:
        foreign = make_address(chat_id='-100777')
        event = make_event(source=foreign)
        await service.ingest(event)
        assert (await queue.depth()).pending == 0
        assert await kinds(analytics) == ['unrouted']

    async def test_fanout_one_envelope_per_pipeline(
        self, service: IngestService, queue: MemoryQueue
    ) -> None:
        """§6.1 шаг 4: отдельный QueueEnvelope на каждый пайплайн."""
        event = make_event()
        await service.ingest(event)
        envelopes = await drain(queue)
        assert sorted(env.pipeline for env in envelopes) == ['audit', 'digest']
        assert all(env.event.uid == event.uid for env in envelopes)
        assert all(env.attempt == 0 for env in envelopes)
        assert all(env.not_before is None for env in envelopes)

    async def test_ingested_recorded(
        self, service: IngestService, analytics: MemoryAnalytics
    ) -> None:
        event = make_event()
        await service.ingest(event)
        recorded = await analytics.recent(kind='ingested')
        assert len(recorded) == 1
        assert recorded[0].event_uid == event.uid
        assert recorded[0].payload == {'pipelines': ['audit', 'digest']}


class TestCrashSafety:
    """A-11 спеки T003: отметка дедупа — строго после fan-out."""

    def make_service(
        self,
        queue: MemoryQueue,
        dedup: MemoryDedupStore,
        registry: MemoryMessageRegistry,
        analytics: MemoryAnalytics,
    ) -> IngestService:
        router = Router(
            [
                RouteSpec(
                    pipeline='digest',
                    events=frozenset(EventKind),
                    sources=(make_address(),),
                ),
                RouteSpec(
                    pipeline='audit',
                    events=frozenset(EventKind),
                    sources=(make_address(),),
                ),
            ]
        )
        return IngestService(
            dedup=dedup,
            registry=registry,
            router=router,
            queue=queue,
            analytics=analytics,
        )

    async def test_mark_not_recorded_when_enqueue_fails(
        self,
        dedup: MemoryDedupStore,
        registry: MemoryMessageRegistry,
        analytics: MemoryAnalytics,
    ) -> None:
        """Падение до записи в очередь не оставляет отметки — ре-эмит примется."""

        class ExplodingQueue(MemoryQueue):
            async def put(self, item: QueueEnvelope) -> None:
                raise RuntimeError('имитация падения до записи в очередь')

        service = self.make_service(ExplodingQueue(), dedup, registry, analytics)
        event = make_event()
        with pytest.raises(RuntimeError):
            await service.ingest(event)
        assert await dedup.seen(event.dedup_key) is False

    async def test_reemit_after_partial_fanout_reenqueues(
        self,
        dedup: MemoryDedupStore,
        registry: MemoryMessageRegistry,
        analytics: MemoryAnalytics,
    ) -> None:
        """Крэш посреди fan-out: ре-эмит дописывает очередь (дубль, не потеря)."""

        class PartialFanoutQueue(MemoryQueue):
            def __init__(self) -> None:
                super().__init__()
                self.puts = 0

            async def put(self, item: QueueEnvelope) -> None:
                self.puts += 1
                if self.puts == 2:  # второй пайплайн первого захода
                    raise RuntimeError('имитация kill посреди fan-out')
                await super().put(item)

        queue = PartialFanoutQueue()
        service = self.make_service(queue, dedup, registry, analytics)
        event = make_event()
        with pytest.raises(RuntimeError):
            await service.ingest(event)
        assert (await queue.depth()).pending == 1  # частичный fan-out остался

        await service.ingest(event)  # ре-эмит после «рестарта»
        assert await dedup.seen(event.dedup_key) is True
        # очередь дописана полностью; дубль envelope гасится outbox'ом на выходе
        assert (await queue.depth()).pending == 3
        assert 'ingested' in await kinds(analytics)

    async def test_unrouted_event_is_marked(
        self,
        dedup: MemoryDedupStore,
        registry: MemoryMessageRegistry,
        analytics: MemoryAnalytics,
    ) -> None:
        """Unrouted-ветка тоже оставляет отметку: повтор — «duplicate»."""
        service = self.make_service(MemoryQueue(), dedup, registry, analytics)
        foreign = make_address(chat_id='-100777')
        await service.ingest(make_event(source=foreign))
        await service.ingest(make_event(source=foreign, uid=uuid4()))
        assert await kinds(analytics) == ['duplicate', 'unrouted']
