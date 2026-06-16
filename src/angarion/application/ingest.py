"""
IngestService (§6.1 ТЗ, FR-7): единая точка входа событий (live и
catch-up).

Порядок строго: проверка дедупа ``seen()`` (до любых записей в
реестр) → поддержка реестра со staleness-guard и обогащением →
router (multicast + фильтры) → fan-out (отдельный ``QueueEnvelope``
на каждый пайплайн) → **отметка дедупа** → аналитика.

Отметка пишется после fan-out сознательно (A-11 спеки T003,
крэш-безопасность §7.1): kill между ``queue.put`` и отметкой даёт при
ре-эмите повторную постановку envelope — дубль обработки гасится
outbox'ом (insert-if-absent C-9), а отметка до постановки давала бы
безвозвратную потерю (ре-эмит гасился бы как «дубль»). Повторный
заход после падения переписывает реестр идемпотентно (``unchanged``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from angarion.domain.keys import make_source_key
from angarion.domain.models import (
    AnalyticsEvent,
    EventKind,
    QueueEnvelope,
    RegistryOutcome,
    RegistryRecord,
)

if TYPE_CHECKING:
    from angarion.application.router import Router
    from angarion.domain.models import InboundEvent
    from angarion.domain.ports import (
        AnalyticsPort,
        DedupStorePort,
        EventQueuePort,
        MessageRegistryPort,
    )


class IngestService:
    """Проверка дедупа → реестр (+ обогащение) → fan-out → отметка."""

    def __init__(
        self,
        *,
        dedup: DedupStorePort,
        registry: MessageRegistryPort,
        router: Router,
        queue: EventQueuePort,
        analytics: AnalyticsPort,
    ) -> None:
        self._dedup = dedup
        self._registry = registry
        self._router = router
        self._queue = queue
        self._analytics = analytics

    async def ingest(self, event: InboundEvent) -> None:
        """Принять нормализованное событие от driving-адаптера (§6.1)."""
        if await self._dedup.seen(event.dedup_key):
            await self._record('duplicate', event)
            return
        event = await self._maintain_registry(event)
        pipelines = self._router.resolve(event.source, event.kind, event)
        if not pipelines:
            await self._dedup.mark_inbound(event.dedup_key)
            await self._record('unrouted', event)
            return
        for pipeline in sorted(pipelines):
            await self._queue.put(QueueEnvelope(pipeline=pipeline, event=event))
        await self._dedup.mark_inbound(event.dedup_key)  # строго после fan-out (A-11)
        await self._record('ingested', event, {'pipelines': sorted(pipelines)})

    async def _maintain_registry(self, event: InboundEvent) -> InboundEvent:
        """
        Шаг 2 §6.1: upsert/mark_deleted + обогащение события из реестра.

        EDITED получает ``previous_text`` вытесненной версии; DELETED —
        текст и метаданные удалённого сообщения (только отсутствующие
        в событии поля). ``stale``-исход реестр не меняет, событие идёт
        дальше без обогащения (guard защищает реестр, не маршрут).
        """
        source_key = make_source_key(
            event.source.messenger,
            event.received_by.account_id,
            event.source.chat_id,
            event.source.thread_id,
        )
        if event.kind is EventKind.MESSAGE_DELETED:
            record = await self._registry.mark_deleted(source_key, event.external_id)
            if record is None:
                return event
            return self._enrich_deleted(event, record)
        delta = await self._registry.upsert(
            RegistryRecord(
                source_key=source_key,
                external_id=event.external_id,
                text=event.text,
                content_hash=event.content_hash,
                media_hash=event.media_hash,
                sender_id=event.sender_id,
                sender_name=event.sender_name,
                event_at=event.event_at,
                edit_ts=(
                    event.event_at if event.kind is EventKind.MESSAGE_EDITED else None
                ),
            )
        )
        if (
            event.kind is EventKind.MESSAGE_EDITED
            and delta.outcome is RegistryOutcome.TEXT_CHANGED
        ):
            return event.model_copy(update={'previous_text': delta.previous_text})
        return event

    @staticmethod
    def _enrich_deleted(event: InboundEvent, record: RegistryRecord) -> InboundEvent:
        """Восстановить из реестра поля, которых нет в событии удаления."""
        updates: dict[str, str | None] = {}
        if event.text is None:
            updates['text'] = record.text
        if event.content_hash is None:
            updates['content_hash'] = record.content_hash
        if event.sender_id is None:
            updates['sender_id'] = record.sender_id
        if event.sender_name is None:
            updates['sender_name'] = record.sender_name
        return event.model_copy(update=updates) if updates else event

    async def _record(
        self,
        kind: str,
        event: InboundEvent,
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                event_uid=event.uid,
                payload=payload or {},
                at=datetime.now(UTC),
            )
        )
