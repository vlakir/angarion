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
    DeadLetter,
    QueueEnvelope,
    RecordKind,
    RegistryOutcome,
    RegistryRecord,
)

if TYPE_CHECKING:
    from angarion.application.router import Router
    from angarion.domain.models import Record
    from angarion.domain.ports import (
        AnalyticsPort,
        DeadLetterPort,
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
        dead_letters: DeadLetterPort,
        max_hops: int = 10,
    ) -> None:
        self._dedup = dedup
        self._registry = registry
        self._router = router
        self._queue = queue
        self._analytics = analytics
        self._dead_letters = dead_letters
        self._max_hops = max_hops

    async def ingest(self, record: Record) -> None:
        """Принять нормализованную запись от driving-адаптера (§6.1)."""
        if await self._dedup.seen(record.dedup_key):
            await self._record('duplicate', record)
            return
        if record.hops > self._max_hops:
            await self._dead_letter_hops(record)
            return
        record = await self._maintain_registry(record)
        pipelines = self._router.resolve(record.source, record.kind, record)
        if not pipelines:
            await self._dedup.mark_inbound(record.dedup_key)
            await self._record('unrouted', record)
            return
        for pipeline in sorted(pipelines):
            await self._queue.put(QueueEnvelope(pipeline=pipeline, record=record))
        await self._dedup.mark_inbound(record.dedup_key)  # строго после fan-out (A-11)
        await self._record('ingested', record, {'pipelines': sorted(pipelines)})

    async def _dead_letter_hops(self, record: Record) -> None:
        """
        Рантайм-backstop циклов (T037, Q2): ``hops > max_hops`` → DLQ.

        Универсальный страховочный предел поверх стартовой DAG-валидации: ловит
        и циклы, замкнутые через **реальную** платформу (P1→internal→P2→группа
        X, P1 слушает X), которые статическая проверка увидеть не может. Запись
        дед-леттерится для каждого пайплайна-приёмника канала (виден в его DLQ),
        затем помечается в dedup — повтор доставки ребра гасится как ``duplicate``
        (без новых DLQ-записей). Если канал никто не слушает (петли нет) — DLQ не
        нужен, фиксируем лишь аналитику.
        """
        pipelines = sorted(self._router.resolve(record.source, record.kind, record))
        error = (
            f'hop_limit_exceeded: hops={record.hops} превысил max_hops={self._max_hops}'
        )
        for pipeline in pipelines:
            await self._dead_letters.put(
                DeadLetter(
                    uid=uuid4(),
                    envelope=QueueEnvelope(pipeline=pipeline, record=record),
                    error=error,
                    failed_at=datetime.now(UTC),
                )
            )
        await self._dedup.mark_inbound(record.dedup_key)
        await self._record('hop_limit_exceeded', record, {'hops': record.hops})

    async def _maintain_registry(self, record: Record) -> Record:
        """
        Шаг 2 §6.1: upsert/mark_deleted + обогащение записи из реестра.

        EDITED получает ``previous_text`` вытесненной версии; DELETED —
        текст и метаданные удалённого сообщения (только отсутствующие
        в записи поля). ``stale``-исход реестр не меняет, запись идёт
        дальше без обогащения (guard защищает реестр, не маршрут).
        """
        source_key = make_source_key(
            record.source.transport,
            record.received_by.account_id,
            record.source.address,
            record.source.thread_id,
        )
        if record.kind is RecordKind.DELETED:
            known = await self._registry.mark_deleted(source_key, record.external_id)
            if known is None:
                return record
            return self._enrich_deleted(record, known)
        delta = await self._registry.upsert(
            RegistryRecord(
                source_key=source_key,
                external_id=record.external_id,
                text=record.text,
                content_hash=record.content_hash,
                media_hash=record.media_hash,
                sender_id=record.sender_id,
                sender_name=record.sender_name,
                event_at=record.event_at,
                edit_ts=(record.event_at if record.kind is RecordKind.EDITED else None),
            )
        )
        if (
            record.kind is RecordKind.EDITED
            and delta.outcome is RegistryOutcome.TEXT_CHANGED
        ):
            return record.model_copy(update={'previous_text': delta.previous_text})
        return record

    @staticmethod
    def _enrich_deleted(record: Record, known: RegistryRecord) -> Record:
        """Восстановить из реестра поля, которых нет в записи удаления."""
        updates: dict[str, str | None] = {}
        if record.text is None:
            updates['text'] = known.text
        if record.content_hash is None:
            updates['content_hash'] = known.content_hash
        if record.sender_id is None:
            updates['sender_id'] = known.sender_id
        if record.sender_name is None:
            updates['sender_name'] = known.sender_name
        return record.model_copy(update=updates) if updates else record

    async def _record(
        self,
        kind: str,
        record: Record,
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                record_uid=record.uid,
                payload=payload or {},
                at=datetime.now(UTC),
            )
        )
