"""
InMemory-хранилища (§12.4): dedup, outbox исходящих (C-9), реестр,
курсоры, state, аналитика, DLQ.

Отметки времени, которые порты не принимают параметром (момент
дедуп-отметки, ``deleted_at``), проставляются ``datetime.now(UTC)``
(§17.4).
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.domain.models import (
    OutboundRecord,
    OutboxStatus,
    RegistryDelta,
    RegistryOutcome,
    RegistryVersion,
)

if TYPE_CHECKING:
    from uuid import UUID

    from pydantic import AwareDatetime

    from angarion.domain.models import (
        AnalyticsEvent,
        DeadLetter,
        DeliveryReceipt,
        OutboundMessage,
        RegistryRecord,
        SourceCursor,
    )


class MemoryDedupStore:
    """``DedupStorePort``: множество виденных входных ключей + время."""

    def __init__(self) -> None:
        self._inbound: dict[str, datetime] = {}

    async def mark_inbound(self, dedup_key: str) -> bool:
        """True — ключ новый; False — дубль."""
        if dedup_key in self._inbound:
            return False
        self._inbound[dedup_key] = datetime.now(UTC)
        return True

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить отметки старше порога."""
        stale = [key for key, at in self._inbound.items() if at < older_than]
        for key in stale:
            del self._inbound[key]
        return len(stale)


class MemoryOutbox:
    """``OutboxPort``: журнал исходящих с insert-if-absent по ключу."""

    def __init__(self) -> None:
        self._records: dict[str, OutboundRecord] = {}

    async def put(
        self,
        msg: OutboundMessage,
        *,
        pipeline: str | None = None,
        event_uid: UUID | None = None,
    ) -> bool:
        """True — записано; False — ключ уже известен."""
        key = msg.idempotency_key
        if key in self._records:
            return False
        now = datetime.now(UTC)
        self._records[key] = OutboundRecord(
            msg=msg,
            next_attempt_at=now,
            created_at=now,
            pipeline=pipeline,
            event_uid=event_uid,
        )
        return True

    async def due(self, limit: int = 50) -> list[OutboundRecord]:
        """Pending с подошедшим сроком, в порядке поступления."""
        now = datetime.now(UTC)
        ripe = [
            rec
            for rec in self._records.values()
            if rec.status is OutboxStatus.PENDING and rec.next_attempt_at <= now
        ]
        return ripe[:limit]

    async def mark_sent(self, idempotency_key: str, receipt: DeliveryReceipt) -> None:
        """Терминальный переход pending → sent; иначе no-op."""
        rec = self._records.get(idempotency_key)
        if rec is None or rec.status is not OutboxStatus.PENDING:
            return
        self._records[idempotency_key] = rec.model_copy(
            update={
                'status': OutboxStatus.SENT,
                'receipt': receipt,
                'finished_at': datetime.now(UTC),
            }
        )

    async def reschedule(
        self, idempotency_key: str, *, not_before: AwareDatetime, error: str
    ) -> None:
        """Отложить ретрай pending-записи; иначе no-op."""
        rec = self._records.get(idempotency_key)
        if rec is None or rec.status is not OutboxStatus.PENDING:
            return
        self._records[idempotency_key] = rec.model_copy(
            update={
                'attempts': rec.attempts + 1,
                'next_attempt_at': not_before,
                'last_error': error,
            }
        )

    async def mark_failed(self, idempotency_key: str, error: str) -> None:
        """Терминальный переход pending → failed; иначе no-op."""
        rec = self._records.get(idempotency_key)
        if rec is None or rec.status is not OutboxStatus.PENDING:
            return
        self._records[idempotency_key] = rec.model_copy(
            update={
                'status': OutboxStatus.FAILED,
                'last_error': error,
                'finished_at': datetime.now(UTC),
            }
        )

    async def get(self, idempotency_key: str) -> OutboundRecord | None:
        """Запись по ключу или None."""
        return self._records.get(idempotency_key)

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить терминальные записи, финализированные раньше порога."""
        stale = [
            key
            for key, rec in self._records.items()
            if rec.finished_at is not None and rec.finished_at < older_than
        ]
        for key in stale:
            del self._records[key]
        return len(stale)


def _effective_ts(rec: RegistryRecord) -> AwareDatetime:
    """Время последней активности записи: удаление, правка или создание."""
    return rec.deleted_at or rec.edit_ts or rec.event_at


def _id_at_least(external_id: str, min_id: str) -> bool:
    """Числовое сравнение для десятичных id, иначе лексикографика (§5)."""
    if external_id.isdecimal() and min_id.isdecimal():
        return int(external_id) >= int(min_id)
    return external_id >= min_id


class MemoryMessageRegistry:
    """``MessageRegistryPort``: записи + история версий per (source, id)."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], RegistryRecord] = {}
        self._versions: dict[tuple[str, str], list[RegistryVersion]] = {}

    async def upsert(self, rec: RegistryRecord) -> RegistryDelta:
        """Четыре исхода §6.1/A-3; staleness-guard по времени активности."""
        key = (rec.source_key, rec.external_id)
        stored = self._records.get(key)
        if stored is None:
            self._records[key] = rec
            return RegistryDelta(outcome=RegistryOutcome.IS_NEW)
        if _effective_ts(rec) < _effective_ts(stored):
            return RegistryDelta(outcome=RegistryOutcome.STALE)
        if rec.content_hash == stored.content_hash:
            return RegistryDelta(outcome=RegistryOutcome.UNCHANGED)
        self._versions.setdefault(key, []).append(
            RegistryVersion(
                text=stored.text,
                content_hash=stored.content_hash,
                recorded_at=stored.edit_ts or stored.event_at,
            )
        )
        self._records[key] = rec
        return RegistryDelta(
            outcome=RegistryOutcome.TEXT_CHANGED, previous_text=stored.text
        )

    async def mark_deleted(
        self, source_key: str, external_id: str
    ) -> RegistryRecord | None:
        """Пометить удалённым (идемпотентно); вернуть последнее состояние."""
        key = (source_key, external_id)
        stored = self._records.get(key)
        if stored is None:
            return None
        if stored.deleted_at is None:
            stored = stored.model_copy(update={'deleted_at': datetime.now(UTC)})
            self._records[key] = stored
        return stored

    async def known_ids(self, source_key: str, min_id: str) -> set[str]:
        """Не удалённые id источника с ``id >= min_id``."""
        return {
            rec.external_id
            for (src, _), rec in self._records.items()
            if src == source_key
            and rec.deleted_at is None
            and _id_at_least(rec.external_id, min_id)
        }

    async def get(self, source_key: str, external_id: str) -> RegistryRecord | None:
        """Текущее состояние сообщения или None."""
        return self._records.get((source_key, external_id))

    async def versions(
        self, source_key: str, external_id: str
    ) -> list[RegistryVersion]:
        """Архив вытесненных версий, старые первыми."""
        return list(self._versions.get((source_key, external_id), []))

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить записи (с их версиями) и версии старше окна."""
        stale_keys = [
            key for key, rec in self._records.items() if _effective_ts(rec) < older_than
        ]
        for key in stale_keys:
            del self._records[key]
            self._versions.pop(key, None)
        for key, versions in list(self._versions.items()):
            kept = [v for v in versions if v.recorded_at >= older_than]
            if kept:
                self._versions[key] = kept
            else:
                del self._versions[key]
        return len(stale_keys)


class MemoryCursorStore:
    """``CursorStorePort``: словарь курсоров per source."""

    def __init__(self) -> None:
        self._cursors: dict[str, SourceCursor] = {}

    async def load(self, source_key: str) -> SourceCursor | None:
        """Курсор источника или None."""
        return self._cursors.get(source_key)

    async def save(self, cursor: SourceCursor) -> None:
        """Перезаписать курсор источника."""
        self._cursors[cursor.source_key] = cursor


class MemoryStateStore:
    """``StateStorePort``: KV со составным ключом (namespace, key)."""

    def __init__(self) -> None:
        self._values: dict[tuple[str, str], str] = {}

    async def get(self, ns: str, key: str) -> str | None:
        """Значение или None."""
        return self._values.get((ns, key))

    async def set(self, ns: str, key: str, value: str) -> None:
        """Записать значение."""
        self._values[(ns, key)] = value

    async def delete(self, ns: str, key: str) -> None:
        """Удалить ключ; отсутствующий — no-op."""
        self._values.pop((ns, key), None)

    async def keys(self, ns: str, prefix: str = '') -> list[str]:
        """Ключи namespace с данным префиксом, отсортированы."""
        return sorted(
            key
            for (namespace, key) in self._values
            if namespace == ns and key.startswith(prefix)
        )


class MemoryAnalytics:
    """``AnalyticsPort``: append-only список событий."""

    def __init__(self) -> None:
        self._events: list[AnalyticsEvent] = []

    async def record(self, event: AnalyticsEvent) -> None:
        """Записать событие."""
        self._events.append(event)

    async def recent(
        self,
        *,
        kind: str | None = None,
        pipeline: str | None = None,
        limit: int = 50,
    ) -> list[AnalyticsEvent]:
        """Последние события (по порядку записи), новые первыми."""
        matched = [
            event
            for event in reversed(self._events)
            if (kind is None or event.kind == kind)
            and (pipeline is None or event.pipeline == pipeline)
        ]
        return matched[:limit]

    async def counts_by_kind(
        self, *, since: AwareDatetime, pipeline: str | None = None
    ) -> dict[str, int]:
        """Количество событий по видам начиная с ``since``."""
        return dict(
            Counter(
                event.kind
                for event in self._events
                if event.at >= since
                and (pipeline is None or event.pipeline == pipeline)
            )
        )

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить события старше порога."""
        kept = [event for event in self._events if event.at >= older_than]
        removed = len(self._events) - len(kept)
        self._events = kept
        return removed


class MemoryDeadLetters:
    """``DeadLetterPort``: словарь записей DLQ в порядке поступления."""

    def __init__(self) -> None:
        self._letters: dict[UUID, DeadLetter] = {}

    async def put(self, letter: DeadLetter) -> None:
        """Положить запись в DLQ."""
        self._letters[letter.uid] = letter

    async def list(
        self, *, pipeline: str | None = None, limit: int = 50
    ) -> list[DeadLetter]:
        """Записи в порядке поступления, опционально по пайплайну."""
        matched = [
            letter
            for letter in self._letters.values()
            if pipeline is None or letter.envelope.pipeline == pipeline
        ]
        return matched[:limit]

    async def take(self, uid: UUID) -> DeadLetter | None:
        """Изъять запись по uid; None, если записи нет."""
        return self._letters.pop(uid, None)
