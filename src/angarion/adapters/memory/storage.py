"""
InMemory-хранилища (§12.4): dedup, outbox исходящих (C-9), реестр,
курсоры, сессии аккаунтов (M3), state, аналитика, DLQ.

Отметки времени, которые порты не принимают параметром (момент
дедуп-отметки, ``deleted_at``), проставляются ``datetime.now(UTC)``
(§17.4).
"""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from angarion.adapters.registry_rules import (
    content_unchanged,
    effective_ts,
    id_at_least,
)
from angarion.domain.models import (
    CommandStatus,
    DynamicSettings,
    OutboundRecord,
    OutboxCommand,
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
        CommandKind,
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

    async def seen(self, dedup_key: str) -> bool:
        """True — ключ уже отмечен (чтение без записи, A-11 T003)."""
        return dedup_key in self._inbound

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
        if effective_ts(rec) < effective_ts(stored):
            return RegistryDelta(outcome=RegistryOutcome.STALE)
        if content_unchanged(rec, stored):
            return RegistryDelta(outcome=RegistryOutcome.UNCHANGED)
        self._versions.setdefault(key, []).append(
            RegistryVersion(
                text=stored.text,
                content_hash=stored.content_hash,
                media_hash=stored.media_hash,
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
            and id_at_least(rec.external_id, min_id)
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
            key for key, rec in self._records.items() if effective_ts(rec) < older_than
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


class MemorySessionStore:
    """``SessionStorePort``: словарь строк сессий per account_id."""

    def __init__(self) -> None:
        self._sessions: dict[str, str] = {}

    async def load(self, account_id: str) -> str | None:
        """Строка сессии аккаунта или None."""
        return self._sessions.get(account_id)

    async def save(self, account_id: str, session_string: str) -> None:
        """Сохранить (перезаписать) строку сессии аккаунта."""
        self._sessions[account_id] = session_string

    async def account_ids(self) -> list[str]:
        """Аккаунты с сохранённой сессией, отсортированы."""
        return sorted(self._sessions)


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


class MemoryRuntimeConfig:
    """``RuntimeConfigPort``: словарь override'ов динамических настроек."""

    def __init__(self) -> None:
        self._overrides: dict[str, Any] = {}

    async def load(self) -> DynamicSettings:
        """Текущие override'ы как ``DynamicSettings`` (None — нет override'а)."""
        return DynamicSettings(**self._overrides)

    async def save(
        self, patch: DynamicSettings, *, updated_by: str | None = None
    ) -> DynamicSettings:
        """Частичное применение: не-None поля patch'а перекрывают сохранённое."""
        _ = updated_by  # автор изменения — для аудита БД-реализации (§12.8)
        self._overrides.update(patch.model_dump(mode='json', exclude_none=True))
        return await self.load()

    async def reset(self, key: str) -> DynamicSettings:
        """Удалить override поля (возврат к файлу); неизвестное — no-op."""
        self._overrides.pop(key, None)
        return await self.load()


class MemoryCommandOutbox:
    """``CommandOutboxPort``: командный мост api→pipeline в памяти (§12.9)."""

    def __init__(self) -> None:
        self._commands: dict[UUID, OutboxCommand] = {}

    async def put(
        self, kind: CommandKind, *, payload: dict[str, Any] | None = None
    ) -> OutboxCommand:
        """Поставить команду (``pending``); вернуть запись."""
        command = OutboxCommand(
            uid=uuid4(),
            kind=kind,
            payload=payload or {},
            created_at=datetime.now(UTC),
        )
        self._commands[command.uid] = command
        return command

    async def take(self, limit: int = 10) -> list[OutboxCommand]:
        """
        Захватить ``pending`` → ``taken`` (FIFO по порядку вставки).

        Корутина без await-точек между чтением и записью — событийный
        цикл не прерывает её, поэтому захват атомарен (паритет с
        SQL-``UPDATE ... WHERE status='pending'``).
        """
        taken: list[OutboxCommand] = []
        for uid, command in self._commands.items():
            if len(taken) >= limit:
                break
            if command.status is not CommandStatus.PENDING:
                continue
            updated = command.model_copy(
                update={
                    'status': CommandStatus.TAKEN,
                    'taken_at': datetime.now(UTC),
                }
            )
            self._commands[uid] = updated
            taken.append(updated)
        return taken

    async def mark_done(self, uid: UUID, *, result: str | None = None) -> None:
        """Терминальный переход ``taken`` → ``done``; иначе no-op."""
        command = self._commands.get(uid)
        if command is None or command.status is not CommandStatus.TAKEN:
            return
        self._commands[uid] = command.model_copy(
            update={
                'status': CommandStatus.DONE,
                'result': result,
                'executed_at': datetime.now(UTC),
            }
        )

    async def mark_failed(self, uid: UUID, error: str) -> None:
        """Терминальный переход ``taken`` → ``failed``; иначе no-op."""
        command = self._commands.get(uid)
        if command is None or command.status is not CommandStatus.TAKEN:
            return
        self._commands[uid] = command.model_copy(
            update={
                'status': CommandStatus.FAILED,
                'error': error,
                'executed_at': datetime.now(UTC),
            }
        )

    async def get(self, uid: UUID) -> OutboxCommand | None:
        """Команда по ``uid`` или None."""
        return self._commands.get(uid)

    async def reclaim_taken(self, older_than: AwareDatetime) -> int:
        """Вернуть зависшие ``taken`` (``taken_at`` старше порога) в ``pending``."""
        stuck = [
            uid
            for uid, command in self._commands.items()
            if command.status is CommandStatus.TAKEN
            and command.taken_at is not None
            and command.taken_at < older_than
        ]
        for uid in stuck:
            self._commands[uid] = self._commands[uid].model_copy(
                update={'status': CommandStatus.PENDING, 'taken_at': None}
            )
        return len(stuck)

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить терминальные команды, исполненные раньше порога."""
        stale = [
            uid
            for uid, command in self._commands.items()
            if command.executed_at is not None and command.executed_at < older_than
        ]
        for uid in stale:
            del self._commands[uid]
        return len(stale)


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
