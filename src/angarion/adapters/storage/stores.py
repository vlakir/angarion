"""
SQLAlchemy-реализации портов ``StorageBundle`` (§12.3; FR-5 спеки T003).

Каждый адаптер получает ``async_sessionmaker`` через конструктор;
запись — короткая транзакция ``async with session.begin()``, никаких
глобальных сессий (FR-3). Поведение — дословный паритет с
InMemory-эталоном (SC-4): общие правила реестра — в
``angarion.adapters.registry_rules``.

Отметки времени, которые порты не принимают параметром (момент
дедуп-отметки, ``deleted_at``, ``created_at`` outbox), проставляются
``datetime.now(UTC)`` (§17.4).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from angarion.adapters.registry_rules import (
    content_unchanged,
    effective_ts,
    id_at_least,
)
from angarion.adapters.storage.orm import (
    AnalyticsEventRow,
    AppSettingRow,
    DeadLetterRow,
    InboundDedupRow,
    MessageRow,
    MessageVersionRow,
    OutboundRow,
    OutboxCommandRow,
    ProcessorStateRow,
    SourceCursorRow,
    TelegramSessionRow,
)
from angarion.domain.models import (
    AnalyticsEvent,
    CommandKind,
    CommandStatus,
    DeadLetter,
    DeliveryReceipt,
    DynamicSettings,
    OutboundMessage,
    OutboundRecord,
    OutboxCommand,
    OutboxStatus,
    QueueEnvelope,
    RegistryDelta,
    RegistryOutcome,
    RegistryRecord,
    RegistryVersion,
    SourceCursor,
)

if TYPE_CHECKING:
    from typing import Any

    from pydantic import AwareDatetime
    from sqlalchemy import CursorResult, Result
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def _is_locked(exc: BaseException) -> bool:
    """``database is locked`` (SQLITE_BUSY) — ровно тот случай для ретрая."""
    return (
        isinstance(exc, OperationalError) and 'database is locked' in str(exc).lower()
    )


# Per-write ретрай ADR §3.1 / T028: в раздельном режиме два процесса пишут в
# один app.db; под WAL писатели сериализуются на едином write-lock'е, и при
# исчерпании busy_timeout (всплеск админ-операций, медленный диск) SQLite
# отдаёт "database is locked". Записи здесь — короткие транзакции с fresh
# session.begin(), безопасные к полному повтору: упавшая на блокировке
# транзакция откатывается целиком, ничего не закоммитив. Только writer'ы —
# читатели под WAL write-lock не берут. reraise=True: исчерпали попытки —
# пробрасываем исходный OperationalError; не-lock ошибки не ретраятся вовсе.
_retry_on_locked = retry(
    retry=retry_if_exception(_is_locked),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.05, max=0.5),
    reraise=True,
)


def _rowcount(result: Result[Any]) -> int:
    """
    ``rowcount`` DML-результата: ``AsyncSession.execute`` типизирован
    базовым ``Result``, у которого атрибута нет в стабах, но DML всегда
    возвращает ``CursorResult`` — сужение без подавления.
    """
    return cast('CursorResult[Any]', result).rowcount


class SqliteDedupStore:
    """``DedupStorePort``: атомарное «отметить, если не было» (§7.4)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def seen(self, dedup_key: str) -> bool:
        """True — ключ уже отмечен (чтение по PK, без записи — A-11)."""
        async with self._sessions() as session:
            return await session.get(InboundDedupRow, dedup_key) is not None

    @_retry_on_locked
    async def mark_inbound(self, dedup_key: str) -> bool:
        """True — ключ новый; False — дубль (insert-or-ignore + rowcount)."""
        stmt = (
            sqlite_insert(InboundDedupRow)
            .values(dedup_key=dedup_key, marked_at=datetime.now(UTC))
            .on_conflict_do_nothing()
        )
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result) == 1

    @_retry_on_locked
    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить отметки старше порога."""
        stmt = delete(InboundDedupRow).where(InboundDedupRow.marked_at < older_than)
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result)


def _row_to_outbound(row: OutboundRow) -> OutboundRecord:
    return OutboundRecord(
        msg=OutboundMessage.model_validate_json(row.msg),
        status=OutboxStatus(row.status),
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        created_at=row.created_at,
        finished_at=row.finished_at,
        receipt=(
            DeliveryReceipt.model_validate_json(row.receipt)
            if row.receipt is not None
            else None
        ),
        last_error=row.last_error,
        pipeline=row.pipeline,
        event_uid=UUID(row.event_uid) if row.event_uid is not None else None,
    )


class SqliteOutbox:
    """``OutboxPort``: журнал исходящих с insert-if-absent по ключу (C-9)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @_retry_on_locked
    async def put(
        self,
        msg: OutboundMessage,
        *,
        pipeline: str | None = None,
        event_uid: UUID | None = None,
    ) -> bool:
        """True — записано; False — ключ уже известен."""
        now = datetime.now(UTC)
        stmt = (
            sqlite_insert(OutboundRow)
            .values(
                idempotency_key=msg.idempotency_key,
                msg=msg.model_dump_json(),
                status=OutboxStatus.PENDING.value,
                attempts=0,
                next_attempt_at=now,
                created_at=now,
                pipeline=pipeline,
                event_uid=str(event_uid) if event_uid is not None else None,
            )
            .on_conflict_do_nothing()
        )
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result) == 1

    async def due(self, limit: int = 50) -> list[OutboundRecord]:
        """Pending с подошедшим сроком, FIFO (rowid — порядок вставки)."""
        stmt = (
            select(OutboundRow)
            .where(
                OutboundRow.status == OutboxStatus.PENDING.value,
                OutboundRow.next_attempt_at <= datetime.now(UTC),
            )
            .order_by(OutboundRow.created_at, text('rowid'))
            .limit(limit)
        )
        async with self._sessions() as session:
            rows = (await session.scalars(stmt)).all()
        return [_row_to_outbound(row) for row in rows]

    @_retry_on_locked
    async def mark_sent(self, idempotency_key: str, receipt: DeliveryReceipt) -> None:
        """Терминальный переход pending → sent; иначе no-op."""
        stmt = (
            update(OutboundRow)
            .where(
                OutboundRow.idempotency_key == idempotency_key,
                OutboundRow.status == OutboxStatus.PENDING.value,
            )
            .values(
                status=OutboxStatus.SENT.value,
                receipt=receipt.model_dump_json(),
                finished_at=datetime.now(UTC),
            )
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    @_retry_on_locked
    async def reschedule(
        self, idempotency_key: str, *, not_before: AwareDatetime, error: str
    ) -> None:
        """Отложить ретрай pending-записи; иначе no-op."""
        stmt = (
            update(OutboundRow)
            .where(
                OutboundRow.idempotency_key == idempotency_key,
                OutboundRow.status == OutboxStatus.PENDING.value,
            )
            .values(
                attempts=OutboundRow.attempts + 1,
                next_attempt_at=not_before,
                last_error=error,
            )
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    @_retry_on_locked
    async def mark_failed(self, idempotency_key: str, error: str) -> None:
        """Терминальный переход pending → failed; иначе no-op."""
        stmt = (
            update(OutboundRow)
            .where(
                OutboundRow.idempotency_key == idempotency_key,
                OutboundRow.status == OutboxStatus.PENDING.value,
            )
            .values(
                status=OutboxStatus.FAILED.value,
                last_error=error,
                finished_at=datetime.now(UTC),
            )
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def get(self, idempotency_key: str) -> OutboundRecord | None:
        """Запись по ключу или None."""
        async with self._sessions() as session:
            row = await session.get(OutboundRow, idempotency_key)
        return _row_to_outbound(row) if row is not None else None

    @_retry_on_locked
    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить терминальные записи, финализированные раньше порога."""
        stmt = delete(OutboundRow).where(
            OutboundRow.finished_at.is_not(None),
            OutboundRow.finished_at < older_than,
        )
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result)


def _row_to_record(row: MessageRow) -> RegistryRecord:
    return RegistryRecord(
        source_key=row.source_key,
        external_id=row.external_id,
        text=row.text,
        content_hash=row.content_hash,
        media_hash=row.media_hash,
        sender_id=row.sender_id,
        sender_name=row.sender_name,
        event_at=row.event_at,
        edit_ts=row.edit_ts,
        deleted_at=row.deleted_at,
    )


def _apply_record(row: MessageRow, rec: RegistryRecord) -> None:
    """Перезаписать состояние строки целиком — паритет с InMemory."""
    row.text = rec.text
    row.content_hash = rec.content_hash
    row.media_hash = rec.media_hash
    row.sender_id = rec.sender_id
    row.sender_name = rec.sender_name
    row.event_at = rec.event_at
    row.edit_ts = rec.edit_ts
    row.deleted_at = rec.deleted_at


class SqliteMessageRegistry:
    """``MessageRegistryPort``: записи + история версий per (source, id)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @_retry_on_locked
    async def upsert(self, rec: RegistryRecord) -> RegistryDelta:
        """Четыре исхода §6.1/A-3; staleness-guard по времени активности."""
        async with self._sessions() as session, session.begin():
            row = await session.get(MessageRow, (rec.source_key, rec.external_id))
            if row is None:
                new_row = MessageRow(
                    source_key=rec.source_key, external_id=rec.external_id
                )
                _apply_record(new_row, rec)
                session.add(new_row)
                return RegistryDelta(outcome=RegistryOutcome.IS_NEW)
            stored = _row_to_record(row)
            if effective_ts(rec) < effective_ts(stored):
                return RegistryDelta(outcome=RegistryOutcome.STALE)
            if content_unchanged(rec, stored):
                return RegistryDelta(outcome=RegistryOutcome.UNCHANGED)
            session.add(
                MessageVersionRow(
                    source_key=rec.source_key,
                    external_id=rec.external_id,
                    text=stored.text,
                    content_hash=stored.content_hash,
                    media_hash=stored.media_hash,
                    recorded_at=stored.edit_ts or stored.event_at,
                )
            )
            _apply_record(row, rec)
            return RegistryDelta(
                outcome=RegistryOutcome.TEXT_CHANGED, previous_text=stored.text
            )

    @_retry_on_locked
    async def mark_deleted(
        self, source_key: str, external_id: str
    ) -> RegistryRecord | None:
        """Пометить удалённым (идемпотентно); вернуть последнее состояние."""
        async with self._sessions() as session, session.begin():
            row = await session.get(MessageRow, (source_key, external_id))
            if row is None:
                return None
            if row.deleted_at is None:
                row.deleted_at = datetime.now(UTC)
            return _row_to_record(row)

    async def known_ids(self, source_key: str, min_id: str) -> set[str]:
        """Не удалённые id источника; сравнение min_id — в Python (A-5)."""
        stmt = select(MessageRow.external_id).where(
            MessageRow.source_key == source_key,
            MessageRow.deleted_at.is_(None),
        )
        async with self._sessions() as session:
            ids = (await session.scalars(stmt)).all()
        return {external_id for external_id in ids if id_at_least(external_id, min_id)}

    async def get(self, source_key: str, external_id: str) -> RegistryRecord | None:
        """Текущее состояние сообщения или None."""
        async with self._sessions() as session:
            row = await session.get(MessageRow, (source_key, external_id))
        return _row_to_record(row) if row is not None else None

    async def versions(
        self, source_key: str, external_id: str
    ) -> list[RegistryVersion]:
        """Архив вытесненных версий, старые первыми."""
        stmt = (
            select(MessageVersionRow)
            .where(
                MessageVersionRow.source_key == source_key,
                MessageVersionRow.external_id == external_id,
            )
            .order_by(MessageVersionRow.seq)
        )
        async with self._sessions() as session:
            rows = (await session.scalars(stmt)).all()
        return [
            RegistryVersion(
                text=row.text,
                content_hash=row.content_hash,
                media_hash=row.media_hash,
                recorded_at=row.recorded_at,
            )
            for row in rows
        ]

    @_retry_on_locked
    async def prune(self, older_than: AwareDatetime) -> int:
        """
        Удалить записи старше окна (архив уносит FK CASCADE) и
        отдельно — устаревшие версии оставшихся записей.
        """
        last_activity = func.coalesce(
            MessageRow.deleted_at, MessageRow.edit_ts, MessageRow.event_at
        )
        async with self._sessions() as session, session.begin():
            removed = await session.execute(
                delete(MessageRow).where(last_activity < older_than)
            )
            await session.execute(
                delete(MessageVersionRow).where(
                    MessageVersionRow.recorded_at < older_than
                )
            )
        return _rowcount(removed)


class SqliteCursorStore:
    """``CursorStorePort``: upsert курсора per source (§9.2)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def load(self, source_key: str) -> SourceCursor | None:
        """Курсор источника или None."""
        async with self._sessions() as session:
            row = await session.get(SourceCursorRow, source_key)
        if row is None:
            return None
        return SourceCursor(
            source_key=row.source_key,
            payload=json.loads(row.payload),
            updated_at=row.updated_at,
        )

    @_retry_on_locked
    async def save(self, cursor: SourceCursor) -> None:
        """Перезаписать курсор источника (insert-or-update)."""
        stmt = sqlite_insert(SourceCursorRow).values(
            source_key=cursor.source_key,
            payload=json.dumps(cursor.payload),
            updated_at=cursor.updated_at,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[SourceCursorRow.source_key],
            set_={
                'payload': stmt.excluded.payload,
                'updated_at': stmt.excluded.updated_at,
            },
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)


class SqliteSessionStore:
    """``SessionStorePort``: upsert строки сессии per account_id (M3)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def load(self, account_id: str) -> str | None:
        """Строка сессии аккаунта или None."""
        async with self._sessions() as session:
            row = await session.get(TelegramSessionRow, account_id)
        return row.session_string if row is not None else None

    @_retry_on_locked
    async def save(self, account_id: str, session_string: str) -> None:
        """Сохранить (перезаписать) строку сессии (insert-or-update)."""
        stmt = sqlite_insert(TelegramSessionRow).values(
            account_id=account_id,
            session_string=session_string,
            updated_at=datetime.now(UTC),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[TelegramSessionRow.account_id],
            set_={
                'session_string': stmt.excluded.session_string,
                'updated_at': stmt.excluded.updated_at,
            },
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def account_ids(self) -> list[str]:
        """Аккаунты с сохранённой сессией, отсортированы."""
        stmt = select(TelegramSessionRow.account_id).order_by(
            TelegramSessionRow.account_id
        )
        async with self._sessions() as session:
            ids = (await session.scalars(stmt)).all()
        return list(ids)


class SqliteStateStore:
    """``StateStorePort``: KV со составным ключом (namespace, key)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get(self, ns: str, key: str) -> str | None:
        """Значение или None."""
        async with self._sessions() as session:
            row = await session.get(ProcessorStateRow, (ns, key))
        return row.value if row is not None else None

    @_retry_on_locked
    async def set(self, ns: str, key: str, value: str) -> None:
        """Записать значение (insert-or-update)."""
        stmt = sqlite_insert(ProcessorStateRow).values(ns=ns, key=key, value=value)
        stmt = stmt.on_conflict_do_update(
            index_elements=[ProcessorStateRow.ns, ProcessorStateRow.key],
            set_={'value': stmt.excluded.value},
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    @_retry_on_locked
    async def delete(self, ns: str, key: str) -> None:
        """Удалить ключ; отсутствующий — no-op."""
        stmt = delete(ProcessorStateRow).where(
            ProcessorStateRow.ns == ns, ProcessorStateRow.key == key
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def keys(self, ns: str, prefix: str = '') -> list[str]:
        """
        Ключи namespace с данным префиксом, отсортированы. Фильтр
        префикса — в Python: ``LIKE`` потребовал бы экранирования
        ``%``/``_``, а семантика нужна ровно ``startswith``.
        """
        stmt = (
            select(ProcessorStateRow.key)
            .where(ProcessorStateRow.ns == ns)
            .order_by(ProcessorStateRow.key)
        )
        async with self._sessions() as session:
            keys = (await session.scalars(stmt)).all()
        return [key for key in keys if key.startswith(prefix)]


class SqliteRuntimeConfig:
    """``RuntimeConfigPort``: override'ы динамических настроек в ``app_settings``."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @staticmethod
    def _to_settings(raw: dict[str, Any]) -> DynamicSettings:
        """Собрать DTO из строк, отбросив неизвестные ключи (эволюция схемы)."""
        known = {k: v for k, v in raw.items() if k in DynamicSettings.model_fields}
        return DynamicSettings(**known)

    async def load(self) -> DynamicSettings:
        """Все override'ы из таблицы как ``DynamicSettings``."""
        async with self._sessions() as session:
            rows = (await session.scalars(select(AppSettingRow))).all()
        return self._to_settings({row.key: json.loads(row.value) for row in rows})

    @_retry_on_locked
    async def save(
        self, patch: DynamicSettings, *, updated_by: str | None = None
    ) -> DynamicSettings:
        """Upsert не-None полей patch'а (insert-or-update по ключу)."""
        now = datetime.now(UTC)
        data = patch.model_dump(mode='json', exclude_none=True)
        async with self._sessions() as session, session.begin():
            for key, value in data.items():
                stmt = sqlite_insert(AppSettingRow).values(
                    key=key,
                    value=json.dumps(value),
                    updated_at=now,
                    updated_by=updated_by,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=[AppSettingRow.key],
                    set_={
                        'value': stmt.excluded.value,
                        'updated_at': stmt.excluded.updated_at,
                        'updated_by': stmt.excluded.updated_by,
                    },
                )
                await session.execute(stmt)
        return await self.load()

    @_retry_on_locked
    async def reset(self, key: str) -> DynamicSettings:
        """Удалить override поля (возврат к файлу); неизвестное — no-op."""
        async with self._sessions() as session, session.begin():
            await session.execute(delete(AppSettingRow).where(AppSettingRow.key == key))
        return await self.load()


def _row_to_command(row: OutboxCommandRow) -> OutboxCommand:
    return OutboxCommand(
        uid=UUID(row.uid),
        kind=CommandKind(row.kind),
        payload=json.loads(row.payload),
        status=CommandStatus(row.status),
        created_at=row.created_at,
        executed_at=row.executed_at,
        result=row.result,
        error=row.error,
    )


class SqliteCommandOutbox:
    """``CommandOutboxPort``: командный мост api→pipeline (§12.9)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @_retry_on_locked
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
        row = OutboxCommandRow(
            uid=str(command.uid),
            kind=command.kind.value,
            payload=json.dumps(command.payload),
            status=command.status.value,
            created_at=command.created_at,
            executed_at=None,
            result=None,
            error=None,
        )
        async with self._sessions() as session, session.begin():
            session.add(row)
        return command

    @_retry_on_locked
    async def take(self, limit: int = 10) -> list[OutboxCommand]:
        """Атомарно захватить ``pending`` → ``taken`` (FIFO), вернуть их."""
        pending = (
            select(OutboxCommandRow.uid)
            .where(OutboxCommandRow.status == CommandStatus.PENDING.value)
            .order_by(OutboxCommandRow.created_at, text('rowid'))
            .limit(limit)
        )
        stmt = (
            update(OutboxCommandRow)
            .where(
                OutboxCommandRow.uid.in_(pending),
                OutboxCommandRow.status == CommandStatus.PENDING.value,
            )
            .values(status=CommandStatus.TAKEN.value)
            .returning(OutboxCommandRow)
        )
        async with self._sessions() as session, session.begin():
            rows = (await session.execute(stmt)).scalars().all()
        return sorted(
            (_row_to_command(row) for row in rows), key=lambda c: c.created_at
        )

    @_retry_on_locked
    async def mark_done(self, uid: UUID, *, result: str | None = None) -> None:
        """Терминальный переход ``taken`` → ``done``; иначе no-op."""
        stmt = (
            update(OutboxCommandRow)
            .where(
                OutboxCommandRow.uid == str(uid),
                OutboxCommandRow.status == CommandStatus.TAKEN.value,
            )
            .values(
                status=CommandStatus.DONE.value,
                result=result,
                executed_at=datetime.now(UTC),
            )
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    @_retry_on_locked
    async def mark_failed(self, uid: UUID, error: str) -> None:
        """Терминальный переход ``taken`` → ``failed``; иначе no-op."""
        stmt = (
            update(OutboxCommandRow)
            .where(
                OutboxCommandRow.uid == str(uid),
                OutboxCommandRow.status == CommandStatus.TAKEN.value,
            )
            .values(
                status=CommandStatus.FAILED.value,
                error=error,
                executed_at=datetime.now(UTC),
            )
        )
        async with self._sessions() as session, session.begin():
            await session.execute(stmt)

    async def get(self, uid: UUID) -> OutboxCommand | None:
        """Команда по ``uid`` или None."""
        async with self._sessions() as session:
            row = await session.get(OutboxCommandRow, str(uid))
        return _row_to_command(row) if row is not None else None

    @_retry_on_locked
    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить терминальные команды, исполненные раньше порога."""
        stmt = delete(OutboxCommandRow).where(
            OutboxCommandRow.executed_at.is_not(None),
            OutboxCommandRow.executed_at < older_than,
        )
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result)


def _row_to_event(row: AnalyticsEventRow) -> AnalyticsEvent:
    return AnalyticsEvent(
        uid=UUID(row.uid),
        kind=row.kind,
        event_uid=UUID(row.event_uid) if row.event_uid is not None else None,
        pipeline=row.pipeline,
        payload=json.loads(row.payload),
        at=row.at,
    )


class SqliteAnalytics:
    """``AnalyticsPort``: append-only журнал + read-сторона (§5)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @_retry_on_locked
    async def record(self, event: AnalyticsEvent) -> None:
        """Записать событие."""
        row = AnalyticsEventRow(
            uid=str(event.uid),
            kind=event.kind,
            event_uid=str(event.event_uid) if event.event_uid is not None else None,
            pipeline=event.pipeline,
            payload=json.dumps(event.payload),
            at=event.at,
        )
        async with self._sessions() as session, session.begin():
            session.add(row)

    async def recent(
        self,
        *,
        kind: str | None = None,
        pipeline: str | None = None,
        limit: int = 50,
    ) -> list[AnalyticsEvent]:
        """Последние события (по порядку записи), новые первыми."""
        stmt = select(AnalyticsEventRow)
        if kind is not None:
            stmt = stmt.where(AnalyticsEventRow.kind == kind)
        if pipeline is not None:
            stmt = stmt.where(AnalyticsEventRow.pipeline == pipeline)
        stmt = stmt.order_by(AnalyticsEventRow.seq.desc()).limit(limit)
        async with self._sessions() as session:
            rows = (await session.scalars(stmt)).all()
        return [_row_to_event(row) for row in rows]

    async def counts_by_kind(
        self, *, since: AwareDatetime, pipeline: str | None = None
    ) -> dict[str, int]:
        """Количество событий по видам начиная с ``since``."""
        stmt = (
            select(AnalyticsEventRow.kind, func.count())
            .where(AnalyticsEventRow.at >= since)
            .group_by(AnalyticsEventRow.kind)
        )
        if pipeline is not None:
            stmt = stmt.where(AnalyticsEventRow.pipeline == pipeline)
        async with self._sessions() as session:
            rows = (await session.execute(stmt)).tuples().all()
        return dict(rows)

    @_retry_on_locked
    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить события старше порога."""
        stmt = delete(AnalyticsEventRow).where(AnalyticsEventRow.at < older_than)
        async with self._sessions() as session, session.begin():
            result = await session.execute(stmt)
        return _rowcount(result)


def _row_to_letter(row: DeadLetterRow) -> DeadLetter:
    return DeadLetter(
        uid=UUID(row.uid),
        envelope=QueueEnvelope.model_validate_json(row.envelope),
        error=row.error,
        failed_at=row.failed_at,
    )


class SqliteDeadLetters:
    """``DeadLetterPort``: put/list/take в порядке поступления (C-2)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    @_retry_on_locked
    async def put(self, letter: DeadLetter) -> None:
        """Положить запись в DLQ."""
        row = DeadLetterRow(
            uid=str(letter.uid),
            pipeline=letter.envelope.pipeline,
            envelope=letter.envelope.model_dump_json(),
            error=letter.error,
            failed_at=letter.failed_at,
        )
        async with self._sessions() as session, session.begin():
            session.add(row)

    async def list(
        self, *, pipeline: str | None = None, limit: int = 50
    ) -> list[DeadLetter]:
        """Записи в порядке поступления, опционально по пайплайну."""
        stmt = select(DeadLetterRow)
        if pipeline is not None:
            stmt = stmt.where(DeadLetterRow.pipeline == pipeline)
        stmt = stmt.order_by(DeadLetterRow.seq).limit(limit)
        async with self._sessions() as session:
            rows = (await session.scalars(stmt)).all()
        return [_row_to_letter(row) for row in rows]

    @_retry_on_locked
    async def take(self, uid: UUID) -> DeadLetter | None:
        """Изъять запись по uid; None, если записи нет."""
        stmt = select(DeadLetterRow).where(DeadLetterRow.uid == str(uid))
        async with self._sessions() as session, session.begin():
            row = (await session.scalars(stmt)).one_or_none()
            if row is None:
                return None
            letter = _row_to_letter(row)
            await session.delete(row)
        return letter
