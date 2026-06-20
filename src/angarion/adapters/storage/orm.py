"""
ORM-сущности SQLite-бэкенда (§12.3 ТЗ; FR-3, FR-4 спеки T003) —
приватные для адаптера (§3.1): наружу порты отдают доменные DTO.

Состав — объём M2. ``DeliveredDedupRow`` из §12.3 сознательно не
создаётся (C-9 T002: выходная идемпотентность — первичный ключ
outbox); вместо него — ``OutboundRow``. ``UserRow`` (M5/B), ``AppSettingRow`` и
``OutboxCommandRow`` (M5/C) добавлены в M5 своими миграциями (C-2).

Время — ``UTCDateTime`` (A-4, §17.4): TEXT ISO 8601 с явным смещением,
нормализация в UTC; фиксированная ширина (``timespec='microseconds'``)
делает лексикографическое сравнение строк в SQL хронологическим.
Сложные значения (envelope, payload, receipt) — JSON-текст: пайплайн
сериализуем без потерь по контракту DTO (§4), pickle исключён.

Surrogate-ключи ``seq`` (``sqlite_autoincrement`` — без переиспользования
id) — порядок поступления для ``recent()`` / ``list()`` / ``versions()``:
паритет с InMemory, который держит insertion order, тогда как времена
событий могут совпадать.

Pydantic здесь не применим: декларативный маппинг SQLAlchemy 2.0
требует собственной метамодели (``DeclarativeBase`` / ``Mapped``).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final

from sqlalchemy import ForeignKeyConstraint, Index, String, TypeDecorator, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect

_ROLE_ADMIN: Final = 'admin'
_ROLE_VIEWER: Final = 'viewer'
"""Значения роли в ``UserRow.role`` (контракт с auth-слоём §12.7)."""


class UTCDateTime(TypeDecorator[datetime]):
    """``AwareDatetime`` строкой ISO 8601 в UTC (§17.4, A-4)."""

    impl = String
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, _dialect: Dialect
    ) -> str | None:
        """Нормализовать в UTC; наивное время — ошибка, не догадка."""
        if value is None:
            return None
        if value.tzinfo is None:
            msg = 'наивный datetime недопустим: время — строго UTC (§17.4)'
            raise ValueError(msg)
        return value.astimezone(UTC).isoformat(timespec='microseconds')

    def process_result_value(
        self, value: str | None, _dialect: Dialect
    ) -> datetime | None:
        """Прочитать ISO 8601 и вернуть aware-datetime в UTC."""
        if value is None:
            return None
        return datetime.fromisoformat(value).astimezone(UTC)


class Base(DeclarativeBase):
    """База ORM-моделей; каждая ``datetime``-колонка — ``UTCDateTime``."""

    type_annotation_map: ClassVar[dict[Any, Any]] = {datetime: UTCDateTime()}


class InboundDedupRow(Base):
    """Отметка входной дедупликации (§7.2); ``marked_at`` — для prune (A-6)."""

    __tablename__ = 'inbound_dedup'

    dedup_key: Mapped[str] = mapped_column(primary_key=True)
    marked_at: Mapped[datetime]


class OutboundRow(Base):
    """
    Журнал outbox исходящих (C-9 T002): персистентная форма
    ``OutboxRecord``. PK ``idempotency_key`` — выходная
    идемпотентность (§7.3); ``record`` / ``receipt`` — JSON DTO.
    """

    __tablename__ = 'outbound'
    __table_args__ = (Index('ix_outbound_due', 'status', 'next_attempt_at'),)

    idempotency_key: Mapped[str] = mapped_column(primary_key=True)
    record: Mapped[str]
    status: Mapped[str]
    attempts: Mapped[int]
    next_attempt_at: Mapped[datetime]
    created_at: Mapped[datetime]
    finished_at: Mapped[datetime | None]
    receipt: Mapped[str | None]
    last_error: Mapped[str | None]
    pipeline: Mapped[str | None]
    record_uid: Mapped[str | None]


class MessageRow(Base):
    """Текущее состояние сообщения в реестре источника (§9.2)."""

    __tablename__ = 'messages'

    source_key: Mapped[str] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(primary_key=True)
    text: Mapped[str | None]
    content_hash: Mapped[str | None]
    media_hash: Mapped[str | None]
    sender_id: Mapped[str | None]
    sender_name: Mapped[str | None]
    event_at: Mapped[datetime]
    edit_ts: Mapped[datetime | None]
    deleted_at: Mapped[datetime | None]


class MessageVersionRow(Base):
    """
    Архивная версия текста, вытесненная редактированием (§9.2).
    FK с ``ON DELETE CASCADE`` — prune записи уносит её архив
    (работает благодаря ``PRAGMA foreign_keys=ON``, FR-3).
    """

    __tablename__ = 'message_versions'
    __table_args__ = (
        ForeignKeyConstraint(
            ['source_key', 'external_id'],
            ['messages.source_key', 'messages.external_id'],
            ondelete='CASCADE',
        ),
        Index('ix_message_versions_message', 'source_key', 'external_id'),
        {'sqlite_autoincrement': True},
    )

    seq: Mapped[int] = mapped_column(primary_key=True)
    source_key: Mapped[str]
    external_id: Mapped[str]
    text: Mapped[str | None]
    content_hash: Mapped[str | None]
    media_hash: Mapped[str | None]
    recorded_at: Mapped[datetime]


class SourceCursorRow(Base):
    """Курсор catch-up per source (§9.2); payload — непрозрачный JSON."""

    __tablename__ = 'source_cursors'

    source_key: Mapped[str] = mapped_column(primary_key=True)
    payload: Mapped[str]
    updated_at: Mapped[datetime]


class TelegramSessionRow(Base):
    """
    Сессия аккаунта платформы (M3, T005): ``account_id`` → строка
    сессии. ``session_string`` хранится **зашифрованным** at-rest
    (Fernet, Q2): шифрует декоратор ``EncryptedSessionStore``
    telegram-адаптера, сюда строка приходит уже как ciphertext — само
    хранилище криптографию не знает (порт оперирует непрозрачной
    строкой).
    """

    __tablename__ = 'telegram_sessions'

    account_id: Mapped[str] = mapped_column(primary_key=True)
    session_string: Mapped[str]
    updated_at: Mapped[datetime]


class ProcessorStateRow(Base):
    """KV-состояние stateful-процессоров (§10.3): (namespace, key) → JSON."""

    __tablename__ = 'processor_state'

    ns: Mapped[str] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str]


class AnalyticsEventRow(Base):
    """Событие аналитики (§4.3); ``seq`` — порядок записи для ``recent()``."""

    __tablename__ = 'analytics_events'
    __table_args__ = ({'sqlite_autoincrement': True},)

    seq: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(unique=True)
    kind: Mapped[str]
    record_uid: Mapped[str | None]
    pipeline: Mapped[str | None]
    payload: Mapped[str]
    at: Mapped[datetime] = mapped_column(index=True)


class DeadLetterRow(Base):
    """
    Запись DLQ (§8, C-2): полный дамп envelope JSON-текстом.
    ``pipeline`` продублирован из envelope для SQL-фильтра ``list()``.
    """

    __tablename__ = 'dead_letters'
    __table_args__ = ({'sqlite_autoincrement': True},)

    seq: Mapped[int] = mapped_column(primary_key=True)
    uid: Mapped[str] = mapped_column(unique=True)
    pipeline: Mapped[str]
    envelope: Mapped[str]
    error: Mapped[str]
    failed_at: Mapped[datetime]


class UserRow(Base):
    """
    Пользователь web-доступа (§12.7, M5/B T023): саморегистрация +
    одобрение администратором.

    ``id`` — UUID (нативный ``sa.Uuid``: CHAR(32) в SQLite); ``login`` —
    уникальный логин входа (fastapi-users оперирует им как «email»-полем
    через адаптер, фаза 2). ``hashed_password``/``is_active`` названы по
    контракту fastapi-users; ``is_active=False`` — заявка ждёт одобрения
    (войти нельзя). ``role`` — ``admin``/``viewer`` строкой (значения
    валидирует auth-слой, БД хранит как TEXT). ``approved_at``/
    ``approved_by`` — аудит одобрения (NULL у заявки и у bootstrap-админа).
    """

    __tablename__ = 'users'

    # Соответствие протоколу fastapi-users (``UserProtocol[Any]``) под
    # mypy --strict требует плоских аннотаций членов протокола — дескриптор
    # ``Mapped`` под ``Any``-параметром bound-проверку не проходит (тот же
    # приём в самой fastapi-users: ``SQLAlchemyBaseUserTable``). Поэтому
    # ``id``/``hashed_password``/``is_active`` и мост-алиасы ``email``/
    # ``is_superuser``/``is_verified`` объявлены здесь как простые типы для
    # type-checker'а, а runtime-определения (Mapped-колонки и property без
    # импорта fastapi-users — storage ниже web-слоя, ADR 2026-06-14) —
    # в ветке ``else``. ``email`` = ``login``; ``superuser`` ↔ роль
    # ``admin``; ``verified`` ↔ одобренный (``is_active``). Сеттеры — для
    # соответствия (mutable) протоколу; в наших потоках не вызываются.
    if TYPE_CHECKING:
        id: uuid.UUID
        hashed_password: str
        is_active: bool
        email: str
        is_superuser: bool
        is_verified: bool
    else:
        id: Mapped[uuid.UUID] = mapped_column(
            Uuid, primary_key=True, default=uuid.uuid4
        )
        hashed_password: Mapped[str]
        is_active: Mapped[bool]

        @property
        def email(self) -> str:
            return self.login

        @email.setter
        def email(self, value: str) -> None:
            self.login = value

        @property
        def is_superuser(self) -> bool:
            return self.role == _ROLE_ADMIN

        @is_superuser.setter
        def is_superuser(self, value: bool) -> None:
            self.role = _ROLE_ADMIN if value else _ROLE_VIEWER

        @property
        def is_verified(self) -> bool:
            return self.is_active

        @is_verified.setter
        def is_verified(self, value: bool) -> None:
            self.is_active = value

    login: Mapped[str] = mapped_column(unique=True)
    role: Mapped[str]
    registered_at: Mapped[datetime]
    approved_at: Mapped[datetime | None]
    approved_by: Mapped[str | None]


class AppSettingRow(Base):
    """
    Override динамической настройки (§12.8, M5/C T024): key-value JSON
    поверх файла. ``key`` — имя поля ``DynamicSettings`` (PK); ``value`` —
    JSON-сериализация значения; ``updated_by`` — автор изменения (аудит,
    источник «override» в ``/ui/settings``). Отсутствие строки = «нет
    override'а, действует значение TOML+env».
    """

    __tablename__ = 'app_settings'

    key: Mapped[str] = mapped_column(primary_key=True)
    value: Mapped[str]
    updated_at: Mapped[datetime]
    updated_by: Mapped[str | None]


class OutboxCommandRow(Base):
    """
    Команда командного outbox (§12.9, M5/C T024): персистентная форма
    ``OutboxCommand``. ``uid`` — PK (handle для ``take``/``mark``);
    ``payload`` / ``result`` — JSON-текст; ``status`` —
    pending/taken/done/failed строкой. Захват и пометка — атомарные
    ``UPDATE ... WHERE status=...`` (at-least-once, §12.9). FIFO —
    по ``created_at`` + ``rowid``; индекс по ``status`` ускоряет
    поллинг pending'ов. ``taken_at`` — момент захвата (lease-маркер для
    reaper зависших ``taken``, T027).
    """

    __tablename__ = 'outbox_commands'
    __table_args__ = (Index('ix_outbox_commands_status', 'status'),)

    uid: Mapped[str] = mapped_column(primary_key=True)
    kind: Mapped[str]
    payload: Mapped[str]
    status: Mapped[str]
    created_at: Mapped[datetime]
    taken_at: Mapped[datetime | None]
    executed_at: Mapped[datetime | None]
    result: Mapped[str | None]
    error: Mapped[str | None]
