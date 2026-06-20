"""
Контракты плагинов (§12.11 ТЗ): адаптер платформы, альтернативные
очереди и хранилища.

Поле ``webhook_router`` добавлено в M5 (T022) аддитивно: типизировано
``Any`` (а не классом FastAPI), чтобы ядро осталось без
инфраструктурных зависимостей (§14.9) — http-адаптер приводит его к
``APIRouter`` при монтировании (ADR 2026-06-14, продолжение решения
2026-06-11).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.models import Transport
from angarion.domain.ports import (
    AnalyticsPort,
    CommandOutboxPort,
    CursorStorePort,
    DeadLetterPort,
    DedupStorePort,
    EventQueuePort,
    MessageRegistryPort,
    OutboxPort,
    RuntimeConfigPort,
    SessionStorePort,
    SinkPort,
    StateStorePort,
)


@runtime_checkable
class Listener(Protocol):
    """
    Формализованный жизненный цикл driving-адаптера (§12.11).

    Listener получает ``IngestService`` через deps фабрики и эмитит в
    него ``Record``'ы, собранные публичными хелперами ключей
    (§7.2).
    """

    async def start(self) -> None:
        """Подключение, catch-up, live-подписка."""
        ...

    async def stop(self) -> None:
        """Graceful shutdown (вызывается ядром)."""
        ...

    async def catchup(self, source_key: str) -> None:
        """Ручной catch-up; при ``history_fetch=False`` — NotSupportedError."""
        ...


ListenerFactory = Callable[..., Listener]
"""Фабрика listener'а: ``(deps, accounts, sources) -> Listener``."""

SenderFactory = Callable[..., SinkPort]
"""Фабрика sender'а: ``(deps, accounts) -> SinkPort``."""


class LoginContext(BaseModel):
    """
    Контекст интерактивного логина аккаунта (``angarion login``; M7 B1,
    T010).

    Логин платформо-специфичен (Telegram — номер/код/2FA, Matrix —
    homeserver/пароль), поэтому шов принадлежит плагину, а не CLI (как
    ``make_listener``/``make_sender``). Плагин получает **непрозрачный**
    ``SessionStorePort`` и ключ шифрования, сам оборачивает хранилище
    своим at-rest-декоратором и персистит сессию — ядро/CLI остаются
    платформо-агностичными. ``config`` — уже провалидированная моделью
    плагина секция ``[accounts.*]``.

    Конструкция композиции (A-2): JSON-контракт DTO не действует;
    ``session`` — Protocol-инстанс, отсюда ``arbitrary_types_allowed``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    account_id: str
    config: BaseModel
    session: SessionStorePort
    session_key: str


LoginFactory = Callable[[LoginContext], Awaitable[None]]
"""Фабрика интерактивного логина: получить сессию у платформы и сохранить
её зашифрованной в ``SessionStorePort`` (``angarion login``)."""


class AdapterPlugin(BaseModel):
    """
    Объект, предоставляемый плагином в entry point ``angarion.adapters``.

    Конструкция композиции (A-2): JSON-контракт DTO на неё не
    распространяется; frozen — сохраняем.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: Transport
    capabilities: AdapterCapabilities
    account_config_model: type[BaseModel]
    """Pydantic-схема секции ``[accounts.*]`` этой платформы."""
    make_listener: ListenerFactory
    make_sender: SenderFactory
    make_login: LoginFactory | None = None
    """
    Фабрика интерактивного логина (``angarion login``, M7 B1): платформа
    получает сессию (пароль/код → токен) и сохраняет её зашифрованной в
    ``SessionStorePort``. ``None`` — платформа без логина (InMemory,
    webhook-only); ``angarion login`` для неё — ``ConfigError``.
    """
    webhook_router: Any = None
    """
    Роутер webhook-listener'а платформы (``push_transport="webhook"``,
    §12.11): монтируется ``create_app`` http-адаптера. Тип — ``Any``
    (фактически ``fastapi.APIRouter | None``): ядро не зависит от FastAPI
    (§14.9, ADR 2026-06-14), потребитель приводит к ``APIRouter``.
    """


class StorageBundle(BaseModel):
    """
    Комплект driven-портов хранения, который собирает storage-бэкенд
    (§12.11, plan 2.5). Объём M1 — порты C-1 (+ outbox после C-9);
    ``session`` добавлен в M3 (T005) под сессии аккаунтов платформы;
    ``runtime_config`` — в M5 (T024) под динамические настройки §12.8,
    ``command_outbox`` — там же под командный мост api→pipeline §12.9.

    Конструкция композиции (A-2): JSON-контракт DTO на неё не
    распространяется; frozen — сохраняем.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    dedup: DedupStorePort
    outbox: OutboxPort
    registry: MessageRegistryPort
    cursors: CursorStorePort
    state: StateStorePort
    analytics: AnalyticsPort
    dead_letters: DeadLetterPort
    session: SessionStorePort
    runtime_config: RuntimeConfigPort
    command_outbox: CommandOutboxPort


QueueFactory = Callable[..., EventQueuePort]
"""Фабрика очереди: ``(config: QueueConfig) -> EventQueuePort``."""

StorageFactory = Callable[..., StorageBundle]
"""Фабрика комплекта хранилищ: ``(config: StorageConfig) -> StorageBundle``."""


class QueueBackend(BaseModel):
    """
    Объект entry point ``angarion.queues`` (plan 2.5): именованная
    фабрика ``EventQueuePort``. Значение ``[queue].backend`` резолвится
    по ``name`` в реестре загруженных entry points.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    make: QueueFactory


class StorageBackend(BaseModel):
    """
    Объект entry point ``angarion.storages`` (plan 2.5): именованная
    фабрика ``StorageBundle``. Значение ``[storage].backend`` резолвится
    по ``name`` в реестре загруженных entry points.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    make: StorageFactory
