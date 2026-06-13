"""
Контракты плагинов (§12.11 ТЗ): адаптер платформы, альтернативные
очереди и хранилища.

Поле ``webhook_router`` сознательно отсутствует в M1 (A-1 спеки
T002): оно типизировано классом FastAPI и появится аддитивно в M5
вместе с HTTP-адаптером; ядро остаётся без инфраструктурных
зависимостей (§14.9).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from collections.abc import Callable
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.models import Messenger
from angarion.domain.ports import (
    AnalyticsPort,
    CursorStorePort,
    DeadLetterPort,
    DedupStorePort,
    EventQueuePort,
    MessageRegistryPort,
    MessageSinkPort,
    OutboxPort,
    SessionStorePort,
    StateStorePort,
)


@runtime_checkable
class Listener(Protocol):
    """
    Формализованный жизненный цикл driving-адаптера (§12.11).

    Listener получает ``IngestService`` через deps фабрики и эмитит в
    него ``InboundEvent``'ы, собранные публичными хелперами ключей
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

SenderFactory = Callable[..., MessageSinkPort]
"""Фабрика sender'а: ``(deps, accounts) -> MessageSinkPort``."""


class AdapterPlugin(BaseModel):
    """
    Объект, предоставляемый плагином в entry point ``angarion.adapters``.

    Конструкция композиции (A-2): JSON-контракт DTO на неё не
    распространяется; frozen — сохраняем.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: Messenger
    capabilities: AdapterCapabilities
    account_config_model: type[BaseModel]
    """Pydantic-схема секции ``[accounts.*]`` этой платформы."""
    make_listener: ListenerFactory
    make_sender: SenderFactory


class StorageBundle(BaseModel):
    """
    Комплект driven-портов хранения, который собирает storage-бэкенд
    (§12.11, plan 2.5). Объём M1 — порты C-1 (+ outbox после C-9);
    ``session`` добавлен в M3 (T005) под сессии аккаунтов платформы;
    ``runtime_config`` придёт аддитивно в M5.

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
