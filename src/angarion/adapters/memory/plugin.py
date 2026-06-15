"""
Плагин платформы «memory» (§12.4, plan 2.6) — «нулевой пациент»
механизма расширения §12.11: полный комплект entry points
(``angarion.adapters`` / ``angarion.queues`` / ``angarion.storages``)
в ``pyproject.toml`` самой библиотеки.

``history_fetch=False`` — честно (истории нет) и заодно постоянно
прогоняет ветку деградации ``catchup_unavailable`` (FR-13) в обычном
запуске.

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime; типы bootstrap/config в
сигнатурах фабрик — строками (TYPE_CHECKING).
"""

from typing import TYPE_CHECKING, Final, Literal

from pydantic import BaseModel, ConfigDict

from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.sink import MemorySink
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCommandOutbox,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
    MemoryOutbox,
    MemoryRuntimeConfig,
    MemorySessionStore,
    MemoryStateStore,
)
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.plugin import (
    AdapterPlugin,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from angarion.bootstrap import AdapterDeps
    from angarion.config import EndpointConfig, QueueConfig, StorageConfig

MEMORY_CAPABILITIES: Final = AdapterCapabilities(
    user_account=True,
    edit_events=True,
    delete_events=True,
    history_fetch=False,
    threads=True,
    push_transport='client',
)
"""Матрица возможностей платформы memory (§12.10, plan 2.6)."""


class MemoryAccountConfig(BaseModel):
    """Секция ``[accounts.*]`` платформы memory: только ``messenger``."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    messenger: Literal['memory']


def _make_listener(
    deps: 'AdapterDeps',
    accounts: 'Mapping[str, BaseModel]',
    _sources: 'Sequence[EndpointConfig]',
) -> MemoryListener:
    """Фабрика listener'а (§12.11): один listener на все аккаунты memory."""
    return MemoryListener(ingest=deps.ingest, account_ids=tuple(accounts))


def _make_sender(
    _deps: 'AdapterDeps', _accounts: 'Mapping[str, BaseModel]'
) -> MemorySink:
    """Фабрика sender'а (§12.11): журнал отправленного."""
    return MemorySink()


def _make_queue(_config: 'QueueConfig') -> MemoryQueue:
    """Фабрика очереди для entry point ``angarion.queues`` (plan 2.5)."""
    return MemoryQueue()


def _make_storage(_config: 'StorageConfig') -> StorageBundle:
    """Фабрика комплекта хранилищ для entry point ``angarion.storages``."""
    return StorageBundle(
        dedup=MemoryDedupStore(),
        outbox=MemoryOutbox(),
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        state=MemoryStateStore(),
        analytics=MemoryAnalytics(),
        dead_letters=MemoryDeadLetters(),
        session=MemorySessionStore(),
        runtime_config=MemoryRuntimeConfig(),
        command_outbox=MemoryCommandOutbox(),
    )


PLUGIN: Final = AdapterPlugin(
    name='memory',
    capabilities=MEMORY_CAPABILITIES,
    account_config_model=MemoryAccountConfig,
    make_listener=_make_listener,
    make_sender=_make_sender,
)
"""Значение entry point ``angarion.adapters:memory``."""

QUEUE_BACKEND: Final = QueueBackend(name='memory', make=_make_queue)
"""Значение entry point ``angarion.queues:memory``."""

STORAGE_BACKEND: Final = StorageBackend(name='memory', make=_make_storage)
"""Значение entry point ``angarion.storages:memory``."""
