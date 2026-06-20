"""
SC-3: InMemory-плагин («нулевой пациент») регистрируется entry
point'ами самой библиотеки и загружается через ``importlib.metadata``,
без прямого импорта модуля плагина.
"""

from __future__ import annotations

import asyncio
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from angarion.config import QueueConfig, StorageConfig
from angarion.domain.plugin import (
    AdapterPlugin,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)
from angarion.domain.ports import EventQueuePort, ProcessorPort

if TYPE_CHECKING:
    from pathlib import Path


def load_single(group: str, name: str) -> object:
    (ep,) = entry_points(group=group, name=name)
    return ep.load()


def test_memory_adapter_plugin_loads_via_entry_point() -> None:
    plugin = load_single('angarion.adapters', 'memory')
    assert isinstance(plugin, AdapterPlugin)
    assert plugin.name == 'memory'
    assert plugin.capabilities.history_fetch is False
    assert plugin.capabilities.push_transport == 'client'


def test_telegram_adapter_plugin_loads_via_entry_point() -> None:
    """SC-3 (M3): telegram-плагин — наравне с InMemory, через entry point."""
    plugin = load_single('angarion.adapters', 'telegram')
    assert isinstance(plugin, AdapterPlugin)
    assert plugin.name == 'telegram'
    assert plugin.capabilities.history_fetch is True
    assert plugin.capabilities.push_transport == 'client'
    account = plugin.account_config_model.model_validate(
        {'transport': 'telegram', 'api_id': 2040, 'api_hash': 'h'}
    )
    assert account.model_dump()['api_id'] == 2040


def test_memory_queue_backend_loads_and_builds_port() -> None:
    backend = load_single('angarion.queues', 'memory')
    assert isinstance(backend, QueueBackend)
    assert backend.name == 'memory'
    queue = backend.make(None)
    assert isinstance(queue, EventQueuePort)


def test_persistqueue_backend_loads_and_builds_port(tmp_path: Path) -> None:
    """FR-2 спеки T003: [queue] backend="persistqueue" + path."""
    backend = load_single('angarion.queues', 'persistqueue')
    assert isinstance(backend, QueueBackend)
    assert backend.name == 'persistqueue'
    config = QueueConfig.model_validate(
        {'backend': 'persistqueue', 'path': str(tmp_path / 'queue.db')}
    )
    queue = backend.make(config)
    assert isinstance(queue, EventQueuePort)
    queue.close()


def test_memory_storage_backend_loads_and_builds_bundle() -> None:
    backend = load_single('angarion.storages', 'memory')
    assert isinstance(backend, StorageBackend)
    assert backend.name == 'memory'
    bundle = backend.make(None)
    assert isinstance(bundle, StorageBundle)


def test_sqlite_storage_backend_loads_and_builds_bundle(tmp_path: Path) -> None:
    """FR-8 спеки T003: [storage] backend="sqlite" + path."""
    backend = load_single('angarion.storages', 'sqlite')
    assert isinstance(backend, StorageBackend)
    assert backend.name == 'sqlite'
    config = StorageConfig.model_validate(
        {'backend': 'sqlite', 'path': str(tmp_path / 'app.db')}
    )
    bundle = backend.make(config)
    assert isinstance(bundle, StorageBundle)
    asyncio.run(bundle.dispose())


def test_passthrough_processor_loads_via_entry_point() -> None:
    proc = load_single('angarion.processors', 'passthrough')
    assert isinstance(proc, ProcessorPort)
    assert proc.name == 'passthrough'


def test_memory_account_model_validates_section() -> None:
    """Схема [accounts.*] платформы memory: только transport (§12.11)."""
    plugin = load_single('angarion.adapters', 'memory')
    assert isinstance(plugin, AdapterPlugin)
    account = plugin.account_config_model.model_validate({'transport': 'memory'})
    assert account.model_dump() == {'transport': 'memory'}
