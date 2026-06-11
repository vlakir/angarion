"""
SC-3: InMemory-плагин («нулевой пациент») регистрируется entry
point'ами самой библиотеки и загружается через ``importlib.metadata``,
без прямого импорта модуля плагина.
"""

from __future__ import annotations

from importlib.metadata import entry_points

from angarion.domain.plugin import (
    AdapterPlugin,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)
from angarion.domain.ports import EventQueuePort, ProcessorPort


def load_single(group: str, name: str) -> object:
    (ep,) = entry_points(group=group, name=name)
    return ep.load()


def test_memory_adapter_plugin_loads_via_entry_point() -> None:
    plugin = load_single('angarion.adapters', 'memory')
    assert isinstance(plugin, AdapterPlugin)
    assert plugin.name == 'memory'
    assert plugin.capabilities.history_fetch is False
    assert plugin.capabilities.push_transport == 'client'


def test_memory_queue_backend_loads_and_builds_port() -> None:
    backend = load_single('angarion.queues', 'memory')
    assert isinstance(backend, QueueBackend)
    assert backend.name == 'memory'
    queue = backend.make(None)
    assert isinstance(queue, EventQueuePort)


def test_memory_storage_backend_loads_and_builds_bundle() -> None:
    backend = load_single('angarion.storages', 'memory')
    assert isinstance(backend, StorageBackend)
    assert backend.name == 'memory'
    bundle = backend.make(None)
    assert isinstance(bundle, StorageBundle)


def test_passthrough_processor_loads_via_entry_point() -> None:
    proc = load_single('angarion.processors', 'passthrough')
    assert isinstance(proc, ProcessorPort)
    assert proc.name == 'passthrough'


def test_memory_account_model_validates_section() -> None:
    """Схема [accounts.*] платформы memory: только messenger (§12.11)."""
    plugin = load_single('angarion.adapters', 'memory')
    assert isinstance(plugin, AdapterPlugin)
    account = plugin.account_config_model.model_validate({'messenger': 'memory'})
    assert account.model_dump() == {'messenger': 'memory'}
