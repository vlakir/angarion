"""Контракт плагина адаптера (§12.10–12.11, FR-1): capabilities + AdapterPlugin."""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
    MemoryOutbox,
    MemorySessionStore,
    MemoryStateStore,
)
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.errors import NotSupportedError
from angarion.domain.plugin import (
    AdapterPlugin,
    Listener,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)


def make_capabilities(**overrides: object) -> AdapterCapabilities:
    fields: dict[str, object] = {
        'user_account': True,
        'edit_events': True,
        'delete_events': True,
        'history_fetch': True,
        'threads': True,
        'push_transport': 'client',
    }
    fields.update(overrides)
    return AdapterCapabilities.model_validate(fields)


class DummyAccountConfig(BaseModel):
    token: str


class DummyListener:
    def __init__(self) -> None:
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def catchup(self, source_key: str) -> None:
        raise NotSupportedError(source_key)


def make_plugin(**overrides: object) -> AdapterPlugin:
    fields: dict[str, object] = {
        'name': 'dummy',
        'capabilities': make_capabilities(),
        'account_config_model': DummyAccountConfig,
        'make_listener': lambda *args, **kwargs: DummyListener(),
        'make_sender': lambda *args, **kwargs: object(),
    }
    fields.update(overrides)
    return AdapterPlugin.model_validate(fields)


class TestAdapterCapabilities:
    def test_all_flags_required(self) -> None:
        with pytest.raises(ValidationError):
            AdapterCapabilities.model_validate({'user_account': True})

    def test_frozen(self) -> None:
        caps = make_capabilities()
        with pytest.raises(ValidationError):
            caps.user_account = False

    def test_push_transport_is_open_string(self) -> None:
        """§12.10: транспорт — открытая строка, не enum."""
        caps = make_capabilities(push_transport='my_custom_bus')
        assert caps.push_transport == 'my_custom_bus'


class TestListenerProtocol:
    def test_structural_conformance(self) -> None:
        listener: Listener = DummyListener()
        assert isinstance(listener, Listener)

    def test_non_conforming_object_rejected(self) -> None:
        assert not isinstance(object(), Listener)

    async def test_lifecycle_and_catchup(self) -> None:
        listener = DummyListener()
        await listener.start()
        assert listener.started
        await listener.stop()
        assert not listener.started
        with pytest.raises(NotSupportedError):
            await listener.catchup('telegram:acc1:-100123')


class TestAdapterPlugin:
    def test_constructs_with_factories(self) -> None:
        plugin = make_plugin()
        assert plugin.name == 'dummy'
        assert plugin.account_config_model is DummyAccountConfig
        listener = plugin.make_listener()
        assert isinstance(listener, DummyListener)

    def test_name_follows_messenger_pattern(self) -> None:
        with pytest.raises(ValidationError):
            make_plugin(name='Bad Name!')

    def test_frozen(self) -> None:
        plugin = make_plugin()
        with pytest.raises(ValidationError):
            plugin.name = 'other'

    def test_webhook_router_defaults_none(self) -> None:
        """M5 (T022): поле есть, по умолчанию None (адаптеры без webhook)."""
        assert make_plugin().webhook_router is None

    def test_webhook_router_accepts_arbitrary_object(self) -> None:
        """Типизировано Any (а не APIRouter) ради §14.9 — ядро fastapi-free."""
        sentinel = object()
        assert make_plugin(webhook_router=sentinel).webhook_router is sentinel

    def test_account_config_model_validates_accounts(self) -> None:
        """Схема секции [accounts.*] принадлежит плагину (§12.11)."""
        plugin = make_plugin()
        account = plugin.account_config_model.model_validate({'token': 'secret'})
        assert isinstance(account, DummyAccountConfig)
        with pytest.raises(ValidationError):
            plugin.account_config_model.model_validate({'unknown': 1})


def make_bundle(**overrides: object) -> StorageBundle:
    fields: dict[str, object] = {
        'dedup': MemoryDedupStore(),
        'outbox': MemoryOutbox(),
        'registry': MemoryMessageRegistry(),
        'cursors': MemoryCursorStore(),
        'state': MemoryStateStore(),
        'analytics': MemoryAnalytics(),
        'dead_letters': MemoryDeadLetters(),
        'session': MemorySessionStore(),
    }
    fields.update(overrides)
    return StorageBundle.model_validate(fields)


class TestBackendContracts:
    """Контракты entry points angarion.queues / angarion.storages (plan 2.5)."""

    def test_storage_bundle_holds_all_driven_storage_ports(self) -> None:
        bundle = make_bundle()
        assert isinstance(bundle.dedup, MemoryDedupStore)
        assert isinstance(bundle.dead_letters, MemoryDeadLetters)

    def test_storage_bundle_rejects_non_port_object(self) -> None:
        with pytest.raises(ValidationError):
            make_bundle(dedup=object())

    def test_storage_bundle_is_frozen_and_closed(self) -> None:
        bundle = make_bundle()
        with pytest.raises(ValidationError):
            setattr(bundle, 'dedup', MemoryDedupStore())
        with pytest.raises(ValidationError):
            make_bundle(runtime_config=object())

    def test_queue_backend_carries_named_factory(self) -> None:
        backend = QueueBackend(name='memory', make=lambda _config: MemoryQueue())
        queue = backend.make(None)
        assert isinstance(queue, MemoryQueue)
        with pytest.raises(ValidationError):
            setattr(backend, 'name', 'other')

    def test_storage_backend_carries_named_factory(self) -> None:
        backend = StorageBackend(name='memory', make=lambda _config: make_bundle())
        bundle = backend.make(None)
        assert isinstance(bundle, StorageBundle)
