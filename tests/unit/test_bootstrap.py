"""
Bootstrap (FR-12, FR-13, FR-15): загрузка плагинов, двухступенчатая
валидация, матрица возможностей §12.10 (SC-2) и сборка AngarionApp.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel, ConfigDict

from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.plugin import QUEUE_BACKEND, STORAGE_BACKEND
from angarion.adapters.memory.sink import MemorySink
from angarion.application import processors
from angarion.application.processors import FunctionProcessor
from angarion.bootstrap import (
    AngarionApp,
    LoadedPlugins,
    _DispatchSink,
    build_app,
    load_plugins,
    load_processors,
)
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    CatchupConfig,
    EndpointConfig,
    PipelineConfig,
    WorkerConfig,
)
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.errors import ConfigError, DeliveryError
from angarion.domain.models import (
    AccountRef,
    Address,
    EventKind,
    OutboundMessage,
)
from angarion.domain.plugin import AdapterPlugin

SRC_CHAT = '-100111'
DST_CHAT = '-100222'


def make_outbound() -> OutboundMessage:
    return OutboundMessage(
        idempotency_key='k->digest:-100999:0',
        target=Address(messenger='memory', chat_id='-100999'),
        send_via=AccountRef(messenger='memory', account_id='main'),
        text='hi',
    )


def make_settings(
    *,
    accounts: dict[str, AccountConfig] | None = None,
    pipelines: dict[str, PipelineConfig] | None = None,
    **overrides: object,
) -> AngarionSettings:
    fields: dict[str, object] = {
        'accounts': accounts
        if accounts is not None
        else {'main': AccountConfig(messenger='memory')},
        'worker': WorkerConfig(poll_interval=0.01),
        'pipelines': pipelines
        if pipelines is not None
        else {'digest': make_pipeline()},
    }
    fields.update(overrides)
    return AngarionSettings.model_validate(fields)


def make_pipeline(**overrides: object) -> PipelineConfig:
    fields: dict[str, object] = {
        'processor': 'passthrough',
        'events': frozenset({EventKind.MESSAGE_NEW}),
        'sources': (EndpointConfig(account='main', chat_id=SRC_CHAT),),
        'targets': (EndpointConfig(account='main', chat_id=DST_CHAT),),
    }
    fields.update(overrides)
    return PipelineConfig.model_validate(fields)


class StubAccountConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra='allow')

    messenger: str


class StubListener:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def catchup(self, source_key: str) -> None:
        pass


def stub_plugins(**caps_overrides: object) -> LoadedPlugins:
    """Синтетический плагин с урезанными возможностями (plan 2.6, SC-2)."""
    caps: dict[str, object] = {
        'user_account': True,
        'edit_events': True,
        'delete_events': True,
        'history_fetch': True,
        'threads': True,
        'push_transport': 'client',
    }
    caps.update(caps_overrides)
    plugin = AdapterPlugin(
        name='stub',
        capabilities=AdapterCapabilities.model_validate(caps),
        account_config_model=StubAccountConfig,
        make_listener=lambda *_args: StubListener(),
        make_sender=lambda *_args: MemorySink(),
    )
    return LoadedPlugins(
        adapters={'stub': plugin},
        queues={'memory': QUEUE_BACKEND},
        storages={'memory': STORAGE_BACKEND},
    )


def stub_settings(pipeline: PipelineConfig) -> AngarionSettings:
    return make_settings(
        accounts={'main': AccountConfig(messenger='stub')},
        pipelines={'digest': pipeline},
    )


class FakeEntryPoint:
    """Минимальный duck-type entry point: bootstrap нужны name и load()."""

    def __init__(self, name: str, obj: object) -> None:
        self.name = name
        self._obj = obj

    def load(self) -> object:
        return self._obj


def patch_entry_points(
    monkeypatch: pytest.MonkeyPatch, target_group: str, eps: list[FakeEntryPoint]
) -> None:
    def fake_entry_points(*, group: str) -> list[FakeEntryPoint]:
        return eps if group == target_group else []

    monkeypatch.setattr('angarion.bootstrap.entry_points', fake_entry_points)


class TestPluginLoading:
    def test_load_plugins_discovers_memory_in_all_groups(self) -> None:
        plugins = load_plugins()
        assert plugins.adapters['memory'].name == 'memory'
        assert plugins.queues['memory'].name == 'memory'
        assert plugins.storages['memory'].name == 'memory'

    def test_load_processors_is_idempotent(self) -> None:
        load_processors()
        load_processors()

    def test_wrong_object_type_in_group_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        patch_entry_points(
            monkeypatch, 'angarion.adapters', [FakeEntryPoint('bad', object())]
        )
        with pytest.raises(ConfigError, match='AdapterPlugin'):
            load_plugins()

    def test_duplicate_plugin_name_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        plugin = stub_plugins().adapters['stub']
        patch_entry_points(
            monkeypatch,
            'angarion.adapters',
            [FakeEntryPoint('a', plugin), FakeEntryPoint('b', plugin)],
        )
        with pytest.raises(ConfigError, match='дубликат'):
            load_plugins()

    def test_processor_entry_point_wrong_type_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        patch_entry_points(
            monkeypatch, 'angarion.processors', [FakeEntryPoint('bad', object())]
        )
        with pytest.raises(ConfigError, match='ProcessorPort'):
            load_processors()

    def test_external_processor_registered_from_entry_point(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def fn(event: object, ctx: object, svc: object) -> object:
            raise NotImplementedError

        external = FunctionProcessor(name='ext_proc', fn=fn)
        monkeypatch.setattr(
            processors, '_registry', dict(processors.registered())
        )
        patch_entry_points(
            monkeypatch, 'angarion.processors', [FakeEntryPoint('ext_proc', external)]
        )
        load_processors()
        assert processors.get_processor('ext_proc') is external


class TestAccountValidation:
    def test_unknown_messenger_fails_with_known_list(self) -> None:
        settings = make_settings(
            accounts={'main': AccountConfig(messenger='ghost')}, pipelines={}
        )
        with pytest.raises(ConfigError, match="ghost.*memory"):
            build_app(settings)

    def test_account_section_validated_by_plugin_model(self) -> None:
        """Лишний ключ для memory-аккаунта (extra='forbid' у плагина)."""
        bad = AccountConfig.model_validate({'messenger': 'memory', 'token': 'x'})
        settings = make_settings(accounts={'main': bad}, pipelines={})
        with pytest.raises(ConfigError, match='main'):
            build_app(settings)


class TestBackendResolution:
    def test_unknown_queue_backend(self) -> None:
        settings = make_settings(queue={'backend': 'kafka'})
        with pytest.raises(ConfigError, match='kafka.*memory'):
            build_app(settings)

    def test_unknown_storage_backend(self) -> None:
        settings = make_settings(storage={'backend': 'sqlite'})
        with pytest.raises(ConfigError, match='sqlite.*memory'):
            build_app(settings)


class TestReferentialIntegrity:
    def test_unknown_processor(self) -> None:
        settings = make_settings(
            pipelines={'digest': make_pipeline(processor='ghost')}
        )
        with pytest.raises(ConfigError, match='ghost'):
            build_app(settings)

    def test_source_references_unknown_account(self) -> None:
        pipeline = make_pipeline(
            sources=(EndpointConfig(account='ghost', chat_id=SRC_CHAT),)
        )
        settings = make_settings(pipelines={'digest': pipeline})
        with pytest.raises(ConfigError, match='ghost'):
            build_app(settings)

    def test_target_references_unknown_account(self) -> None:
        pipeline = make_pipeline(
            targets=(EndpointConfig(account='ghost', chat_id=DST_CHAT),)
        )
        settings = make_settings(pipelines={'digest': pipeline})
        with pytest.raises(ConfigError, match='ghost'):
            build_app(settings)


class TestCapabilitiesMatrix:
    """SC-2: четыре ветки деградации §12.10 (webhook-ветка — M5, A-1)."""

    def test_edited_subscription_without_edit_events_fails(self) -> None:
        pipeline = make_pipeline(
            events=frozenset({EventKind.MESSAGE_EDITED}),
        )
        with pytest.raises(ConfigError, match='edit_events'):
            build_app(
                stub_settings(pipeline), plugins=stub_plugins(edit_events=False)
            )

    def test_deleted_subscription_without_delete_events_fails(self) -> None:
        pipeline = make_pipeline(events=frozenset({EventKind.MESSAGE_DELETED}))
        with pytest.raises(ConfigError, match='delete_events'):
            build_app(
                stub_settings(pipeline), plugins=stub_plugins(delete_events=False)
            )

    def test_thread_id_without_threads_fails(self) -> None:
        pipeline = make_pipeline(
            sources=(
                EndpointConfig(account='main', chat_id=SRC_CHAT, thread_id='7'),
            )
        )
        with pytest.raises(ConfigError, match='thread'):
            build_app(stub_settings(pipeline), plugins=stub_plugins(threads=False))

    def test_thread_id_in_target_without_threads_fails(self) -> None:
        pipeline = make_pipeline(
            targets=(
                EndpointConfig(account='main', chat_id=DST_CHAT, thread_id='7'),
            )
        )
        with pytest.raises(ConfigError, match='thread'):
            build_app(stub_settings(pipeline), plugins=stub_plugins(threads=False))

    async def test_history_fetch_false_degrades_catchup(self) -> None:
        """Платформа memory честно без истории → catchup_unavailable."""
        app = build_app(make_settings())
        expected_key = f'memory:main:{SRC_CHAT}'
        assert app.catchup_unavailable == (expected_key,)
        await app.start()
        try:
            events = await app.storage.analytics.recent(kind='catchup_unavailable')
            assert [e.payload['source_key'] for e in events] == [expected_key]
        finally:
            await app.stop()

    def test_catchup_disabled_silences_degradation(self) -> None:
        app = build_app(make_settings(catchup=CatchupConfig(enabled=False)))
        assert app.catchup_unavailable == ()


class TestPluginFactoryContracts:
    """Фабрики плагина обязаны отдавать объекты протоколов (§12.11)."""

    def test_listener_factory_must_return_listener(self) -> None:
        plugins = stub_plugins()
        broken = plugins.adapters['stub'].model_copy(
            update={'make_listener': lambda *_args: object()}
        )
        with pytest.raises(ConfigError, match='Listener'):
            build_app(
                stub_settings(make_pipeline()),
                plugins=plugins.model_copy(update={'adapters': {'stub': broken}}),
            )

    def test_sender_factory_must_return_sink(self) -> None:
        plugins = stub_plugins()
        broken = plugins.adapters['stub'].model_copy(
            update={'make_sender': lambda *_args: object()}
        )
        with pytest.raises(ConfigError, match='MessageSinkPort'):
            build_app(
                stub_settings(make_pipeline()),
                plugins=plugins.model_copy(update={'adapters': {'stub': broken}}),
            )


class TestAppAssembly:
    def test_build_app_wires_memory_platform(self) -> None:
        app = build_app(make_settings())
        assert isinstance(app, AngarionApp)
        assert isinstance(app.listeners[0], MemoryListener)
        assert isinstance(app.sinks['memory'], MemorySink)

    async def test_end_to_end_emit_to_delivery(self) -> None:
        """Мини-сквозной прогон: emit → ingest → worker → outbox → sink."""
        app = build_app(make_settings())
        await app.start()
        try:
            listener = app.listeners[0]
            assert isinstance(listener, MemoryListener)
            await listener.emit(
                {'chat_id': SRC_CHAT, 'external_id': '1', 'text': 'hi'}
            )
            sink = app.sinks['memory']
            assert isinstance(sink, MemorySink)
            for _ in range(500):
                if sink.sent:
                    break
                await asyncio.sleep(0.01)
            assert sink.sent[0].text == 'hi'
            assert sink.sent[0].target.chat_id == DST_CHAT
        finally:
            await app.stop()

    async def test_listeners_started_and_stopped(self) -> None:
        app = build_app(make_settings())
        listener = app.listeners[0]
        assert isinstance(listener, MemoryListener)
        await app.start()
        assert listener.started
        await app.stop()
        assert not listener.started

    async def test_double_start_is_rejected(self) -> None:
        app = build_app(make_settings())
        await app.start()
        try:
            with pytest.raises(RuntimeError):
                await app.start()
        finally:
            await app.stop()

    async def test_stop_without_start_is_noop(self) -> None:
        app = build_app(make_settings())
        await app.stop()


class TestDispatchSink:
    async def test_routes_by_messenger(self) -> None:
        sink = MemorySink()
        dispatch = _DispatchSink({'memory': sink})
        receipt = await dispatch.send(make_outbound())
        assert receipt.external_id == '1'
        assert sink.sent

    async def test_unknown_messenger_raises_delivery_error(self) -> None:
        dispatch = _DispatchSink({})
        with pytest.raises(DeliveryError, match='memory'):
            await dispatch.send(make_outbound())
