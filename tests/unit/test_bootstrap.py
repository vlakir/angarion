"""
Bootstrap (FR-12, FR-13, FR-15): загрузка плагинов, двухступенчатая
валидация, матрица возможностей §12.10 (SC-2) и сборка AngarionApp.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import BaseModel, ConfigDict

from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.plugin import QUEUE_BACKEND, STORAGE_BACKEND
from angarion.adapters.memory.sink import MemorySink
from angarion.application import processors
from angarion.application.manual import ManualEvent, build_manual_record
from angarion.application.processors import FunctionProcessor
from angarion.bootstrap import (
    AngarionApp,
    LoadedPlugins,
    _catchup_degradation,
    _DispatchSink,
    _loop_guard_sources,
    _maybe_dispose,
    _validate_accounts,
    build_app,
    load_plugins,
    load_processors,
)
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    CatchupConfig,
    EndpointConfig,
    MediaConfig,
    PipelineConfig,
    WorkerConfig,
)
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.errors import ConfigError, DeliveryError
from angarion.domain.models import (
    AccountRef,
    Endpoint,
    OutboundRecord,
    RecordKind,
)
from angarion.domain.plugin import AdapterPlugin

if TYPE_CHECKING:
    from pathlib import Path

SRC_CHAT = '-100111'
DST_CHAT = '-100222'


def make_outbound() -> OutboundRecord:
    return OutboundRecord(
        idempotency_key='k->digest:-100999:0',
        target=Endpoint(transport='memory', address='-100999'),
        send_via=AccountRef(transport='memory', account_id='main'),
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
        else {'main': AccountConfig(transport='memory')},
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
        'events': frozenset({RecordKind.NEW}),
        'sources': (EndpointConfig(account='main', address=SRC_CHAT),),
        'targets': (EndpointConfig(account='main', address=DST_CHAT),),
    }
    fields.update(overrides)
    return PipelineConfig.model_validate(fields)


class StubAccountConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra='allow')

    transport: str


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
        accounts={'main': AccountConfig(transport='stub')},
        pipelines={'digest': pipeline},
    )


class FakeEntryPoint:
    """Минимальный duck-type entry point: bootstrap нужны name и load()."""

    def __init__(
        self, name: str, obj: object, error: Exception | None = None
    ) -> None:
        self.name = name
        self._obj = obj
        self._error = error

    def load(self) -> object:
        if self._error is not None:
            raise self._error
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

    def test_entry_point_with_missing_extra_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Entry point, чья зависимость не установлена (extra C-1 T003),
        пропускается с предупреждением, а не роняет load_plugins().
        """
        broken = FakeEntryPoint(
            'persistqueue',
            None,
            error=ModuleNotFoundError("No module named 'persistqueue'"),
        )
        patch_entry_points(monkeypatch, 'angarion.queues', [broken])
        plugins = load_plugins()
        assert 'persistqueue' not in plugins.queues

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

    def test_processor_entry_point_with_missing_extra_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Процессор-entry point без extra (llm без httpx) — skip + warning."""
        registry = {
            name: proc
            for name, proc in processors.registered().items()
            if name != 'llm'
        }
        monkeypatch.setattr(processors, '_registry', registry)
        broken = FakeEntryPoint(
            'llm', None, error=ModuleNotFoundError("No module named 'httpx'")
        )
        patch_entry_points(monkeypatch, 'angarion.processors', [broken])
        load_processors()  # не падает
        assert 'llm' not in processors.registered()

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
            accounts={'main': AccountConfig(transport='ghost')}, pipelines={}
        )
        with pytest.raises(ConfigError, match="ghost.*memory"):
            build_app(settings)

    def test_account_section_validated_by_plugin_model(self) -> None:
        """Лишний ключ для memory-аккаунта (extra='forbid' у плагина)."""
        bad = AccountConfig.model_validate({'transport': 'memory', 'token': 'x'})
        settings = make_settings(accounts={'main': bad}, pipelines={})
        with pytest.raises(ConfigError, match='main'):
            build_app(settings)


class TestBackendResolution:
    def test_unknown_queue_backend(self) -> None:
        settings = make_settings(queue={'backend': 'kafka'})
        with pytest.raises(ConfigError, match='kafka.*memory'):
            build_app(settings)

    def test_unknown_storage_backend(self) -> None:
        settings = make_settings(storage={'backend': 'postgres'})
        with pytest.raises(ConfigError, match='postgres.*memory'):
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
            sources=(EndpointConfig(account='ghost', address=SRC_CHAT),)
        )
        settings = make_settings(pipelines={'digest': pipeline})
        with pytest.raises(ConfigError, match='ghost'):
            build_app(settings)

    def test_target_references_unknown_account(self) -> None:
        pipeline = make_pipeline(
            targets=(EndpointConfig(account='ghost', address=DST_CHAT),)
        )
        settings = make_settings(pipelines={'digest': pipeline})
        with pytest.raises(ConfigError, match='ghost'):
            build_app(settings)


class TestProcessorConfigValidation:
    """FR-0 (T021): ``processor_config`` валидируется на старте, до событий."""

    def test_invalid_processor_config_fails_fast(self) -> None:
        """Лишний ключ в config процессора ``template`` (extra='forbid')."""
        pipeline = make_pipeline(
            processor='template',
            processor_config={'template': '{{ text }}', 'bogus': 1},
        )
        with pytest.raises(ConfigError, match='processor_config.*template'):
            build_app(make_settings(pipelines={'digest': pipeline}))

    def test_missing_required_key_fails_fast(self) -> None:
        """Отсутствие обязательного ``template`` → падение при build_app."""
        pipeline = make_pipeline(
            processor='template', processor_config={'edited': '{{ text }}'}
        )
        with pytest.raises(ConfigError, match='processor_config'):
            build_app(make_settings(pipelines={'digest': pipeline}))

    def test_valid_processor_config_builds(self) -> None:
        pipeline = make_pipeline(
            processor='template', processor_config={'template': '{{ text }}'}
        )
        app = build_app(make_settings(pipelines={'digest': pipeline}))
        assert isinstance(app, AngarionApp)

    def test_processor_without_config_model_skips_validation(self) -> None:
        """``passthrough`` (config_model() → None): любой config игнорируется."""
        pipeline = make_pipeline(
            processor='passthrough', processor_config={'anything': 'goes'}
        )
        app = build_app(make_settings(pipelines={'digest': pipeline}))
        assert isinstance(app, AngarionApp)


class TestCapabilitiesMatrix:
    """SC-2: четыре ветки деградации §12.10 (webhook-ветка — M5, A-1)."""

    def test_edited_subscription_without_edit_events_fails(self) -> None:
        pipeline = make_pipeline(
            events=frozenset({RecordKind.EDITED}),
        )
        with pytest.raises(ConfigError, match='edit_events'):
            build_app(
                stub_settings(pipeline), plugins=stub_plugins(edit_events=False)
            )

    def test_deleted_subscription_without_delete_events_fails(self) -> None:
        pipeline = make_pipeline(events=frozenset({RecordKind.DELETED}))
        with pytest.raises(ConfigError, match='delete_events'):
            build_app(
                stub_settings(pipeline), plugins=stub_plugins(delete_events=False)
            )

    def test_thread_id_without_threads_fails(self) -> None:
        pipeline = make_pipeline(
            sources=(
                EndpointConfig(account='main', address=SRC_CHAT, thread_id='7'),
            )
        )
        with pytest.raises(ConfigError, match='thread'):
            build_app(stub_settings(pipeline), plugins=stub_plugins(threads=False))

    def test_thread_id_in_target_without_threads_fails(self) -> None:
        pipeline = make_pipeline(
            targets=(
                EndpointConfig(account='main', address=DST_CHAT, thread_id='7'),
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
        with pytest.raises(ConfigError, match='SinkPort'):
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
                {'address': SRC_CHAT, 'external_id': '1', 'text': 'hi'}
            )
            sink = app.sinks['memory']
            assert isinstance(sink, MemorySink)
            for _ in range(500):
                if sink.sent:
                    break
                await asyncio.sleep(0.01)
            assert sink.sent[0].text == 'hi'
            assert sink.sent[0].target.address == DST_CHAT
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


class _PruneSpy:
    def __init__(self) -> None:
        self.calls = 0

    async def prune(self, _older_than: object) -> int:
        self.calls += 1
        return 0


class _PurgeSpy:
    def __init__(self) -> None:
        self.calls: list[int] = []

    async def purge_acked(self, keep_latest: int) -> int:
        self.calls.append(keep_latest)
        return 0


class TestPruneAndDispose:
    """§17.3: фоновая prune-задача рантайма + закрытие ресурсов хранилища."""

    async def test_prune_once_skips_unbounded_retention(self) -> None:
        spy = _PruneSpy()
        await AngarionApp._prune_once(datetime.now(UTC), 0, spy)
        assert spy.calls == 0

    async def test_prune_once_runs_for_bounded_retention(self) -> None:
        spy = _PruneSpy()
        await AngarionApp._prune_once(datetime.now(UTC), 7, spy)
        assert spy.calls == 1

    async def test_purge_queue_acked_skips_when_unbounded(self) -> None:
        """keep_acked=0 — acked-строки храним бессрочно (§17.3, T016)."""
        spy = _PurgeSpy()
        await AngarionApp._purge_queue_acked(spy, 0)
        assert spy.calls == []

    async def test_purge_queue_acked_runs_for_positive_keep(self) -> None:
        spy = _PurgeSpy()
        await AngarionApp._purge_queue_acked(spy, 1000)
        assert spy.calls == [1000]

    def test_prune_media_skips_unbounded_retention(self, tmp_path: Path) -> None:
        """retention_days=0 — храним бессрочно (файлы не трогаем)."""
        stale = tmp_path / 'old.bin'
        stale.write_bytes(b'x')
        old = (datetime.now(UTC) - timedelta(days=100)).timestamp()
        os.utime(stale, (old, old))
        AngarionApp._prune_media(
            datetime.now(UTC),
            MediaConfig(retention_days=0, storage_dir=str(tmp_path)),
        )
        assert stale.exists()

    def test_prune_media_missing_dir_is_noop(self, tmp_path: Path) -> None:
        """Отсутствующий каталог — нечего чистить, без ошибки."""
        AngarionApp._prune_media(
            datetime.now(UTC),
            MediaConfig(retention_days=7, storage_dir=str(tmp_path / 'nope')),
        )

    def test_prune_media_deletes_old_keeps_fresh(self, tmp_path: Path) -> None:
        """Файлы старше retention_days удаляются, свежие остаются."""
        now = datetime.now(UTC)
        old_file = tmp_path / 'old.bin'
        fresh_file = tmp_path / 'fresh.bin'
        old_file.write_bytes(b'x')
        fresh_file.write_bytes(b'y')
        old = (now - timedelta(days=30)).timestamp()
        fresh = (now - timedelta(days=1)).timestamp()
        os.utime(old_file, (old, old))
        os.utime(fresh_file, (fresh, fresh))
        AngarionApp._prune_media(
            now, MediaConfig(retention_days=7, storage_dir=str(tmp_path))
        )
        assert not old_file.exists()
        assert fresh_file.exists()

    async def test_no_prune_task_when_interval_zero(self) -> None:
        app = build_app(make_settings())  # prune_interval = 0 по умолчанию
        await app.start()
        try:
            assert len(app._tasks) == 3  # worker + delivery + outbox-consumer
        finally:
            await app.stop()

    async def test_prune_task_created_and_invokes_store(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = build_app(
            make_settings(
                worker=WorkerConfig(poll_interval=0.01, prune_interval=0.01)
            )
        )
        calls: list[object] = []

        async def spy(older_than: object) -> int:
            calls.append(older_than)
            return 0

        monkeypatch.setattr(app.storage.dedup, 'prune', spy)
        await app.start()
        try:
            assert len(app._tasks) == 4  # worker + delivery + outbox + prune
            for _ in range(200):
                if calls:
                    break
                await asyncio.sleep(0.01)
        finally:
            await app.stop()
        assert calls

    async def test_maybe_dispose_calls_dispose_when_present(self) -> None:
        disposed: list[bool] = []

        class _WithDispose:
            async def dispose(self) -> None:
                disposed.append(True)

        await _maybe_dispose(cast('object', _WithDispose()))  # type: ignore[arg-type]
        assert disposed == [True]

    async def test_maybe_dispose_noop_without_method(self) -> None:
        await _maybe_dispose(cast('object', object()))  # type: ignore[arg-type]


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


def _chain_settings() -> AngarionSettings:
    """Цепочка P1 →(internal stage1)→ P2 поверх реальных плагинов."""
    return AngarionSettings.model_validate(
        {
            'accounts': {
                'main': AccountConfig(transport='memory'),
                'wire': AccountConfig(transport='internal'),
            },
            'pipelines': {
                'p1': PipelineConfig(
                    processor='passthrough',
                    events=frozenset({RecordKind.NEW}),
                    sources=(EndpointConfig(account='main', address=SRC_CHAT),),
                    targets=(EndpointConfig(account='wire', address='stage1'),),
                ),
                'p2': PipelineConfig(
                    processor='passthrough',
                    events=frozenset({RecordKind.NEW}),
                    sources=(EndpointConfig(account='wire', address='stage1'),),
                    targets=(EndpointConfig(account='main', address=DST_CHAT),),
                ),
            },
        }
    )


class TestInternalWireBootstrap:
    """T037: внутренний транспорт в швах bootstrap (A1, A8)."""

    def test_internal_edge_not_loop_guarded(self) -> None:
        """A1: internal source==target по построению — но guard его НЕ берёт."""
        settings = _chain_settings()
        accounts = _validate_accounts(settings, load_plugins().adapters)
        assert _loop_guard_sources(settings, accounts) == ()

    def test_real_loop_still_guarded_alongside_internal(self) -> None:
        """Реальную петлю (memory source==target) guard ловит, internal — нет."""
        feed = '-100333'
        settings = AngarionSettings.model_validate(
            {
                'accounts': {
                    'main': AccountConfig(transport='memory'),
                    'wire': AccountConfig(transport='internal'),
                },
                'pipelines': {
                    'echo': PipelineConfig(
                        processor='passthrough',
                        events=frozenset({RecordKind.NEW}),
                        sources=(EndpointConfig(account='main', address=SRC_CHAT),),
                        targets=(EndpointConfig(account='main', address=SRC_CHAT),),
                    ),
                    'p1': PipelineConfig(
                        processor='passthrough',
                        events=frozenset({RecordKind.NEW}),
                        sources=(EndpointConfig(account='main', address=feed),),
                        targets=(EndpointConfig(account='wire', address='stage1'),),
                    ),
                    'p2': PipelineConfig(
                        processor='passthrough',
                        events=frozenset({RecordKind.NEW}),
                        sources=(EndpointConfig(account='wire', address='stage1'),),
                        targets=(EndpointConfig(account='main', address=DST_CHAT),),
                    ),
                },
            }
        )
        accounts = _validate_accounts(settings, load_plugins().adapters)
        guarded = _loop_guard_sources(settings, accounts)
        assert [(g.transport, g.address) for g in guarded] == [('memory', SRC_CHAT)]

    def test_internal_source_excluded_from_catchup_degradation(self) -> None:
        """A8: internal-источник не числится в catchup-недоступных."""
        settings = _chain_settings()
        accounts = _validate_accounts(settings, load_plugins().adapters)
        degraded = _catchup_degradation(settings, accounts)
        assert all('internal' not in key for key in degraded)


class TestManualTrigger:
    """Программный ручной триггер (T038, фаза 1): submit_event / run_pipeline."""

    @staticmethod
    def _event(**overrides: object) -> ManualEvent:
        fields: dict[str, object] = {
            'source': Endpoint(transport='memory', address=SRC_CHAT),
            'text': 'hi',
        }
        fields.update(overrides)
        return ManualEvent.model_validate(fields)

    async def test_submit_event_routes_and_enqueues(self) -> None:
        app = build_app(make_settings())
        await app.submit_event(self._event())
        assert (await app.queue.depth()).pending == 1
        item = await app.queue.get()
        assert item.envelope.pipeline == 'digest'
        assert item.envelope.record.origin == 'manual'
        assert item.envelope.record.source.address == SRC_CHAT

    async def test_submit_event_accepts_ready_record(self) -> None:
        app = build_app(make_settings())
        record = build_manual_record(self._event())
        await app.submit_event(record)
        item = await app.queue.get()
        assert item.envelope.record.uid == record.uid

    async def test_submit_event_forces_manual_origin_on_ready_record(self) -> None:
        # готовый Record с чужим origin → ручной путь приводит к 'manual'
        app = build_app(make_settings())
        record = build_manual_record(self._event()).model_copy(
            update={'origin': 'live'}
        )
        await app.submit_event(record)
        item = await app.queue.get()
        assert item.envelope.record.origin == 'manual'

    async def test_submit_event_idempotency_key_dedups(self) -> None:
        app = build_app(make_settings())
        await app.submit_event(self._event(idempotency_key='cli-1'))
        await app.submit_event(self._event(idempotency_key='cli-1'))
        assert (await app.queue.depth()).pending == 1

    async def test_submit_event_without_key_processes_each_call(self) -> None:
        app = build_app(make_settings())
        await app.submit_event(self._event())
        await app.submit_event(self._event())
        assert (await app.queue.depth()).pending == 2

    async def test_run_pipeline_stages_bypassing_router(self) -> None:
        app = build_app(make_settings())
        # источник '-999' не совпадает ни с одним маршрутом — но прямой
        # запуск router минует, поэтому envelope всё равно ставится
        event = self._event(source=Endpoint(transport='memory', address='-999'))
        await app.run_pipeline('digest', event)
        item = await app.queue.get()
        assert item.envelope.pipeline == 'digest'
        assert item.envelope.record.origin == 'manual'
        assert (await app.queue.depth()).pending == 0

    async def test_run_pipeline_unknown_name_raises(self) -> None:
        app = build_app(make_settings())
        with pytest.raises(ValueError, match='ghost'):
            await app.run_pipeline('ghost', self._event())
