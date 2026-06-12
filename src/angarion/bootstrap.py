"""
Composition root (FR-15, plan 2.9): сборка конвейера из конфигурации.

Последовательность (§12.11): загрузка плагинов из entry points →
**вторая ступень валидации** (первая, структурная — ``angarion.config``):
секции аккаунтов — моделями плагинов, ссылочная целостность
пайплайнов, резолв бэкендов очереди/хранилища по реестрам, сверка с
матрицей возможностей §12.10 (FR-13) → провязка
ingest / worker / delivery / listeners в контейнер ``AngarionApp``.

CLI здесь нет (M3, C-5): приложение получает ``AngarionApp`` из
``build_app()`` и управляет жизненным циклом через ``start()/stop()``.

``AccountRef.account_id`` в M1 — имя секции ``[accounts.*]``;
платформенный id появится с реальными адаптерами (M3).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from importlib.metadata import entry_points
from typing import Final, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, PrivateAttr, ValidationError

from angarion.application import processors
from angarion.application.delivery import DeliveryWorker
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.application.worker import PipelineBinding, PipelineWorker
from angarion.config import AngarionSettings, EndpointConfig, PipelineConfig
from angarion.domain.errors import ConfigError, DeliveryError
from angarion.domain.keys import make_source_key
from angarion.domain.models import (
    AccountRef,
    Address,
    AnalyticsEvent,
    DeliveryReceipt,
    EventKind,
    OutboundMessage,
    PipelineContextData,
    TargetSpec,
)
from angarion.domain.plugin import (
    AdapterPlugin,
    Listener,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)
from angarion.domain.ports import EventQueuePort, MessageSinkPort, ProcessorPort
from angarion.log import get_logger

ADAPTERS_GROUP: Final = 'angarion.adapters'
QUEUES_GROUP: Final = 'angarion.queues'
STORAGES_GROUP: Final = 'angarion.storages'
PROCESSORS_GROUP: Final = 'angarion.processors'

_log = get_logger('angarion.bootstrap')


class AdapterDeps(BaseModel):
    """
    Зависимости, передаваемые фабрикам плагина (§12.11): listener
    эмитит события в ``ingest``. Конструкция композиции (A-2).
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    ingest: IngestService


class LoadedPlugins(BaseModel):
    """Реестры загруженных entry points (FR-12). Конструкция композиции."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    adapters: dict[str, AdapterPlugin]
    queues: dict[str, QueueBackend]
    storages: dict[str, StorageBackend]


class _Account(BaseModel):
    """Аккаунт после второй ступени валидации: плагин + модель плагина."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    plugin: AdapterPlugin
    config: BaseModel


class _NamedPluginObject(Protocol):
    """Общий вид объектов entry points: у всех есть имя для реестра."""

    @property
    def name(self) -> str: ...


def _load_group[T: _NamedPluginObject](group: str, expected: type[T]) -> dict[str, T]:
    """
    Загрузить entry points группы в реестр по ``obj.name`` (FR-12).

    Entry point с неустановленной зависимостью (опциональный extra,
    C-1 T003) пропускается с предупреждением: библиотека регистрирует
    бэкенды вроде ``persistqueue`` в собственном ``pyproject.toml``, и
    без extra их модули не импортируются.
    """
    registry: dict[str, T] = {}
    for ep in entry_points(group=group):
        try:
            obj = ep.load()
        except ModuleNotFoundError as exc:
            _log.warning(
                'entry_point_unavailable', group=group, name=ep.name, error=str(exc)
            )
            continue
        if not isinstance(obj, expected):
            msg = (
                f'entry point {ep.name!r} группы {group!r} должен быть '
                f'{expected.__name__}, получен {type(obj).__name__}'
            )
            raise ConfigError(msg)
        if obj.name in registry:
            msg = f'дубликат имени {obj.name!r} в entry points группы {group!r}'
            raise ConfigError(msg)
        registry[obj.name] = obj
    return registry


def load_plugins() -> LoadedPlugins:
    """Загрузить реестры адаптеров, очередей и хранилищ (§12.11)."""
    return LoadedPlugins(
        adapters=_load_group(ADAPTERS_GROUP, AdapterPlugin),
        queues=_load_group(QUEUES_GROUP, QueueBackend),
        storages=_load_group(STORAGES_GROUP, StorageBackend),
    )


def load_processors() -> None:
    """
    Зарегистрировать процессоры из entry points (§10.1, FR-17).

    Идемпотентно: объект, уже зарегистрированный под своим именем
    (например, встроенный ``passthrough``, регистрируемый при импорте
    модуля), пропускается; чужой объект под занятым именем —
    ``ConfigError`` из реестра.
    """
    for ep in entry_points(group=PROCESSORS_GROUP):
        obj = ep.load()
        if not isinstance(obj, ProcessorPort):
            msg = (
                f'entry point {ep.name!r} группы {PROCESSORS_GROUP!r} '
                f'не удовлетворяет ProcessorPort: {type(obj).__name__}'
            )
            raise ConfigError(msg)
        if processors.registered().get(obj.name) is obj:
            continue
        processors.register(obj)


def _validate_accounts(
    settings: AngarionSettings, adapters: dict[str, AdapterPlugin]
) -> dict[str, _Account]:
    """Аккаунты: messenger по реестру плагинов + схема плагина (FR-2, FR-12)."""
    accounts: dict[str, _Account] = {}
    for name, section in settings.accounts.items():
        plugin = adapters.get(section.messenger)
        if plugin is None:
            known = ', '.join(sorted(adapters)) or '<пусто>'
            msg = (
                f'аккаунт {name!r}: неизвестный messenger '
                f'{section.messenger!r}; зарегистрированы: {known}'
            )
            raise ConfigError(msg)
        try:
            config = plugin.account_config_model.model_validate(section.model_dump())
        except ValidationError as exc:
            msg = f'аккаунт {name!r} не проходит схему платформы {plugin.name!r}: {exc}'
            raise ConfigError(msg) from exc
        accounts[name] = _Account(name=name, plugin=plugin, config=config)
    return accounts


def _resolve_backend[T: _NamedPluginObject](
    registry: dict[str, T], backend: str, what: str
) -> T:
    """Резолв ``backend = "..."`` по реестру entry points (plan 2.5)."""
    resolved = registry.get(backend)
    if resolved is None:
        known = ', '.join(sorted(registry)) or '<пусто>'
        msg = f'неизвестный бэкенд {what} {backend!r}; зарегистрированы: {known}'
        raise ConfigError(msg)
    return resolved


def _endpoint_account(
    pipeline: str, role: str, ep: EndpointConfig, accounts: dict[str, _Account]
) -> _Account:
    """Ссылочная целостность: источник/цель указывает на известный аккаунт."""
    account = accounts.get(ep.account)
    if account is None:
        known = ', '.join(sorted(accounts)) or '<пусто>'
        msg = (
            f'пайплайн {pipeline!r}: {role} ссылается на неизвестный '
            f'аккаунт {ep.account!r}; известны: {known}'
        )
        raise ConfigError(msg)
    if ep.thread_id is not None and not account.plugin.capabilities.threads:
        msg = (
            f'пайплайн {pipeline!r}: задан thread_id {ep.thread_id!r}, но '
            f'платформа {account.plugin.name!r} не поддерживает threads (§12.10)'
        )
        raise ConfigError(msg)
    return account


def _check_subscription(pipeline: str, cfg: PipelineConfig, account: _Account) -> None:
    """Невыполнимая подписка → fail-fast (§12.10, FR-13)."""
    caps = account.plugin.capabilities
    requirements = (
        (EventKind.MESSAGE_EDITED, caps.edit_events, 'edit_events'),
        (EventKind.MESSAGE_DELETED, caps.delete_events, 'delete_events'),
    )
    for kind, supported, flag in requirements:
        if kind in cfg.events and not supported:
            msg = (
                f'пайплайн {pipeline!r}: подписка на {kind} невыполнима — '
                f'платформа {account.plugin.name!r} не даёт {flag} (§12.10)'
            )
            raise ConfigError(msg)


def _build_pipelines(
    settings: AngarionSettings, accounts: dict[str, _Account]
) -> tuple[list[RouteSpec], dict[str, PipelineBinding]]:
    """Маршруты Router'а + связки worker'а из секций ``[pipelines.*]``."""
    routes: list[RouteSpec] = []
    bindings: dict[str, PipelineBinding] = {}
    for name, cfg in settings.pipelines.items():
        processor = processors.get_processor(cfg.processor)
        sources: list[Address] = []
        for ep in cfg.sources:
            account = _endpoint_account(name, 'источник', ep, accounts)
            _check_subscription(name, cfg, account)
            sources.append(
                Address(
                    messenger=account.plugin.name,
                    chat_id=ep.chat_id,
                    thread_id=ep.thread_id,
                )
            )
        targets: list[TargetSpec] = []
        for ep in cfg.targets:
            account = _endpoint_account(name, 'цель', ep, accounts)
            targets.append(
                TargetSpec(
                    target=Address(
                        messenger=account.plugin.name,
                        chat_id=ep.chat_id,
                        thread_id=ep.thread_id,
                    ),
                    send_via=AccountRef(
                        messenger=account.plugin.name, account_id=ep.account
                    ),
                )
            )
        routes.append(
            RouteSpec(
                pipeline=name,
                events=cfg.events,
                sources=tuple(sources),
                only_replies=cfg.only_replies,
            )
        )
        bindings[name] = PipelineBinding(
            processor=processor,
            ctx=PipelineContextData(
                pipeline=name, targets=targets, settings=cfg.processor_config
            ),
        )
    return routes, bindings


def _catchup_degradation(
    settings: AngarionSettings, accounts: dict[str, _Account]
) -> tuple[str, ...]:
    """
    Источники, по которым catch-up невозможен (``history_fetch=False``,
    §12.10): отключение + предупреждение при старте, не падение.
    """
    if not settings.catchup.enabled:
        return ()
    degraded: set[str] = set()
    for cfg in settings.pipelines.values():
        for ep in cfg.sources:
            account = accounts[ep.account]
            if not account.plugin.capabilities.history_fetch:
                degraded.add(
                    make_source_key(
                        account.plugin.name, ep.account, ep.chat_id, ep.thread_id
                    )
                )
    return tuple(sorted(degraded))


class _DispatchSink:
    """Маршрутизация исходящих по платформе ``send_via.messenger``."""

    def __init__(self, sinks: dict[str, MessageSinkPort]) -> None:
        self._sinks = dict(sinks)

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        """Делегировать отправку sink'у платформы сообщения."""
        sink = self._sinks.get(msg.send_via.messenger)
        if sink is None:
            known = ', '.join(sorted(self._sinks)) or '<пусто>'
            msg_text = (
                f'нет sender для платформы {msg.send_via.messenger!r}; '
                f'доступны: {known}'
            )
            raise DeliveryError(msg_text)
        return await sink.send(msg)


def _make_platform_adapters(
    settings: AngarionSettings,
    accounts: dict[str, _Account],
    deps: AdapterDeps,
) -> tuple[tuple[Listener, ...], dict[str, MessageSinkPort]]:
    """Listener и sender на каждую платформу, имеющую аккаунты (§12.11)."""
    by_messenger: dict[str, dict[str, _Account]] = {}
    for account in accounts.values():
        by_messenger.setdefault(account.plugin.name, {})[account.name] = account
    listeners: list[Listener] = []
    sinks: dict[str, MessageSinkPort] = {}
    for messenger, platform_accounts in by_messenger.items():
        plugin = next(iter(platform_accounts.values())).plugin
        account_models = {
            name: account.config for name, account in platform_accounts.items()
        }
        platform_sources = [
            ep
            for cfg in settings.pipelines.values()
            for ep in cfg.sources
            if accounts[ep.account].plugin.name == messenger
        ]
        listener = plugin.make_listener(deps, account_models, platform_sources)
        if not isinstance(listener, Listener):
            msg = (
                f'фабрика make_listener плагина {messenger!r} вернула объект '
                f'вне протокола Listener: {type(listener).__name__}'
            )
            raise ConfigError(msg)
        sink = plugin.make_sender(deps, account_models)
        if not isinstance(sink, MessageSinkPort):
            msg = (
                f'фабрика make_sender плагина {messenger!r} вернула объект '
                f'вне порта MessageSinkPort: {type(sink).__name__}'
            )
            raise ConfigError(msg)
        listeners.append(listener)
        sinks[messenger] = sink
    return tuple(listeners), sinks


class AngarionApp(BaseModel):
    """
    Контейнер собранного конвейера (plan 2.9; CLI — M3).

    ``start()``: ``queue.recover()`` (§8) → объявление деградации
    catch-up (FR-13) → asyncio-задачи worker'а и delivery →
    listener'ы. ``stop()`` — зеркально и graceful: стоп приёма,
    затем отмена циклов (текущий item дообрабатывается).

    Конструкция композиции (A-2): JSON-контракт DTO не действует.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    settings: AngarionSettings
    ingest: IngestService
    worker: PipelineWorker
    delivery: DeliveryWorker
    listeners: tuple[Listener, ...]
    queue: EventQueuePort
    storage: StorageBundle
    sinks: dict[str, MessageSinkPort]
    catchup_unavailable: tuple[str, ...]

    _tasks: list[asyncio.Task[None]] = PrivateAttr(default_factory=list)

    async def start(self) -> None:
        """Запустить конвейер; повторный запуск без stop — ошибка."""
        if self._tasks:
            msg = 'AngarionApp уже запущен'
            raise RuntimeError(msg)
        await self._announce_degradation()
        await self.queue.recover()
        self._tasks = [
            asyncio.create_task(self.worker.run(), name='angarion-worker'),
            asyncio.create_task(self.delivery.run(), name='angarion-delivery'),
        ]
        for listener in self.listeners:
            await listener.start()

    async def stop(self) -> None:
        """Graceful-остановка: приём → циклы; идемпотентна."""
        for listener in self.listeners:
            await listener.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks = []

    async def _announce_degradation(self) -> None:
        """§12.10: предупреждение в лог + ``catchup_unavailable`` в аналитику."""
        for source_key in self.catchup_unavailable:
            _log.warning('catchup_unavailable', source_key=source_key)
            await self.storage.analytics.record(
                AnalyticsEvent(
                    uid=uuid4(),
                    kind='catchup_unavailable',
                    payload={'source_key': source_key},
                    at=datetime.now(UTC),
                )
            )


def build_app(
    settings: AngarionSettings, *, plugins: LoadedPlugins | None = None
) -> AngarionApp:
    """
    Собрать конвейер из конфигурации (FR-15).

    ``plugins`` по умолчанию загружаются из entry points; параметр
    позволяет тестам подставлять синтетические плагины с урезанными
    возможностями (plan 2.6). Все нарушения второй ступени валидации —
    ``ConfigError`` (fail-fast §11).
    """
    registry = plugins if plugins is not None else load_plugins()
    load_processors()
    accounts = _validate_accounts(settings, registry.adapters)
    queue = _resolve_backend(registry.queues, settings.queue.backend, 'очереди').make(
        settings.queue
    )
    storage = _resolve_backend(
        registry.storages, settings.storage.backend, 'хранилища'
    ).make(settings.storage)
    routes, bindings = _build_pipelines(settings, accounts)
    ingest = IngestService(
        dedup=storage.dedup,
        registry=storage.registry,
        router=Router(routes),
        queue=queue,
        analytics=storage.analytics,
    )
    listeners, sinks = _make_platform_adapters(
        settings, accounts, AdapterDeps(ingest=ingest)
    )
    worker = PipelineWorker(
        queue=queue,
        outbox=storage.outbox,
        analytics=storage.analytics,
        dead_letters=storage.dead_letters,
        state=storage.state,
        pipelines=bindings,
        max_retries=settings.worker.max_retries,
        backoff_base=settings.worker.backoff_base,
        backoff_cap=settings.worker.backoff_cap,
        log=get_logger('angarion.worker'),
    )
    delivery = DeliveryWorker(
        outbox=storage.outbox,
        sink=_DispatchSink(sinks),
        analytics=storage.analytics,
        max_retries=settings.worker.max_retries,
        backoff_base=settings.worker.backoff_base,
        backoff_cap=settings.worker.backoff_cap,
        poll_interval=settings.worker.poll_interval,
        log=get_logger('angarion.delivery'),
    )
    return AngarionApp(
        settings=settings,
        ingest=ingest,
        worker=worker,
        delivery=delivery,
        listeners=listeners,
        queue=queue,
        storage=storage,
        sinks=sinks,
        catchup_unavailable=_catchup_degradation(settings, accounts),
    )
