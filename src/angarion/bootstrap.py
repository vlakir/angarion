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
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from importlib.metadata import entry_points
from typing import Final, Protocol
from uuid import uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationError,
)

from angarion.application import processors
from angarion.application.delivery import DeliveryWorker
from angarion.application.ingest import IngestService
from angarion.application.loop_guard import GuardedSource, LoopGuardSink
from angarion.application.outbox_consumer import OutboxConsumer
from angarion.application.router import Router, RouteSpec
from angarion.application.worker import PipelineBinding, PipelineWorker
from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    MediaConfig,
    PipelineConfig,
)
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
    эмитит события в ``ingest``; реальным адаптерам (Telegram, M3) нужны
    ещё driven-порты (``storage``) и конфигурация (``settings``).

    ``shared`` — скретчпад на одну сборку (общий для всех вызовов фабрик):
    плагин мемоизирует в нём объекты, которые listener и sender обязаны
    делить (Telegram — единый ``ClientRegistry``, §12.1). Конструкция
    композиции (A-2); ``shared`` mutable, поэтому ``frozen`` не мешает.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    ingest: IngestService
    storage: StorageBundle
    settings: AngarionSettings
    shared: dict[str, object] = Field(default_factory=dict)


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


class _Prunable(Protocol):
    """Driven-порт с ретеншн-очисткой (§17.3): dedup/registry/analytics/outbox."""

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить записи старше порога; вернуть число удалённых."""


async def _maybe_dispose(storage: StorageBundle) -> None:
    """Закрыть ресурсы хранилища, если бэкенд их держит (sqlite — пул движка)."""
    dispose = getattr(storage, 'dispose', None)
    if callable(dispose):
        await dispose()


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
    (встроенные ``passthrough``/``template``, регистрируемые при импорте
    модуля), пропускается; чужой объект под занятым именем —
    ``ConfigError`` из реестра.

    Entry point с неустановленной зависимостью (опциональный extra,
    например ``llm`` без ``angarion[llm]``) пропускается с
    предупреждением, как в ``_load_group`` (C-1 T003): модуль процессора
    без extra не импортируется.
    """
    for ep in entry_points(group=PROCESSORS_GROUP):
        try:
            obj = ep.load()
        except ModuleNotFoundError as exc:
            _log.warning(
                'entry_point_unavailable',
                group=PROCESSORS_GROUP,
                name=ep.name,
                error=str(exc),
            )
            continue
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


def _validate_processor_config(
    pipeline: str, cfg: PipelineConfig, processor: ProcessorPort
) -> None:
    """
    FR-0 (T021): валидировать ``processor_config`` на старте через хук
    ``config_model`` процессора — невалидный конфиг падает ``ConfigError``
    при ``build_app``, до приёма событий, а не на первом событии.
    """
    model = processor.config_model()
    if model is None:
        return
    try:
        model.model_validate(cfg.processor_config)
    except ValidationError as exc:
        msg = (
            f'пайплайн {pipeline!r}: processor_config процессора '
            f'{cfg.processor!r} не проходит схему: {exc}'
        )
        raise ConfigError(msg) from exc


def _build_pipelines(
    settings: AngarionSettings, accounts: dict[str, _Account]
) -> tuple[list[RouteSpec], dict[str, PipelineBinding]]:
    """Маршруты Router'а + связки worker'а из секций ``[pipelines.*]``."""
    routes: list[RouteSpec] = []
    bindings: dict[str, PipelineBinding] = {}
    for name, cfg in settings.pipelines.items():
        processor = processors.get_processor(cfg.processor)
        _validate_processor_config(name, cfg, processor)
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
            forward_media=cfg.forward_media,
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


def _make_catchup(
    listeners: tuple[Listener, ...],
) -> Callable[[str], Awaitable[None]]:
    """
    Колбэк ручного catch-up для outbox-consumer'а (§12.9): пробует
    listener'ы по очереди, пока чей-то ``catchup`` не примет источник
    (нерезолвленный → ``KeyError``, пробуем следующий). Ни один не принял
    → ``KeyError`` наружу (consumer пометит команду ``failed``).
    """

    async def run_catchup(source_key: str) -> None:
        for listener in listeners:
            try:
                await listener.catchup(source_key)
            except KeyError:
                continue
            else:
                return
        msg = f'источник не резолвлен ни одним listener: {source_key}'
        raise KeyError(msg)

    return run_catchup


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


def _loop_guard_sources(
    settings: AngarionSettings, accounts: dict[str, _Account]
) -> tuple[GuardedSource, ...]:
    """
    Источники, совпадающие хотя бы с одной целью (петля ``source==target``).

    Сверка идентичности — по ``(messenger, chat_id, thread_id)``; для
    пометки dedup нужен и ``account_id``, поэтому возвращаем сам источник.
    Пусто, если ни одна цель не совпадает с источником — guard не нужен.
    """
    targets = {
        (accounts[ep.account].plugin.name, ep.chat_id, ep.thread_id)
        for cfg in settings.pipelines.values()
        for ep in cfg.targets
    }
    guarded: dict[tuple[str, str, str, str | None], GuardedSource] = {}
    for cfg in settings.pipelines.values():
        for ep in cfg.sources:
            messenger = accounts[ep.account].plugin.name
            if (messenger, ep.chat_id, ep.thread_id) in targets:
                key = (messenger, ep.account, ep.chat_id, ep.thread_id)
                guarded[key] = GuardedSource(
                    messenger=messenger,
                    account_id=ep.account,
                    chat_id=ep.chat_id,
                    thread_id=ep.thread_id,
                )
    return tuple(guarded.values())


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
    outbox_consumer: OutboxConsumer
    listeners: tuple[Listener, ...]
    queue: EventQueuePort
    storage: StorageBundle
    sinks: dict[str, MessageSinkPort]
    catchup_unavailable: tuple[str, ...]
    restart_event: asyncio.Event

    _tasks: list[asyncio.Task[None]] = PrivateAttr(default_factory=list)

    async def start(self) -> None:
        """Запустить конвейер; повторный запуск без stop — ошибка."""
        if self._tasks:
            msg = 'AngarionApp уже запущен'
            raise RuntimeError(msg)
        await self._announce_degradation()
        await self.queue.recover()
        # listener'ы стартуют ДО циклов worker/delivery: реальные адаптеры
        # (Telegram) на старте подключают клиентов, а catch-up кладёт в
        # очередь — она работает без работающего worker'а, события копятся
        for listener in self.listeners:
            await listener.start()
        self._tasks = [
            asyncio.create_task(self.worker.run(), name='angarion-worker'),
            asyncio.create_task(self.delivery.run(), name='angarion-delivery'),
            asyncio.create_task(self.outbox_consumer.run(), name='angarion-outbox'),
        ]
        prune_task = self._make_prune_task()
        if prune_task is not None:
            self._tasks.append(prune_task)

    async def stop(self) -> None:
        """Graceful-остановка: приём → циклы → закрытие хранилища; идемпотентна."""
        for listener in self.listeners:
            await listener.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks = []
        await _maybe_dispose(self.storage)

    def _make_prune_task(self) -> asyncio.Task[None] | None:
        """Фоновая prune-задача ретеншна §17.3 (``prune_interval`` = 0 → выкл.)."""
        interval = self.settings.worker.prune_interval
        if interval <= 0:
            return None
        return asyncio.create_task(self._prune_loop(interval), name='angarion-prune')

    async def _prune_loop(self, interval: float) -> None:
        """Периодическая очистка по окнам ретеншна (§17.3; ``0`` — бессрочно)."""
        storage_cfg = self.settings.storage
        while True:
            await asyncio.sleep(interval)
            now = datetime.now(UTC)
            await self._prune_once(now, storage_cfg.dedup_ttl_days, self.storage.dedup)
            await self._prune_once(
                now, storage_cfg.registry_window_days, self.storage.registry
            )
            await self._prune_once(
                now, storage_cfg.analytics_retention_days, self.storage.analytics
            )
            await self._prune_once(now, storage_cfg.dedup_ttl_days, self.storage.outbox)
            await self._purge_queue_acked(self.queue, self.settings.queue.keep_acked)
            self._prune_media(now, self.settings.media)

    @staticmethod
    async def _prune_once(now: datetime, days: int, store: _Prunable) -> None:
        if days <= 0:  # 0 = бессрочно (§17.3) → не чистим
            return
        await store.prune(now - timedelta(days=days))

    @staticmethod
    async def _purge_queue_acked(queue: EventQueuePort, keep_acked: int) -> None:
        """Ретеншн acked-строк очереди (§17.3, T016); ``0`` — бессрочно."""
        if keep_acked <= 0:  # 0 = бессрочно (§17.3) → не чистим
            return
        await queue.purge_acked(keep_latest=keep_acked)

    @staticmethod
    def _prune_media(now: datetime, media: MediaConfig) -> None:
        """
        Ретеншн скачанных файлов медиа (M7 A3, §17.3): удалить файлы в
        ``storage_dir`` старше ``retention_days`` по mtime.

        ``retention_days=0`` — бессрочно (как прочие окна §17.3).
        Отсутствующий каталог — нечего чистить (скачивание выключено или
        ещё ничего не скачано). ФС-операции синхронны, но дёшевы и идут в
        фоновой prune-задаче.
        """
        if media.retention_days <= 0:
            return
        directory = media.storage_path
        if not directory.is_dir():
            return
        cutoff = (now - timedelta(days=media.retention_days)).timestamp()
        for path in directory.iterdir():
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)

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


def build_storage(
    settings: AngarionSettings, *, plugins: LoadedPlugins | None = None
) -> StorageBundle:
    """
    Собрать комплект хранилищ по ``[storage].backend`` (резолв по реестру).

    Отдельная точка входа для CLI-команд, которым нужно только хранилище,
    а не весь конвейер: ``angarion login`` пишет сессию через
    ``SessionStorePort``. Sqlite-бэкенд применяет миграции при сборке (C-3).
    """
    registry = plugins if plugins is not None else load_plugins()
    return _resolve_backend(
        registry.storages, settings.storage.backend, 'хранилища'
    ).make(settings.storage)


def build_queue(
    settings: AngarionSettings, *, plugins: LoadedPlugins | None = None
) -> EventQueuePort:
    """
    Собрать очередь по ``[queue].backend`` (резолв по реестру).

    Отдельная точка входа для api-роли (§12.9, T024): web-процесс
    дотягивается до очереди (диагностика, requeue DLQ) без построения
    конвейера и подключения Telethon-клиентов.
    """
    registry = plugins if plugins is not None else load_plugins()
    return _resolve_backend(registry.queues, settings.queue.backend, 'очереди').make(
        settings.queue
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
        settings,
        accounts,
        AdapterDeps(ingest=ingest, storage=storage, settings=settings),
    )
    worker = PipelineWorker(
        queue=queue,
        outbox=storage.outbox,
        analytics=storage.analytics,
        dead_letters=storage.dead_letters,
        state=storage.state,
        runtime_config=storage.runtime_config,
        pipelines=bindings,
        max_retries=settings.worker.max_retries,
        backoff_base=settings.worker.backoff_base,
        backoff_cap=settings.worker.backoff_cap,
        log=get_logger('angarion.worker'),
    )
    dispatch_sink: MessageSinkPort = _DispatchSink(sinks)
    guarded_sources = _loop_guard_sources(settings, accounts)
    if guarded_sources:
        # цель совпала с источником — гасим петлю собственных доставок
        dispatch_sink = LoopGuardSink(
            inner=dispatch_sink, dedup=storage.dedup, sources=guarded_sources
        )
    delivery = DeliveryWorker(
        outbox=storage.outbox,
        sink=dispatch_sink,
        analytics=storage.analytics,
        max_retries=settings.worker.max_retries,
        backoff_base=settings.worker.backoff_base,
        backoff_cap=settings.worker.backoff_cap,
        poll_interval=settings.worker.poll_interval,
        log=get_logger('angarion.delivery'),
    )
    restart_event = asyncio.Event()
    outbox_consumer = OutboxConsumer(
        command_outbox=storage.command_outbox,
        sink=dispatch_sink,
        analytics=storage.analytics,
        catchup=_make_catchup(listeners),
        request_restart=restart_event.set,
        poll_seconds=settings.worker.outbox_poll_seconds,
        log=get_logger('angarion.outbox'),
    )
    return AngarionApp(
        settings=settings,
        ingest=ingest,
        worker=worker,
        delivery=delivery,
        outbox_consumer=outbox_consumer,
        listeners=listeners,
        queue=queue,
        storage=storage,
        sinks=sinks,
        catchup_unavailable=_catchup_degradation(settings, accounts),
        restart_event=restart_event,
    )
