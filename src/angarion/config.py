"""
Конфигурация angarion (§11 ТЗ; объём M1 — C-4 спеки T002, FR-16).

Двухступенчатая валидация (plan 2.7): здесь — **структурная** стадия
(pydantic-settings: TOML + env-override ``ANGARION_*`` с
``__``-вложенностью, инвариант ретеншна §11/§17.3); **ссылочная и
плагинная** стадия (модели аккаунтов плагинов, ссылки на аккаунты,
реестр плагинов, матрица §12.10) — в ``angarion.bootstrap``.

Секции ``[api*]`` появятся в M5 аддитивно (C-4).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from angarion.domain.errors import ConfigError
from angarion.domain.models import INTERNAL_TRANSPORT, MediaRef, RecordKind, Transport


class AccountConfig(BaseModel):
    """
    Секция ``[accounts.*]`` — структурно лишь ``transport`` (FR-2, T041);
    остальные ключи — сырые: схема аккаунта принадлежит плагину
    транспорта и валидируется его ``account_config_model`` в bootstrap.
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    transport: Transport


class StorageConfig(BaseModel):
    """
    Секция ``[storage]``: резолв ``backend`` по реестру entry points +
    общие параметры ретеншна §17.3 (``0`` = бессрочно).
    Бэкенд-специфичные ключи (``path`` и т.п.) — сырые, их понимает
    фабрика бэкенда.
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    backend: str = 'memory'
    dedup_ttl_days: int = Field(default=30, ge=0)
    registry_window_days: int = Field(default=7, ge=0)
    analytics_retention_days: int = Field(default=90, ge=0)

    @model_validator(mode='after')
    def _dedup_covers_registry_window(self) -> Self:
        """
        Инвариант §11: ``dedup_ttl_days ≥ registry_window_days`` (иначе
        повторный catch-up ре-эмитировал бы события с уже вычищенными
        dedup-ключами); ``0`` трактуется как бесконечность.
        """
        ttl_unbounded = self.dedup_ttl_days == 0
        window_unbounded = self.registry_window_days == 0
        if window_unbounded and not ttl_unbounded:
            msg = (
                'dedup_ttl_days должен быть бессрочным (0) при '
                'registry_window_days = 0 (§11)'
            )
            raise ValueError(msg)
        if (
            not ttl_unbounded
            and not window_unbounded
            and self.dedup_ttl_days < self.registry_window_days
        ):
            msg = (
                f'dedup_ttl_days ({self.dedup_ttl_days}) должен быть не меньше '
                f'registry_window_days ({self.registry_window_days}) (§11)'
            )
            raise ValueError(msg)
        return self


class MediaConfig(BaseModel):
    """
    Секция ``[media]`` (M7 A3): глобальная политика скачивания/хранения/
    ретеншна вложений (§3.A спеки T010).

    Скачивание физически выполняет **принимающий аккаунт** при ingest (до
    fan-out в пайплайны), поэтому политика глобальная, account/source-level;
    per-pipeline переопределение пересылки медиа — отдельный follow-up
    (T033, решение Владимира 2026-06-16). По умолчанию **не скачиваем**
    (только метаданные, fast-path пересылки по платформенной ссылке §3.A):
    дёшево, безопасно, диск не растёт. ``download=True`` — явный opt-in для
    кросс-аккаунт/кросс-платформа доставки и доступа процессоров к
    ``local_path``.

    Конвенция ``0`` = «без ограничения»: ``max_size=0`` — лимита размера
    нет; ``retention_days=0`` — храним бессрочно (как §17.3 для прочих
    окон). ``allowed_kinds`` пуст — разрешены все виды.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    download: bool = False
    allowed_kinds: frozenset[str] = frozenset()
    max_size: int = Field(default=0, ge=0)
    storage_dir: str = 'data/media'
    retention_days: int = Field(default=0, ge=0)

    @property
    def storage_path(self) -> Path:
        """Каталог скачанных файлов (git-ignored, рядом с ``data/``)."""
        return Path(self.storage_dir)

    def should_download(self, media: MediaRef) -> bool:
        """
        Качать ли это вложение принимающим аккаунтом (M7 A3).

        ``False`` при выключенной политике, уже скачанном (``local_path``),
        отсутствии платформенной ссылки (``ref`` — без неё рефетчить нечего),
        виде вне whitelist'а или превышении ``max_size``. Размер неизвестен
        (``size=None``) — лимит не применяется (best-effort).
        """
        if not self.download or media.ref is None or media.local_path is not None:
            return False
        if self.allowed_kinds and media.kind not in self.allowed_kinds:
            return False
        if self.max_size and media.size is not None:
            return media.size <= self.max_size
        return True


class QueueConfig(BaseModel):
    """
    Секция ``[queue]``: резолв ``backend`` + общие поля. ``depth_warn``
    — порог мониторинга глубины §17.5 (фоновая проверка — M3).
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    backend: str = 'memory'
    depth_warn: int = Field(default=500, ge=1)
    keep_acked: int = Field(default=1000, ge=0)
    """
    Ретеншн подтверждённых записей очереди (§17.3, T016): сколько
    новейших acked-строк держать как буфер; остальные удаляются фоновой
    prune-задачей. ``0`` — хранить бессрочно (чистка выключена, как
    прочие окна §17.3). Чистка в любом случае работает только при
    ``[worker] prune_interval > 0``.
    """


class WorkerConfig(BaseModel):
    """
    Секция ``[worker]`` (plan 2.2, аддитивно к §11): retry/backoff
    обработки и доставки + период опроса outbox у ``DeliveryWorker`` +
    период фоновой prune-очистки ретеншна (§17.3; ``0`` — отключено).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    max_retries: int = Field(default=5, ge=0)
    backoff_base: float = Field(default=1.0, gt=0)
    backoff_cap: float = Field(default=60.0, gt=0)
    poll_interval: float = Field(default=1.0, gt=0)
    prune_interval: float = Field(default=0.0, ge=0)
    outbox_poll_seconds: float = Field(default=5.0, gt=0)
    """Период опроса командного outbox consumer'ом (§12.9, FR-5; default 5)."""
    shutdown_drain_seconds: float = Field(default=5.0, gt=0)
    """
    Граница graceful-дренажа воркеров при остановке (§3.2, T031): сколько
    ждать завершения in-flight операции, прежде чем оборвать её. Защищает
    ``app.stop()`` от подвисания на залипшем в throttle/``FloodWait`` sleep
    воркере; возможный дубль от обрыва покрывает at-least-once (§7.1).
    """
    command_lease_seconds: float = Field(default=300.0, gt=0)
    """
    Lease захвата командного outbox (§12.9, T027): ``taken``-команда без
    терминальной пометки дольше этого срока считается зависшей (краш
    consumer'а между ``take`` и пометкой) и возвращается reaper'ом в
    ``pending`` на переисполнение (идемпотентно, at-least-once). Reaper
    крутится в фоновой prune-задаче — активен лишь при ``prune_interval >
    0``. Дефолт 300 с — заведомо больше нормального исполнения команды
    (notify/catchup/restart), чтобы не реклеймить ещё живой захват.
    """


class ChainsConfig(BaseModel):
    """
    Секция ``[chains]`` (T037): параметры внутреннего провода цепочек.

    ``max_hops`` — рантайм-лимит прыжков записи по внутренним рёбрам (Q2):
    при превышении re-ingested запись уходит в DLQ с пометкой
    ``hop_limit_exceeded``. Универсальный backstop поверх стартовой
    DAG-валидации: ловит и циклы, замкнутые через **реальную** платформу
    (P1→internal→P2→группа X, P1 слушает X), которые стартовая проверка
    статически увидеть не может. Дефолт 10 — заведомо больше реальных
    цепочек, ``ge=1`` (нулевой лимит запретил бы любой прыжок).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    max_hops: int = Field(default=10, ge=1)


class CatchupConfig(BaseModel):
    """
    Секция ``[catchup]`` (§9.3, §11); в M1 нужна ветке деградации FR-13.

    ``interval`` (M3, фаза 5) — период фонового страхующего **глубокого**
    catch-up (§9.3, Q9/N1) в секундах; ``None``/``0`` — фоновый catch-up
    выключен, остаётся только прогон на старте процесса.

    ``recent_*`` (T032) — отдельный **лёгкий** поллинг узкого недавнего
    окна как дешёвый backstop правок/удалений: частая (``recent_interval``)
    сверка только последних ``recent_window_messages`` сообщений не старше
    ``recent_window_minutes`` минут (окно = min(N, M)). Глубокий catch-up
    остаётся страховкой на длинный хвост. Включается **per-pipeline**
    (``[pipelines.*].recent_poll``); сами ``recent_*`` — общие параметры
    окна/частоты. ``recent_interval = 0`` глушит лёгкий поллинг глобально.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    enabled: bool = True
    max_messages_per_source: int = Field(default=2000, ge=1)
    max_age_days: int = Field(default=7, ge=1)
    interval: float | None = Field(default=None, ge=0)
    recent_interval: float = Field(default=30.0, ge=0)
    """Период лёгкого поллинга недавнего окна, с (T032); ``0`` — выключен."""
    recent_window_messages: int = Field(default=30, ge=1)
    """Размер окна лёгкого поллинга по числу последних сообщений (T032)."""
    recent_window_minutes: int = Field(default=10, ge=1)
    """Верхняя граница возраста окна лёгкого поллинга, мин (T032)."""


class SenderConfig(BaseModel):
    """
    Секция ``[telegram.sender]`` (M3, фаза 4/5): пороги троттлинга и
    устойчивой отправки Telegram-sender'а (§12.1, FR «Sender»).

    Token-bucket per (account, chat): ≤ ``chat_per_second`` на чат,
    ≤ ``account_per_minute`` на аккаунт. FloodWait → повтор того же
    сообщения до ``flood_max_retries`` раз; transient (сеть/5xx) →
    ретраи tenacity до ``transient_max_attempts`` с backoff.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    chat_per_second: float = Field(default=1.0, gt=0)
    account_per_minute: float = Field(default=20.0, gt=0)
    flood_max_retries: int = Field(default=5, ge=0)
    transient_max_attempts: int = Field(default=3, ge=1)
    backoff_base: float = Field(default=1.0, gt=0)
    backoff_cap: float = Field(default=60.0, gt=0)


class TelegramConfig(BaseModel):
    """
    Секция ``[telegram]`` (M3, фаза 5): платформенные runtime-тюнеры
    Telegram-адаптера, сгруппированные под платформой (решение Владимира
    2026-06-13, ADR; пересмотр с per-plugin config в M5).

    ``live_buffer_soft_limit`` — мягкий лимит live-буфера (Q8/W3): при
    превышении — warning + аналитика ``live_buffer_high``, ничего не
    теряется. ``sender`` — секция ``[telegram.sender]``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    live_buffer_soft_limit: int = Field(default=1000, ge=1)
    sender: SenderConfig = SenderConfig()


class MatrixConfig(BaseModel):
    """
    Секция ``[matrix]`` (M7 B2, T010): runtime-тюнеры Matrix-адаптера.

    ``store_dir`` — каталог E2EE key-store ``matrix-nio`` (olm/megolm):
    отдельный sqlite на ФС, который nio ведёт сам. Единственное
    отступление от «вся сессия в app.db» (§6 спеки T010): git-ignored,
    как ``data/``; токен/``device_id`` всё так же в ``app.db``.

    Буфер live-событий не нужен: sync-колбэки nio ингестятся inline (в
    отличие от concurrent-апдейтов Telethon), глубокий catch-up по
    ``/messages`` — фаза B3.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    store_dir: str = 'data/matrix-e2e'


class NotifyConfig(BaseModel):
    """
    Секция ``[api.notify]`` (§12.7/§12.9, T024): цель уведомления о
    заявке на регистрацию.

    Когда заданы ``account`` (ссылка на ``[accounts.*]``) и ``address``,
    регистрация ставит команду ``notify`` в командный outbox; consumer
    (pipeline-процесс) отправляет запись через ``SinkPort``.
    Пустой ``account``/``address`` — уведомление отключено (заявки видны
    только в ``/ui/users``); сбой отправки неблокирующий (``notify_failed``
    в аналитику, на регистрацию не влияет, §12.9 FR-5).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    account: str = ''
    address: str = ''
    thread_id: str | None = None

    @property
    def enabled(self) -> bool:
        """Уведомление активно, только когда заданы и аккаунт, и адрес."""
        return bool(self.account and self.address)


class ApiConfig(BaseModel):
    """
    Секция ``[api]`` (M5, §12.5/§12.7): web-адаптер и аутентификация.

    ``host``/``port`` — bind встроенного раннера (uvicorn придёт с
    ролями процессов §12.9, T024); по умолчанию строго локальный
    ``127.0.0.1``. ``auth`` — режим: ``"users"`` (fastapi-users, default)
    или ``"none"`` (dev/локально — синтетический админ; при bind ≠
    ``127.0.0.1`` bootstrap пишет громкое предупреждение). ``secret`` —
    ключ подписи JWT, кладётся из env ``ANGARION_API__SECRET`` (секрет,
    не в TOML); обязателен при ``auth="users"`` — иначе fail-fast в
    bootstrap (§12.7, FR-0). ``jwt_lifetime`` — срок access-токена (сек).
    ``cookie_secure`` — флаг ``Secure`` cookie UI (включить за TLS-прокси).
    ``registration_enabled`` — саморегистрация; ``max_pending_registrations``
    — лимит заявок, ждущих одобрения (защита от замусоривания).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    host: str = '127.0.0.1'
    port: int = Field(default=8000, ge=1, le=65535)
    auth: Literal['users', 'none'] = 'users'
    secret: str = ''
    jwt_lifetime: int = Field(default=3600, ge=1)
    cookie_secure: bool = False
    registration_enabled: bool = True
    max_pending_registrations: int = Field(default=20, ge=1)
    notify: NotifyConfig = NotifyConfig()
    """Цель уведомления о заявке на регистрацию (``[api.notify]``, §12.9)."""


class EndpointConfig(BaseModel):
    """
    Источник или цель пайплайна: ссылка на аккаунт + адрес на транспорте
    (§11, T041; ``address`` — обобщение прежнего ``chat_id``).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    account: str
    address: str
    thread_id: str | None = None


class PipelineConfig(BaseModel):
    """Секция ``[pipelines.*]`` (§11): подписка, маршрут, процессор."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    processor: str
    events: frozenset[RecordKind] = Field(min_length=1)
    only_replies: bool = False
    sources: tuple[EndpointConfig, ...] = Field(min_length=1)
    targets: tuple[EndpointConfig, ...] = Field(min_length=1)
    processor_config: dict[str, Any] = Field(default_factory=dict)
    forward_media: bool = True
    """
    Пересылать ли вложения исходного события получателям этого пайплайна
    (T033, send-time concern). По умолчанию ``True`` — медиа транзитом
    (как с M7). При ``False`` worker отстригает ``media`` у исходящих перед
    outbox: один источник может питать пайплайн-зеркало с медиа и
    текст-только пайплайн одновременно. Скачивание медиа — отдельная
    глобальная ``[media]``-политика (account/source-level, до fan-out).
    """
    recent_poll: bool = False
    """
    Включить лёгкий поллинг недавнего окна (T032) для **источников этого
    пайплайна**: частая дешёвая сверка узкого окна как backstop правок/
    удалений в дополнение к редкому глубокому catch-up. По умолчанию
    ``False`` (доп-трафик — осознанный opt-in). Параметры окна/частоты —
    общие в ``[catchup]`` (``recent_*``). Исполнение — на уровне источника:
    источник поллится, если входит хотя бы в один пайплайн с
    ``recent_poll = true``.
    """


def _internal_channel(ep: EndpointConfig) -> tuple[str, str | None]:
    """Канал внутреннего ребра — ``(address, thread_id)`` (идентичность Endpoint)."""
    return (ep.address, ep.thread_id)


def internal_edges(
    pipelines: Mapping[str, PipelineConfig],
    accounts: Mapping[str, AccountConfig],
) -> dict[str, set[str]]:
    """
    Орграф рёбер pipeline→pipeline по внутренним каналам (T037).

    Ребро ``P→Q`` — если P пишет в внутренний канал (``target`` на транспорте
    ``internal``), а Q его слушает (``source`` на том же канале). Аккаунты с
    неизвестным/не-``internal`` транспортом игнорируются (внешние рёбра графом
    цепочек не считаются; отсутствующий аккаунт отвергнет bootstrap).

    Публичная (используется и стартовой DAG-валидацией ``find_internal_cycle``,
    и web-viz ``/ui/pipelines`` — единый источник топологии цепочек, T037 фаза 3).
    """
    producers: dict[tuple[str, str | None], set[str]] = {}
    consumers: dict[tuple[str, str | None], set[str]] = {}
    for name, cfg in pipelines.items():
        for ep in cfg.targets:
            account = accounts.get(ep.account)
            if account is not None and account.transport == INTERNAL_TRANSPORT:
                producers.setdefault(_internal_channel(ep), set()).add(name)
        for ep in cfg.sources:
            account = accounts.get(ep.account)
            if account is not None and account.transport == INTERNAL_TRANSPORT:
                consumers.setdefault(_internal_channel(ep), set()).add(name)
    edges: dict[str, set[str]] = {}
    for channel, channel_producers in producers.items():
        for producer in channel_producers:
            edges.setdefault(producer, set()).update(consumers.get(channel, set()))
    return edges


def _first_cycle(edges: Mapping[str, set[str]]) -> list[str] | None:
    """
    DFS-поиск первого цикла в орграфе; путь с замыканием либо ``None``.

    ``visiting``: нет ключа — узел не посещён (white); ``True`` — в текущем
    стеке (gray, ребро назад = цикл); ``False`` — полностью обойдён (black).
    Сортировка для детерминированного (воспроизводимого) пути в сообщении.
    """
    visiting: dict[str, bool] = {}
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        visiting[node] = True
        stack.append(node)
        for nxt in sorted(edges.get(node, set())):
            if visiting.get(nxt):
                return [*stack[stack.index(nxt) :], nxt]
            if nxt not in visiting:
                found = visit(nxt)
                if found is not None:
                    return found
        stack.pop()
        visiting[node] = False
        return None

    for node in sorted(edges):
        if node not in visiting:
            found = visit(node)
            if found is not None:
                return found
    return None


def find_internal_cycle(
    pipelines: Mapping[str, PipelineConfig],
    accounts: Mapping[str, AccountConfig],
) -> list[str] | None:
    """
    Цикл в графе внутренних рёбер либо ``None``, если граф ацикличен (T037, Q2).

    Возвращает имена пайплайнов цикла с замыканием (``['p1', 'p2', 'p1']``),
    включая self-loop ``['p', 'p']`` (пайплайн пишет и слушает один канал).
    fan-out/fan-in циклами не являются. Граф производен от конфига — не
    доменная сущность (спека §5, НЕ ДОЛЖНА вводить граф в домен).
    """
    return _first_cycle(internal_edges(pipelines, accounts))


class AngarionSettings(BaseSettings):
    """
    Корень конфигурации (§11, объём C-4). Источники: init-данные (TOML
    из ``load_settings``) + env ``ANGARION_*`` поверх них.
    """

    model_config = SettingsConfigDict(
        frozen=True,
        extra='forbid',
        env_prefix='ANGARION_',
        env_nested_delimiter='__',
    )

    accounts: dict[str, AccountConfig] = Field(default_factory=dict)
    storage: StorageConfig = StorageConfig()
    media: MediaConfig = MediaConfig()
    queue: QueueConfig = QueueConfig()
    worker: WorkerConfig = WorkerConfig()
    chains: ChainsConfig = ChainsConfig()
    catchup: CatchupConfig = CatchupConfig()
    telegram: TelegramConfig = TelegramConfig()
    matrix: MatrixConfig = MatrixConfig()
    api: ApiConfig = ApiConfig()
    pipelines: dict[str, PipelineConfig] = Field(default_factory=dict)
    session_key: str = ''
    """
    Ключ шифрования сессий Telegram at-rest (Q2 спеки T005, ADR
    2026-06-13); из env ``ANGARION_SESSION_KEY`` (секрет, не в TOML).
    Пустой при наличии сессий в БД — fail-fast в telegram-адаптере.
    """
    admin_login: str = ''
    admin_password: str = ''
    """
    Bootstrap первого администратора (§12.7, FR-0): из env
    ``ANGARION_ADMIN_LOGIN`` / ``ANGARION_ADMIN_PASSWORD`` (секреты, не в
    TOML). На пустой таблице пользователей при включённом api создаётся
    админ; при ``api.auth="users"`` и отсутствии этих env — fail-fast в
    bootstrap (реализация — T023, фаза 2).
    """

    @model_validator(mode='after')
    def _reject_internal_cycles(self) -> Self:
        """
        Цепочка внутренних рёбер обязана быть ацикличной (T037, Q2, SC).

        Fail-fast при старте (как прочие стартовые инварианты конфига §12.10):
        статически выразимый цикл внутренних рёбер — детерминированная понятная
        ошибка вместо бесконечной петли. Рантайм-backstop (``[chains] max_hops``)
        ловит то, что статически не видно (цикл через реальную платформу).
        """
        cycle = find_internal_cycle(self.pipelines, self.accounts)
        if cycle is not None:
            path = ' → '.join(cycle)
            msg = f'циклическая цепочка внутренних пайплайнов: {path}'
            raise ValueError(msg)
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        _settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Env поверх init: init-данные — это TOML из ``load_settings``."""
        return (env_settings, dotenv_settings, init_settings, file_secret_settings)


def load_settings(toml_file: str | Path) -> AngarionSettings:
    """
    Загрузить конфигурацию из TOML-файла с env-override (FR-16).

    Структурные ошибки (включая инвариант ретеншна) — ``ConfigError``
    (fail-fast §11); отсутствующий файл — тоже ошибка, а не пустые
    default'ы.
    """
    path = Path(toml_file)
    if not path.is_file():
        msg = f'файл конфигурации не найден: {path}'
        raise ConfigError(msg)
    data = TomlConfigSettingsSource(AngarionSettings, toml_file=path)()
    try:
        return AngarionSettings(**data)
    except ValidationError as exc:
        msg = f'невалидная конфигурация {path}: {exc}'
        raise ConfigError(msg) from exc
