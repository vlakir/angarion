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
from angarion.domain.models import EventKind, Messenger


class AccountConfig(BaseModel):
    """
    Секция ``[accounts.*]`` — структурно лишь ``messenger`` (FR-2);
    остальные ключи — сырые: схема аккаунта принадлежит плагину
    платформы и валидируется его ``account_config_model`` в bootstrap.
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    messenger: Messenger


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


class QueueConfig(BaseModel):
    """
    Секция ``[queue]``: резолв ``backend`` + общие поля. ``depth_warn``
    — порог мониторинга глубины §17.5 (фоновая проверка — M3).
    """

    model_config = ConfigDict(frozen=True, extra='allow')

    backend: str = 'memory'
    depth_warn: int = Field(default=500, ge=1)


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


class CatchupConfig(BaseModel):
    """
    Секция ``[catchup]`` (§9.3, §11); в M1 нужна ветке деградации FR-13.

    ``interval`` (M3, фаза 5) — период фонового страхующего catch-up
    (§9.3, Q9/N1) в секундах; ``None``/``0`` — фоновый catch-up выключен,
    остаётся только прогон на старте процесса.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    enabled: bool = True
    max_messages_per_source: int = Field(default=2000, ge=1)
    max_age_days: int = Field(default=7, ge=1)
    interval: float | None = Field(default=None, ge=0)


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


class EndpointConfig(BaseModel):
    """Источник или цель пайплайна: ссылка на аккаунт + адрес чата (§11)."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    account: str
    chat_id: str
    thread_id: str | None = None


class PipelineConfig(BaseModel):
    """Секция ``[pipelines.*]`` (§11): подписка, маршрут, процессор."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    processor: str
    events: frozenset[EventKind] = Field(min_length=1)
    only_replies: bool = False
    sources: tuple[EndpointConfig, ...] = Field(min_length=1)
    targets: tuple[EndpointConfig, ...] = Field(min_length=1)
    processor_config: dict[str, Any] = Field(default_factory=dict)


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
    queue: QueueConfig = QueueConfig()
    worker: WorkerConfig = WorkerConfig()
    catchup: CatchupConfig = CatchupConfig()
    telegram: TelegramConfig = TelegramConfig()
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
