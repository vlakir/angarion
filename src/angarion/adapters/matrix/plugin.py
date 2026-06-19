"""
Контракт и сборка matrix-плагина (§12.10, §12.11 ТЗ; M7 B1, T010).

Объём B1 — каркас: матрица возможностей (полный профиль, как Telegram),
схема секции ``[accounts.*]`` и парольный ``angarion login``-шов
(homeserver/пароль → ``access_token`` + ``device_id`` в зашифрованную
сессию). Фабрики ``make_listener``/``make_sender`` — заглушки,
сигнализирующие fail-fast, что приём/отправка появятся в B2/B3:
зарегистрировать сейчас весь pipeline нечем, но плагин должен грузиться
из entry point, чтобы ``angarion login`` резолвил платформу.

Пароль — секрет (§17.7): не в TOML и не в модели аккаунта, а из env
``ANGARION_MATRIX_PASSWORD`` либо интерактивный ``getpass`` при логине.

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime; типы bootstrap/config в
сигнатурах фабрик — строками (TYPE_CHECKING).
"""

import os
from getpass import getpass
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from angarion.adapters.matrix.listener import MatrixListener
from angarion.adapters.matrix.realclient import MatrixClient, password_login
from angarion.adapters.matrix.sender import MatrixSender
from angarion.adapters.matrix.session import MatrixEncryptedSessionStore
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.plugin import AdapterPlugin, LoginContext
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from angarion.adapters.matrix.client import MatrixClientPort
    from angarion.bootstrap import AdapterDeps
    from angarion.config import EndpointConfig
    from angarion.domain.ports import MessageSinkPort

_PW_ENV: Final = 'ANGARION_MATRIX_PASSWORD'
"""Env-источник пароля для неинтерактивного логина (§17.7; иначе getpass)."""

MATRIX_CAPABILITIES: Final = AdapterCapabilities(
    user_account=True,
    edit_events=True,
    delete_events=True,
    history_fetch=True,
    threads=True,
    push_transport='client',
)
"""Матрица возможностей платформы matrix (§12.10): полный профиль —
правки ``m.replace``, удаления-redactions, история через sync, треды
``m.thread``; sync-loop как Telethon (``push_transport="client"``)."""


class MatrixAccountConfig(BaseModel):
    """
    Секция ``[accounts.*]`` платформы matrix (FR «Регистрация», §3.B).

    ``homeserver`` — URL сервера (например ``https://matrix.org``),
    ``user_id`` — полный MXID (``@user:server``). Пароль здесь **не**
    хранится (секрет, §17.7) — он подаётся при ``angarion login`` через
    env/интерактив. ``account_id`` (ключ сессии в БД) — имя самой секции,
    отдельным полем не дублируется.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    messenger: Literal['matrix']
    homeserver: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    device_name: str = 'angarion'


def _resolve_password() -> str:
    """Пароль логина: env ``ANGARION_MATRIX_PASSWORD`` либо ``getpass`` (§17.7)."""
    return os.environ.get(_PW_ENV) or getpass('Matrix password: ')


async def _login(ctx: LoginContext) -> None:
    """
    Парольный ``angarion login`` Matrix-аккаунта (B1).

    Получает у homeserver ``access_token`` + ``device_id`` и сохраняет
    зашифрованную ``MatrixSession`` через свой at-rest-декоратор —
    хранилище получает только ciphertext (как Telegram, ADR 2026-06-13).
    """
    cfg = MatrixAccountConfig.model_validate(ctx.config.model_dump())
    store = MatrixEncryptedSessionStore(ctx.session, ctx.session_key)
    session_string = await password_login(
        cfg.homeserver, cfg.user_id, _resolve_password(), cfg.device_name
    )
    await store.save(ctx.account_id, session_string)


_CLIENTS_KEY: Final = 'matrix.clients'
"""Ключ мемоизации общего пула Matrix-клиентов в ``deps.shared``."""


def _shared_clients(
    deps: 'AdapterDeps', accounts: 'Mapping[str, BaseModel]'
) -> dict[str, 'MatrixClientPort']:
    """
    Общий пул Matrix-клиентов для listener+sender (мемо в ``deps.shared``).

    Один nio ``AsyncClient`` на аккаунт обслуживает и приём (sync-loop), и
    отправку — listener и sender делят инстансы (как Telegram-``ClientRegistry``).
    Сессия читается из общего ``SessionStorePort`` через Fernet-декоратор
    (расшифровка at-rest); E2EE key-store — per-account подкаталог
    ``[matrix].store_dir`` (device-сторы не пересекаются). Восстановление
    отложено в ``restore()`` (идемпотентно, дёргают и listener.start, и
    sender перед первой отправкой — роль-сплит §12.9).
    """
    existing = deps.shared.get(_CLIENTS_KEY)
    if isinstance(existing, dict):
        return existing
    session_store = MatrixEncryptedSessionStore(
        deps.storage.session, deps.settings.session_key
    )
    store_dir = deps.settings.matrix.store_dir
    clients: dict[str, MatrixClientPort] = {
        account_id: MatrixClient(
            account_id=account_id,
            session_store=session_store,
            store_dir=str(Path(store_dir) / account_id),
        )
        for account_id in accounts
    }
    deps.shared[_CLIENTS_KEY] = clients
    return clients


def _make_listener(
    deps: 'AdapterDeps',
    accounts: 'Mapping[str, BaseModel]',
    sources: 'Sequence[EndpointConfig]',
) -> MatrixListener:
    """Фабрика Matrix-listener (§12.11): общий пул + проводка [catchup]/[media]."""
    catchup = deps.settings.catchup
    # T032 (A-1): recent_poll — per-pipeline, исполнение — per-source; источник
    # поллится, если входит хотя бы в один пайплайн с recent_poll=true.
    recent_poll_endpoints = frozenset(
        ep
        for cfg in deps.settings.pipelines.values()
        if cfg.recent_poll
        for ep in cfg.sources
    ) & frozenset(sources)
    return MatrixListener(
        ingest=deps.ingest,
        clients=_shared_clients(deps, accounts),
        sources=sources,
        cursors=deps.storage.cursors,
        analytics=deps.storage.analytics,
        log=get_logger('angarion.matrix.listener'),
        media_policy=deps.settings.media,
        catchup_enabled=catchup.enabled,
        catchup_max_messages=catchup.max_messages_per_source,
        catchup_max_age_days=catchup.max_age_days,
        recent_poll_endpoints=recent_poll_endpoints,
        recent_interval=catchup.recent_interval,
        recent_window_messages=catchup.recent_window_messages,
        recent_window_minutes=catchup.recent_window_minutes,
    )


def _make_sender(
    deps: 'AdapterDeps', accounts: 'Mapping[str, BaseModel]'
) -> 'MessageSinkPort':
    """Фабрика Matrix-sender (§12.11, B3): тот же пул клиентов, что у listener."""
    return MatrixSender(
        clients=_shared_clients(deps, accounts),
        log=get_logger('angarion.matrix.sender'),
    )


PLUGIN: Final = AdapterPlugin(
    name='matrix',
    capabilities=MATRIX_CAPABILITIES,
    account_config_model=MatrixAccountConfig,
    make_listener=_make_listener,
    make_sender=_make_sender,
    make_login=_login,
)
"""Значение entry point ``angarion.adapters:matrix`` (§12.11)."""
