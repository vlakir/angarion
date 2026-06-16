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
from typing import TYPE_CHECKING, Final, Literal, NoReturn

from pydantic import BaseModel, ConfigDict, Field

from angarion.adapters.matrix.realclient import password_login
from angarion.adapters.matrix.session import MatrixEncryptedSessionStore
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.errors import NotSupportedError
from angarion.domain.plugin import AdapterPlugin, LoginContext

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

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


def _make_listener(
    _deps: 'AdapterDeps',
    _accounts: 'Mapping[str, BaseModel]',
    _sources: 'Sequence[EndpointConfig]',
) -> NoReturn:
    """Заглушка B1: matrix-listener реализуется в B2 (fail-fast, §12.11)."""
    msg = (
        'Matrix listener реализуется в фазе B2 (T010); в B1 доступен '
        'только `angarion login`'
    )
    raise NotSupportedError(msg)


def _make_sender(
    _deps: 'AdapterDeps', _accounts: 'Mapping[str, BaseModel]'
) -> 'MessageSinkPort':
    """Заглушка B1: matrix-sender реализуется в B3 (fail-fast, §12.11)."""
    msg = (
        'Matrix sender реализуется в фазе B3 (T010); в B1 доступен '
        'только `angarion login`'
    )
    raise NotSupportedError(msg)


PLUGIN: Final = AdapterPlugin(
    name='matrix',
    capabilities=MATRIX_CAPABILITIES,
    account_config_model=MatrixAccountConfig,
    make_listener=_make_listener,
    make_sender=_make_sender,
    make_login=_login,
)
"""Значение entry point ``angarion.adapters:matrix`` (§12.11)."""
