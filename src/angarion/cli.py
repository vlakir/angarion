"""
CLI приложения (§11.8, FR «CLI и запуск»; M3, T005, фаза 5).

Три команды поверх composition root (``bootstrap``):

- ``angarion run --config app.toml`` — боевой запуск роли ``pipeline``
  (ingest + worker + Telethon-клиенты в одном процессе, §12.1) с graceful
  shutdown по SIGINT/SIGTERM (стоп приёма → дообработка → курсоры →
  закрытие клиентов и БД, §14.7).
- ``angarion migrate --config app.toml`` — применение миграций Alembic
  (sqlite-бэкенд).
- ``angarion login --config app.toml --account NAME`` — интерактивная
  авторизация аккаунта (номер → код → 2FA) → ``StringSession`` в ``app.db``
  (зашифрован, Q2); дальнейшие ``run`` неинтерактивны.

structlog конфигурируется здесь (право приложения, plan 2.8): цепочка с
``mask_secrets`` (§17.7). Сетевые/интерактивные части (login,
подключение клиентов) изолированы за инъецируемыми seam'ами и в тестах
подменяются фейками.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from angarion.adapters.storage.engine import apply_migrations
from angarion.adapters.telegram.plugin import TelegramAccountConfig
from angarion.adapters.telegram.realclient import login_and_export_session
from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.bootstrap import AngarionApp, build_app, build_storage
from angarion.config import load_settings
from angarion.domain.errors import ConfigError
from angarion.log import get_logger, mask_secrets

if TYPE_CHECKING:
    from collections.abc import Sequence

    from angarion.config import AngarionSettings

_log = get_logger('angarion.cli')

LoginFn = Callable[[int, str], Awaitable[str]]
"""Интерактивный логин: ``(api_id, api_hash) -> session_string``."""


def _configure_logging() -> None:
    """structlog-цепочка приложения с маскированием секретов (§17.7)."""
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            mask_secrets,
            structlog.processors.TimeStamper(fmt='iso', utc=True),
            structlog.dev.ConsoleRenderer(),
        ],
    )


def _build_parser() -> argparse.ArgumentParser:
    """Парсер с подкомандами run/migrate/login (§11.8)."""
    parser = argparse.ArgumentParser(prog='angarion')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ('run', 'migrate', 'login'):
        cmd = sub.add_parser(name)
        cmd.add_argument('--config', required=True, type=Path)
        if name == 'login':
            cmd.add_argument('--account', required=True)
    return parser


def cmd_migrate(settings: AngarionSettings) -> None:
    """Применить миграции Alembic к ``app.db`` (sqlite-бэкенд, FR «CLI»)."""
    data = settings.storage.model_dump()
    if data.get('backend') != 'sqlite':
        msg = 'angarion migrate поддерживает только [storage] backend = "sqlite"'
        raise ConfigError(msg)
    path = data.get('path')
    if not path:
        msg = '[storage].path обязателен для backend = "sqlite"'
        raise ConfigError(msg)
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    apply_migrations(db_path)
    _log.info('migrated', path=str(db_path))


def _install_signal_handlers(stop: asyncio.Event) -> None:
    """SIGINT/SIGTERM → взвести событие остановки (graceful shutdown, §14.7)."""
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)


async def _serve(app: AngarionApp, stop: asyncio.Event) -> None:
    """Запустить конвейер и держать до сигнала, затем graceful-стоп."""
    await app.start()
    try:
        await stop.wait()
    finally:
        await app.stop()


async def cmd_run(
    settings: AngarionSettings,
    *,
    app_factory: Callable[[AngarionSettings], AngarionApp] = build_app,
) -> None:
    """Боевой запуск конвейера с graceful shutdown (FR «CLI», §14.7)."""
    app = app_factory(settings)
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    _log.info('starting')
    await _serve(app, stop)
    _log.info('stopped')


async def cmd_login(
    settings: AngarionSettings,
    account_name: str,
    *,
    login: LoginFn = login_and_export_session,
) -> None:
    """Интерактивная авторизация аккаунта → зашифрованная сессия в БД (Q3)."""
    section = settings.accounts.get(account_name)
    if section is None:
        known = ', '.join(sorted(settings.accounts)) or '<пусто>'
        msg = f'аккаунт {account_name!r} не найден в конфиге; известны: {known}'
        raise ConfigError(msg)
    cfg = TelegramAccountConfig.model_validate(section.model_dump())
    storage = build_storage(settings)
    try:
        session_store = EncryptedSessionStore(storage.session, settings.session_key)
        session_string = await login(cfg.api_id, cfg.api_hash)
        await session_store.save(account_name, session_string)
        _log.info('logged_in', account=account_name)
    finally:
        dispose = getattr(storage, 'dispose', None)
        if callable(dispose):
            await dispose()


def main(argv: Sequence[str] | None = None) -> int:
    """Точка входа CLI (``[project.scripts] angarion``)."""
    args = _build_parser().parse_args(argv)
    _configure_logging()
    try:
        settings = load_settings(args.config)
        if args.command == 'migrate':
            cmd_migrate(settings)
        elif args.command == 'run':
            asyncio.run(cmd_run(settings))
        elif args.command == 'login':
            asyncio.run(cmd_login(settings, args.account))
    except ConfigError as exc:
        _log.error('config_error', error=str(exc))
        return 1
    return 0
