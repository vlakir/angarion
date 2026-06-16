"""
CLI приложения (§11.8, FR «CLI и запуск»; M3, T005, фаза 5).

Три команды поверх composition root (``bootstrap``):

- ``angarion run --config app.toml [--role pipeline|api|combined]
  [--with-api]`` — боевой запуск по роли процесса (§12.9): ``pipeline``
  (ingest + worker + Telethon + consumer командного outbox в одном
  процессе, §12.1, по умолчанию), ``api`` (отдельный web-процесс —
  producer outbox + дашборд, без конвейера), ``combined`` / ``--with-api``
  (конвейер + uvicorn в одном процессе). Graceful shutdown по
  SIGINT/SIGTERM (стоп приёма → дообработка → курсоры → закрытие клиентов
  и БД, §14.7); в combined команда ``restart_pipeline`` гасит процесс
  (§3.2, супервизор поднимает).
- ``angarion migrate --config app.toml`` — применение миграций Alembic
  (sqlite-бэкенд).
- ``angarion login --config app.toml --account NAME`` — интерактивная
  авторизация аккаунта → зашифрованная сессия в ``app.db``; дальнейшие
  ``run`` неинтерактивны. Шов логина платформо-специфичен и принадлежит
  плагину (``make_login``, M7 B1): Telegram — номер/код/2FA, Matrix —
  homeserver/пароль.

structlog конфигурируется здесь (право приложения, plan 2.8): цепочка с
``mask_secrets`` (§17.7). Сетевые/интерактивные части (login,
подключение клиентов) изолированы за инъецируемыми seam'ами и в тестах
подменяются фейками.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from angarion.adapters.http.server import serve_api, serve_combined
from angarion.adapters.storage.engine import apply_migrations
from angarion.bootstrap import AngarionApp, build_app, build_storage, load_plugins
from angarion.config import load_settings
from angarion.domain.errors import ConfigError
from angarion.domain.plugin import LoginContext
from angarion.log import get_logger, mask_secrets

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from angarion.bootstrap import LoadedPlugins
    from angarion.config import AngarionSettings

_log = get_logger('angarion.cli')


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
        if name == 'run':
            # Роли процессов (§12.9, T024): pipeline (по умолчанию — приём
            # + worker + Telethon), api (отдельный web-процесс), combined
            # (конвейер + uvicorn в одном процессе). ``--with-api`` —
            # сокращение для combined.
            cmd.add_argument(
                '--role',
                choices=('pipeline', 'api', 'combined'),
                default='pipeline',
            )
            cmd.add_argument('--with-api', action='store_true')
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
    role: str = 'pipeline',
    app_factory: Callable[[AngarionSettings], AngarionApp] = build_app,
) -> None:
    """
    Боевой запуск с graceful shutdown по роли процесса (§12.9, §14.7).

    ``pipeline`` — приём + worker + Telethon (и consumer командного
    outbox); ``api`` — отдельный web-процесс (producer outbox + дашборд,
    без конвейера); ``combined`` — конвейер + uvicorn в одном процессе.
    """
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    _log.info('starting', role=role)
    if role == 'api':
        await serve_api(settings, stop)
    elif role == 'combined':
        await serve_combined(settings, stop)
    else:
        await _serve(app_factory(settings), stop)
    _log.info('stopped', role=role)


async def cmd_login(
    settings: AngarionSettings,
    account_name: str,
    *,
    plugins: LoadedPlugins | None = None,
) -> None:
    """
    Интерактивная авторизация аккаунта → зашифрованная сессия в БД.

    Платформо-агностично: шов логина принадлежит плагину (``make_login``,
    M7 B1), CLI лишь резолвит плагин по ``messenger`` аккаунта и отдаёт
    ему непрозрачный ``SessionStorePort`` + ключ шифрования.
    """
    registry = plugins if plugins is not None else load_plugins()
    section = settings.accounts.get(account_name)
    if section is None:
        known = ', '.join(sorted(settings.accounts)) or '<пусто>'
        msg = f'аккаунт {account_name!r} не найден в конфиге; известны: {known}'
        raise ConfigError(msg)
    plugin = registry.adapters.get(section.messenger)
    if plugin is None:
        known = ', '.join(sorted(registry.adapters)) or '<пусто>'
        msg = (
            f'аккаунт {account_name!r}: неизвестный messenger '
            f'{section.messenger!r}; зарегистрированы: {known}'
        )
        raise ConfigError(msg)
    if plugin.make_login is None:
        msg = f'платформа {plugin.name!r} не поддерживает `angarion login`'
        raise ConfigError(msg)
    cfg = plugin.account_config_model.model_validate(section.model_dump())
    storage = build_storage(settings, plugins=registry)
    try:
        await plugin.make_login(
            LoginContext(
                account_id=account_name,
                config=cfg,
                session=storage.session,
                session_key=settings.session_key,
            )
        )
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
            role = 'combined' if args.with_api else args.role
            asyncio.run(cmd_run(settings, role=role))
        elif args.command == 'login':
            asyncio.run(cmd_login(settings, args.account))
    except ConfigError as exc:
        _log.error('config_error', error=str(exc))
        return 1
    return 0
