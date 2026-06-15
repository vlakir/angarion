"""
Движок и миграции SQLite-бэкенда (FR-3, FR-6, FR-7 спеки T003).

Движок — ``sqlite+aiosqlite``; на каждом connect — ``PRAGMA
journal_mode=WAL`` и ``foreign_keys=ON`` (§12.3). Миграции живут в
``src/angarion/migrations/`` (C-5 — едут в wheel) и применяются
программно: ``alembic upgrade head`` без ``alembic.ini``.

env.py async-шаблона вызывает ``asyncio.run()``, поэтому из потока с
работающим event loop'ом прогон уходит в отдельный поток (A-8);
из «чистого» синхронного контекста выполняется на месте.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Final

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, event
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import DBAPIConnection
    from sqlalchemy.pool import ConnectionPoolEntry

MIGRATIONS_DIR: Final = Path(__file__).resolve().parents[2] / 'migrations'
"""Каталог Alembic внутри пакета ``angarion`` (C-5)."""


def make_engine(path: Path) -> AsyncEngine:
    """Async-движок поверх файла ``app.db`` с PRAGMA-хуком (FR-3)."""
    engine = create_async_engine(f'sqlite+aiosqlite:///{path}')

    @event.listens_for(engine.sync_engine, 'connect')
    def _set_pragmas(
        dbapi_connection: DBAPIConnection, _record: ConnectionPoolEntry
    ) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA foreign_keys=ON')
        # busy_timeout — ADR §3.1 (T024): в раздельном режиме (--role api +
        # --role pipeline) два процесса пишут в один app.db; WAL разводит
        # читателей с писателем, а busy_timeout даёт писателям подождать
        # снятия блокировки (до 5 с) вместо немедленного "database is
        # locked". Записи api-процесса редкие (админ-действия), поэтому
        # таймаута достаточно; явный per-write ретрай — в BACKLOG (T028).
        cursor.execute('PRAGMA busy_timeout=5000')
        cursor.close()

    return engine


def _alembic_config(path: Path) -> Config:
    """Программная конфигурация Alembic: без ``alembic.ini`` (FR-7)."""
    config = Config()
    config.set_main_option('script_location', str(MIGRATIONS_DIR))
    config.set_main_option('sqlalchemy.url', f'sqlite+aiosqlite:///{path}')
    return config


def _upgrade_to_head(path: Path) -> None:
    command.upgrade(_alembic_config(path), 'head')


def apply_migrations(path: Path) -> None:
    """
    Программный ``alembic upgrade head`` (FR-7); идемпотентен.

    env.py внутри делает ``asyncio.run()`` — при работающем event
    loop'е в текущем потоке прогон выполняется в отдельном (A-8).
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        _upgrade_to_head(path)
        return
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(_upgrade_to_head, path).result()


def ensure_schema_current(path: Path) -> None:
    """
    FR-7, ``auto_migrate = false``: ревизия БД обязана совпадать с
    head кода — иначе fail-fast с внятным сообщением.
    """
    head = ScriptDirectory.from_config(_alembic_config(path)).get_current_head()
    engine = create_engine(f'sqlite:///{path}')
    try:
        with engine.connect() as connection:
            current = MigrationContext.configure(connection).get_current_revision()
    finally:
        engine.dispose()
    if current != head:
        msg = (
            f'схема {path} не соответствует коду (ревизия {current!r}, '
            f'ожидается {head!r}): примени миграции — `alembic upgrade head` '
            f'или [storage] auto_migrate = true'
        )
        raise ConfigError(msg)
