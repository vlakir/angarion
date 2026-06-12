"""
Alembic env (async-шаблон, FR-6 спеки T003).

Конфигурация приходит программно из
``angarion.adapters.storage.engine`` (без ``alembic.ini``):
``script_location`` — этот каталог, ``sqlalchemy.url`` —
``sqlite+aiosqlite:///<path>``.

Миграции выполняются **атомарно**: драйвер pysqlite/aiosqlite сам не
оборачивает DDL в транзакцию (autocommit до первого DML), поэтому
kill -9 посреди ``upgrade head`` оставлял бы частичную схему без
записи ``alembic_version`` — повторный старт падал бы на «table
already exists» (поймано kill-тестом FR-12). Стандартный рецепт
SQLAlchemy: ``isolation_level = None`` на connect + явный ``BEGIN
IMMEDIATE`` на событии begin — весь прогон до записи версии живёт в
одной транзакции, обрыв откатывается журналом SQLite.
"""

import asyncio

from alembic import context
from sqlalchemy import event, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from angarion.adapters.storage.orm import Base

config = context.config
target_metadata = Base.metadata


def _disable_driver_autobegin(dbapi_connection, _record) -> None:
    dbapi_connection.isolation_level = None


def _begin_immediate(connection: Connection) -> None:
    connection.exec_driver_sql('BEGIN IMMEDIATE')


def run_migrations_offline() -> None:
    """Offline-режим: DDL в виде SQL-скрипта, без подключения к БД."""
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )
    event.listen(connectable.sync_engine, 'connect', _disable_driver_autobegin)
    event.listen(connectable.sync_engine, 'begin', _begin_immediate)
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Online-режим; вызывающая сторона следит, чтобы loop'а не было (A-8)."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
