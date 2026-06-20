"""
Частности SQLAlchemy/SQLite-бэкенда сверх контрактов (FR-3, FR-6,
FR-7 спеки T003): персистентность через переоткрытие, миграции и
``auto_migrate``, PRAGMA WAL / foreign_keys, хранение времени строкой
ISO 8601 в UTC (A-4), фабрика бэкенда.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
import sqlalchemy as sa
from angarion.testing import (
    SOURCE_KEY,
    make_analytics_event,
    make_cursor,
    make_dead_letter,
    make_outbound,
    make_registry_record,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker

from angarion.adapters.storage.engine import apply_migrations, make_engine
from angarion.adapters.storage.orm import Base, UserRow, UTCDateTime
from angarion.adapters.storage.plugin import STORAGE_BACKEND, SqliteStorage
from angarion.adapters.storage.stores import SqliteStateStore, _is_locked
from angarion.config import StorageConfig
from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from pathlib import Path


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / 'app.db'


@pytest.fixture
async def open_storage(
    db_path: Path,
) -> AsyncIterator[Callable[..., SqliteStorage]]:
    """
    Фабрика «рестарта процесса»: каждый вызов — новый бэкенд поверх
    того же файла ``app.db``; по завершении теста движки закрываются.
    """
    opened: list[SqliteStorage] = []

    def factory(**overrides: object) -> SqliteStorage:
        config = StorageConfig.model_validate(
            {'backend': 'sqlite', 'path': str(db_path), **overrides}
        )
        storage = STORAGE_BACKEND.make(config)
        assert isinstance(storage, SqliteStorage)
        opened.append(storage)
        return storage

    yield factory
    for storage in opened:
        await storage.dispose()


async def test_state_cursor_and_dedup_survive_reopen(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    first = open_storage()
    await first.state.set('digest', 'k', '{"n": 1}')
    await first.cursors.save(make_cursor())
    assert await first.dedup.mark_inbound('k1') is True

    reopened = open_storage()
    assert await reopened.state.get('digest', 'k') == '{"n": 1}'
    assert await reopened.cursors.load(SOURCE_KEY) == make_cursor()
    assert await reopened.dedup.mark_inbound('k1') is False


async def test_outbox_registry_dlq_analytics_survive_reopen(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    first = open_storage()
    msg = make_outbound()
    await first.outbox.put(msg)
    await first.registry.upsert(make_registry_record())
    letter = make_dead_letter()
    await first.dead_letters.put(letter)
    event = make_analytics_event()
    await first.analytics.record(event)

    reopened = open_storage()
    record = await reopened.outbox.get(msg.idempotency_key)
    assert record is not None
    assert record.record == msg
    assert await reopened.registry.get(SOURCE_KEY, '42') == make_registry_record()
    assert await reopened.dead_letters.list() == [letter]
    assert await reopened.analytics.recent() == [event]


def test_migrations_create_full_m2_schema(db_path: Path) -> None:
    """FR-6: первая ревизия — полная схема M2, колонки совпадают с ORM."""
    apply_migrations(db_path)
    engine = sa.create_engine(f'sqlite:///{db_path}')
    inspector = sa.inspect(engine)
    tables = set(inspector.get_table_names())
    expected = {table.name for table in Base.metadata.sorted_tables}
    assert expected <= tables
    assert 'alembic_version' in tables
    for table in Base.metadata.sorted_tables:
        db_columns = {col['name'] for col in inspector.get_columns(table.name)}
        assert db_columns == {col.name for col in table.columns}, table.name
    engine.dispose()


def test_users_table_roundtrip(db_path: Path) -> None:
    """M5/B (T023): UserRow переживает миграцию 0003 — UUID/bool/время."""
    apply_migrations(db_path)
    engine = sa.create_engine(f'sqlite:///{db_path}')
    uid = uuid4()
    registered = datetime.now(UTC)
    with closing(engine.connect()) as conn:
        conn.execute(
            sa.insert(UserRow).values(
                id=uid,
                login='root',
                hashed_password='$argon2$hash',
                role='admin',
                is_active=True,
                registered_at=registered,
                approved_at=None,
                approved_by=None,
            )
        )
        conn.commit()
        row = conn.execute(
            sa.select(UserRow).where(UserRow.login == 'root')
        ).one()
    assert row.id == uid
    assert row.role == 'admin'
    assert row.is_active is True
    assert row.registered_at == registered
    assert row.approved_at is None
    engine.dispose()


def test_userrow_fastapi_users_bridge() -> None:
    """Алиасы протокола fastapi-users (email/superuser/verified) — оба конца."""
    user = UserRow(
        login='x',
        hashed_password='h',
        role='viewer',
        is_active=False,
        registered_at=datetime.now(UTC),
    )
    assert user.email == 'x'
    assert user.is_superuser is False
    assert user.is_verified is False
    user.email = 'y'
    assert user.login == 'y'
    user.is_superuser = True
    assert user.role == 'admin'
    user.is_superuser = False
    assert user.role == 'viewer'
    user.is_verified = True
    assert user.is_active is True


def test_users_login_is_unique(db_path: Path) -> None:
    """login уникален — повторная регистрация занятого логина невозможна."""
    apply_migrations(db_path)
    engine = sa.create_engine(f'sqlite:///{db_path}')
    stmt = sa.insert(UserRow).values(
        login='dup',
        hashed_password='h',
        role='viewer',
        is_active=False,
        registered_at=datetime.now(UTC),
    )
    with closing(engine.connect()) as conn:
        conn.execute(stmt.values(id=uuid4()))
        conn.commit()
        with pytest.raises(sa.exc.IntegrityError):
            conn.execute(stmt.values(id=uuid4()))
            conn.commit()
    engine.dispose()


_KILL_MID_MIGRATION_CHILD = r"""
import os, signal, sqlite3, sys
from pathlib import Path

# Прибить процесс на первом CREATE INDEX — середина прогона DDL.
orig_connect = sqlite3.connect


def connect(*args, **kwargs):
    conn = orig_connect(*args, **kwargs)

    def authorizer(action, *_rest):
        if action == sqlite3.SQLITE_CREATE_INDEX:
            os.kill(os.getpid(), signal.SIGKILL)
        return sqlite3.SQLITE_OK

    try:
        conn.set_authorizer(authorizer)
    except Exception:  # noqa: BLE001, S110
        pass
    return conn


sqlite3.connect = connect

from angarion.adapters.storage.engine import apply_migrations  # noqa: E402

apply_migrations(Path(sys.argv[1]))
"""


@pytest.mark.skipif(os.name != 'posix', reason='SIGKILL — POSIX-only (A-10)')
def test_kill_mid_migration_rolls_back_atomically(db_path: Path) -> None:
    """Регрессия (CI 3.14, kill-тест): DDL миграции — одна транзакция.

    Без транзакционного DDL kill -9 посреди upgrade head оставлял
    таблицы без записи alembic_version, и рестарт падал на
    «table already exists».
    """
    proc = subprocess.run(  # noqa: S603
        [sys.executable, '-c', _KILL_MID_MIGRATION_CHILD, str(db_path)],
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert proc.returncode == -signal.SIGKILL, proc.stderr.decode()[-500:]
    with closing(sqlite3.connect(db_path)) as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    assert tables == [], f'частичная схема пережила kill: {tables}'
    apply_migrations(db_path)  # повторный прогон по чистой БД проходит


def test_apply_migrations_is_idempotent(db_path: Path) -> None:
    apply_migrations(db_path)
    apply_migrations(db_path)


def test_auto_migrate_false_without_schema_fails(db_path: Path) -> None:
    """FR-7: auto_migrate=false при расхождении схемы — fail-fast."""
    config = StorageConfig.model_validate(
        {'backend': 'sqlite', 'path': str(db_path), 'auto_migrate': False}
    )
    with pytest.raises(ConfigError, match='миграци'):
        STORAGE_BACKEND.make(config)


async def test_auto_migrate_false_on_current_schema_builds(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    open_storage()  # применяет миграции (auto_migrate=true по умолчанию)
    storage = open_storage(auto_migrate=False)
    assert await storage.state.get('digest', 'k') is None


def test_factory_without_path_fails() -> None:
    config = StorageConfig.model_validate({'backend': 'sqlite'})
    with pytest.raises(ConfigError, match='path'):
        STORAGE_BACKEND.make(config)


async def test_wal_and_foreign_keys_pragmas(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    """FR-3: PRAGMA journal_mode=WAL и foreign_keys=ON на каждом connect."""
    storage = open_storage()
    async with storage.engine.connect() as conn:
        journal = (await conn.execute(sa.text('PRAGMA journal_mode'))).scalar()
        foreign_keys = (await conn.execute(sa.text('PRAGMA foreign_keys'))).scalar()
    assert journal == 'wal'
    assert foreign_keys == 1


async def test_datetimes_stored_as_iso_utc_text(
    open_storage: Callable[..., SqliteStorage], db_path: Path
) -> None:
    """A-4 / §17.4: в БД — ISO 8601 с явным смещением, нормализовано в UTC."""
    storage = open_storage()
    moment = datetime(2026, 6, 12, 15, 30, tzinfo=timezone(timedelta(hours=3)))
    await storage.cursors.save(make_cursor(updated_at=moment))

    with closing(sqlite3.connect(db_path)) as conn:
        (raw,) = conn.execute('SELECT updated_at FROM source_cursors').fetchone()
    assert raw == '2026-06-12T12:30:00.000000+00:00'

    loaded = await storage.cursors.load(SOURCE_KEY)
    assert loaded is not None
    assert loaded.updated_at == moment
    assert loaded.updated_at.tzinfo == UTC


def test_utc_datetime_rejects_naive_datetime() -> None:
    decorator = UTCDateTime()
    with pytest.raises(ValueError, match='UTC'):
        decorator.process_bind_param(datetime(2026, 6, 12, 12, 0), object())  # noqa: DTZ001


async def test_registry_record_full_roundtrip(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    storage = open_storage()
    rec = make_registry_record(sender_id='u1', sender_name='Ann')
    await storage.registry.upsert(rec)
    assert await storage.registry.get(SOURCE_KEY, '42') == rec


async def test_analytics_event_full_roundtrip(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    storage = open_storage()
    event = make_analytics_event(
        payload={'n': 1}, record_uid=uuid4(), pipeline='digest'
    )
    await storage.analytics.record(event)
    assert await storage.analytics.recent() == [event]


async def test_outbox_record_uid_roundtrip(
    open_storage: Callable[..., SqliteStorage],
) -> None:
    storage = open_storage()
    msg = make_outbound()
    record_uid = uuid4()
    await storage.outbox.put(msg, pipeline='digest', record_uid=record_uid)
    record = await storage.outbox.get(msg.idempotency_key)
    assert record is not None
    assert record.record_uid == record_uid
    assert record.pipeline == 'digest'


def test_is_locked_matches_only_busy_error() -> None:
    """T028: ретраим ровно ``database is locked``, не любой OperationalError."""
    locked = OperationalError('UPDATE x', None, Exception('database is locked'))
    other = OperationalError('UPDATE x', None, Exception('no such table: x'))
    assert _is_locked(locked) is True
    assert _is_locked(other) is False
    assert _is_locked(ValueError('database is locked')) is False


async def test_writer_retries_through_database_locked(db_path: Path) -> None:
    """T028 / ADR §3.1: при контеншне двух писателей запись не падает.

    Второй процесс держит write-lock дольше busy_timeout — голый писатель
    ловит ``database is locked`` (контроль), а store-писатель пересиживает
    блокировку per-write ретраем и коммитит, как только lock освобождается.
    """
    apply_migrations(db_path)
    # busy_timeout опущен до 50 мс, чтобы блокировка всплыла быстро (см.
    # make_engine): иначе при дефолтных 5 с тест ждал бы исчерпания таймаута.
    engine = make_engine(db_path, busy_timeout_ms=50)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = SqliteStateStore(sessions)

    holder = sqlite3.connect(db_path, isolation_level=None)
    try:
        holder.execute('PRAGMA busy_timeout=0')
        holder.execute('BEGIN IMMEDIATE')  # держим единый write-lock БД

        # контроль: писатель без ретрая под тем же lock'ом падает сразу.
        with closing(sqlite3.connect(db_path, isolation_level=None)) as rival:
            rival.execute('PRAGMA busy_timeout=50')
            with pytest.raises(sqlite3.OperationalError, match='database is locked'):
                rival.execute('BEGIN IMMEDIATE')

        async def _release() -> None:
            await asyncio.sleep(0.2)
            holder.execute('COMMIT')  # отпускаем lock — ретрай дожмёт запись

        releaser = asyncio.create_task(_release())
        await store.set('ns', 'k', 'v')  # переживает блокировку через ретрай
        await releaser

        assert await store.get('ns', 'k') == 'v'  # запись реально закоммичена
    finally:
        holder.close()
        await engine.dispose()
