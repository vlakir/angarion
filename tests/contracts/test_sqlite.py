"""
Контрактные наборы портов хранения на SQLAlchemy/SQLite-реализациях
(FR-5 спеки T003; SC-1 — прогон через ``angarion.testing``).

Схема каждой тестовой БД разворачивается миграциями Alembic, не
``create_all()`` (FR-11, §13.2 — заодно проверяются сами миграции).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from angarion.testing import (
    AnalyticsContract,
    CursorStoreContract,
    DeadLetterContract,
    DedupStoreContract,
    MessageRegistryContract,
    OutboxContract,
    SessionStoreContract,
    StateStoreContract,
)

from angarion.adapters.storage.plugin import STORAGE_BACKEND, SqliteStorage
from angarion.config import StorageConfig
from angarion.domain import ports

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@pytest.fixture
async def sqlite_storage(tmp_path: Path) -> AsyncIterator[SqliteStorage]:
    config = StorageConfig.model_validate(
        {'backend': 'sqlite', 'path': str(tmp_path / 'app.db')}
    )
    storage = STORAGE_BACKEND.make(config)
    assert isinstance(storage, SqliteStorage)
    yield storage
    await storage.dispose()


class TestSqliteDedupStore(DedupStoreContract):
    @pytest.fixture
    def dedup(self, sqlite_storage: SqliteStorage) -> ports.DedupStorePort:
        return sqlite_storage.dedup


class TestSqliteOutbox(OutboxContract):
    @pytest.fixture
    def outbox(self, sqlite_storage: SqliteStorage) -> ports.OutboxPort:
        return sqlite_storage.outbox


class TestSqliteMessageRegistry(MessageRegistryContract):
    @pytest.fixture
    def registry(self, sqlite_storage: SqliteStorage) -> ports.MessageRegistryPort:
        return sqlite_storage.registry


class TestSqliteCursorStore(CursorStoreContract):
    @pytest.fixture
    def cursors(self, sqlite_storage: SqliteStorage) -> ports.CursorStorePort:
        return sqlite_storage.cursors


class TestSqliteSessionStore(SessionStoreContract):
    @pytest.fixture
    def session_store(self, sqlite_storage: SqliteStorage) -> ports.SessionStorePort:
        return sqlite_storage.session


class TestSqliteStateStore(StateStoreContract):
    @pytest.fixture
    def state(self, sqlite_storage: SqliteStorage) -> ports.StateStorePort:
        return sqlite_storage.state


class TestSqliteAnalytics(AnalyticsContract):
    @pytest.fixture
    def analytics(self, sqlite_storage: SqliteStorage) -> ports.AnalyticsPort:
        return sqlite_storage.analytics


class TestSqliteDeadLetters(DeadLetterContract):
    @pytest.fixture
    def dead_letters(self, sqlite_storage: SqliteStorage) -> ports.DeadLetterPort:
        return sqlite_storage.dead_letters


async def test_sqlite_adapters_satisfy_their_ports(
    sqlite_storage: SqliteStorage,
) -> None:
    conformance = [
        (sqlite_storage.dedup, ports.DedupStorePort),
        (sqlite_storage.outbox, ports.OutboxPort),
        (sqlite_storage.registry, ports.MessageRegistryPort),
        (sqlite_storage.cursors, ports.CursorStorePort),
        (sqlite_storage.session, ports.SessionStorePort),
        (sqlite_storage.state, ports.StateStorePort),
        (sqlite_storage.analytics, ports.AnalyticsPort),
        (sqlite_storage.dead_letters, ports.DeadLetterPort),
    ]
    for impl, port in conformance:
        assert isinstance(impl, port), port.__name__
