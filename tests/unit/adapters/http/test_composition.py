"""
Web composition root (§12.5/§12.8, T024): проекция портов в
``AngarionDeps`` + сборка ``SettingsNotifier`` с подписчиком уровня лога.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

import pytest
import structlog
from conftest import make_settings

if TYPE_CHECKING:
    from pathlib import Path

    from angarion.adapters.storage.plugin import SqliteStorage

from angarion.adapters.http.composition import (
    build_settings_notifier,
    build_web_deps,
)
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCommandOutbox,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryMessageRegistry,
    MemoryRuntimeConfig,
    MemorySessionStore,
    MemoryStateStore,
)
from angarion.config import ApiConfig
from angarion.domain.models import DynamicSettings
from angarion.domain.plugin import StorageBundle


def _memory_storage() -> StorageBundle:
    from angarion.adapters.memory.storage import MemoryDedupStore, MemoryOutbox

    return StorageBundle(
        dedup=MemoryDedupStore(),
        outbox=MemoryOutbox(),
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        state=MemoryStateStore(),
        analytics=MemoryAnalytics(),
        dead_letters=MemoryDeadLetters(),
        session=MemorySessionStore(),
        runtime_config=MemoryRuntimeConfig(),
        command_outbox=MemoryCommandOutbox(),
    )


def test_build_web_deps_projects_storage_ports() -> None:
    storage = _memory_storage()
    queue = MemoryQueue()
    deps = build_web_deps(make_settings(), storage, queue)
    assert deps.queue is queue
    assert deps.analytics is storage.analytics
    assert deps.runtime_config is storage.runtime_config
    assert deps.command_outbox is storage.command_outbox
    assert deps.dead_letters is storage.dead_letters


def test_build_web_deps_auth_none_leaves_sessionmaker_none() -> None:
    deps = build_web_deps(
        make_settings(api=ApiConfig(auth='none')), _memory_storage(), MemoryQueue()
    )
    assert deps.auth_sessionmaker is None


@pytest.fixture
async def sqlite_storage(tmp_path: Path) -> AsyncIterator[SqliteStorage]:
    from angarion.adapters.storage.plugin import STORAGE_BACKEND, SqliteStorage
    from angarion.config import StorageConfig

    config = StorageConfig.model_validate(
        {'backend': 'sqlite', 'path': str(tmp_path / 'app.db')}
    )
    storage = STORAGE_BACKEND.make(config)
    assert isinstance(storage, SqliteStorage)
    yield storage
    await storage.dispose()


async def test_build_web_deps_users_derives_sessionmaker_from_storage(
    sqlite_storage: SqliteStorage,
) -> None:
    settings = make_settings(api=ApiConfig(auth='users', secret='x' * 32))
    deps = build_web_deps(settings, sqlite_storage, MemoryQueue())
    assert deps.auth_sessionmaker is sqlite_storage.sessions


def test_build_web_deps_explicit_sessionmaker_wins() -> None:
    sentinel = object()
    settings = make_settings(api=ApiConfig(auth='users', secret='x' * 32))
    deps = build_web_deps(
        settings, _memory_storage(), MemoryQueue(), auth_sessionmaker=sentinel
    )
    assert deps.auth_sessionmaker is sentinel


class TestSettingsNotifier:
    pytestmark = pytest.mark.asyncio

    @pytest.fixture(autouse=True)
    def _restore_structlog(self) -> Iterator[None]:
        yield
        structlog.reset_defaults()

    async def test_notifier_applies_log_level(self) -> None:
        notifier = build_settings_notifier()
        await notifier.notify(DynamicSettings(log_level='ERROR'))
        expected = structlog.make_filtering_bound_logger(logging.ERROR)
        assert structlog.get_config()['wrapper_class'] is expected
