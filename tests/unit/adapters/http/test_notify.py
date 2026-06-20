"""
Неблокирующее уведомление о заявке (§12.7/§12.9, T024, фаза 4):
постановка команды ``notify`` в outbox; выключенное — no-op; сбой
сборки — ``notify_failed`` в аналитику без исключения.
"""

from __future__ import annotations

from conftest import make_settings

from angarion.adapters.http.auth.notify import NOTIFY_FAILED, notify_registration
from angarion.adapters.http.deps import AngarionDeps
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCommandOutbox,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryMessageRegistry,
    MemoryRuntimeConfig,
    MemoryStateStore,
)
from angarion.config import AccountConfig, ApiConfig, NotifyConfig
from angarion.domain.models import CommandKind


def _deps(api: ApiConfig, command_outbox: MemoryCommandOutbox, analytics: MemoryAnalytics) -> AngarionDeps:
    settings = make_settings(
        api=api, accounts={'acc1': AccountConfig(transport='memory')}
    )
    return AngarionDeps(
        queue=MemoryQueue(),
        analytics=analytics,
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        runtime_config=MemoryRuntimeConfig(),
        command_outbox=command_outbox,
        dead_letters=MemoryDeadLetters(),
        settings=settings,
    )


async def test_notify_disabled_is_noop() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    deps = _deps(ApiConfig(auth='none'), outbox, analytics)
    await notify_registration(deps, login='alice')
    assert await outbox.take() == []
    assert await analytics.recent() == []


async def test_notify_enabled_enqueues_command() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    api = ApiConfig(
        auth='none', notify=NotifyConfig(account='acc1', address='-999')
    )
    deps = _deps(api, outbox, analytics)
    await notify_registration(deps, login='alice')
    commands = await outbox.take()
    assert len(commands) == 1
    assert commands[0].kind is CommandKind.NOTIFY
    record = commands[0].payload['record']
    assert record['target']['address'] == '-999'
    assert record['send_via']['account_id'] == 'acc1'
    assert 'alice' in record['text']


async def test_notify_unknown_account_records_failure_without_raising() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    api = ApiConfig(
        auth='none', notify=NotifyConfig(account='ghost', address='-999')
    )
    deps = _deps(api, outbox, analytics)
    await notify_registration(deps, login='alice')  # не падает
    assert await outbox.take() == []
    failures = await analytics.recent(kind=NOTIFY_FAILED)
    assert failures and failures[0].payload['login'] == 'alice'
