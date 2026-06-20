"""
Админ-операции и динамика (§12.8, FR-4; T024, фаза 4): сервисные функции
(пауза/возобновление, save/reset динамики, requeue DLQ attempt=0,
restart/catchup через outbox) + ASGI-ручки ``/api/v1/admin`` и страницы
``/ui/settings`` / ``/ui/dlq`` на InMemory-портах (auth="none" → admin).

Аудит ``admin_op`` проверяется после каждой операции (spec §4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from angarion.adapters.http import AngarionDeps
from angarion.adapters.http.ops import (
    ADMIN_OP,
    record_admin_op,
    request_catchup,
    request_inject,
    request_restart,
    requeue_dead_letter,
    reset_setting,
    save_settings,
    set_pause,
)
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCommandOutbox,
    MemoryDeadLetters,
    MemoryRuntimeConfig,
)
from angarion.application.settings import SettingsNotifier
from angarion.domain.models import CommandKind, DynamicSettings
from angarion.testing.factories import make_dead_letter, make_record

if TYPE_CHECKING:
    from httpx import AsyncClient


# --- сервисные функции (прямой вызов на InMemory-портах) ---


def _recording_notifier(seen: list[DynamicSettings]) -> SettingsNotifier:
    """Notifier с async-подписчиком, складывающим полученные настройки."""

    async def record(settings: DynamicSettings) -> None:
        seen.append(settings)

    notifier = SettingsNotifier()
    notifier.subscribe(record)
    return notifier


async def test_set_pause_adds_and_audits() -> None:
    rc, analytics = MemoryRuntimeConfig(), MemoryAnalytics()
    seen: list[DynamicSettings] = []
    notifier = _recording_notifier(seen)
    settings = await set_pause(
        rc, analytics, notifier, pipeline='digest', paused=True, by='admin'
    )
    assert settings.paused_pipelines == frozenset({'digest'})
    assert seen and seen[-1].paused_pipelines == frozenset({'digest'})
    ops = await analytics.recent(kind=ADMIN_OP)
    assert ops[0].payload['operation'] == 'pause'
    assert ops[0].payload['by'] == 'admin'
    assert ops[0].pipeline == 'digest'


async def test_resume_removes() -> None:
    rc, analytics = MemoryRuntimeConfig(), MemoryAnalytics()
    await set_pause(rc, analytics, None, pipeline='digest', paused=True, by='a')
    settings = await set_pause(
        rc, analytics, None, pipeline='digest', paused=False, by='a'
    )
    assert settings.paused_pipelines == frozenset()


async def test_save_settings_persists_notifies_audits() -> None:
    rc, analytics = MemoryRuntimeConfig(), MemoryAnalytics()
    seen: list[DynamicSettings] = []
    notifier = _recording_notifier(seen)
    settings = await save_settings(
        rc, analytics, notifier, patch=DynamicSettings(log_level='WARNING'), by='a'
    )
    assert settings.log_level == 'WARNING'
    assert (await rc.load()).log_level == 'WARNING'
    assert seen[-1].log_level == 'WARNING'
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload['operation'] == 'settings_update'
    assert op.payload['changed'] == ['log_level']


async def test_reset_setting_returns_to_file() -> None:
    rc, analytics = MemoryRuntimeConfig(), MemoryAnalytics()
    await rc.save(DynamicSettings(log_level='DEBUG'))
    settings = await reset_setting(rc, analytics, None, key='log_level', by='a')
    assert settings.log_level is None
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload['operation'] == 'settings_reset'


async def test_requeue_resets_attempt_and_audits() -> None:
    dlq, queue, analytics = MemoryDeadLetters(), MemoryQueue(), MemoryAnalytics()
    letter = make_dead_letter(error='boom')
    await dlq.put(letter)
    returned = await requeue_dead_letter(
        dlq, queue, analytics, uid=letter.uid, by='a'
    )
    assert returned.uid == letter.uid
    item = await queue.get()
    assert item.envelope.attempt == 0
    assert item.envelope.not_before is None
    assert await dlq.take(letter.uid) is None  # уже изъята
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload['operation'] == 'requeue'
    assert 'requeued_at' in op.payload


async def test_requeue_unknown_is_404() -> None:
    from fastapi import HTTPException as _HTTPException

    dlq, queue, analytics = MemoryDeadLetters(), MemoryQueue(), MemoryAnalytics()
    with pytest.raises(_HTTPException) as exc:
        await requeue_dead_letter(dlq, queue, analytics, uid=uuid4(), by='a')
    assert exc.value.status_code == 404


async def test_request_restart_puts_command() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await request_restart(outbox, analytics, by='a')
    assert command.kind is CommandKind.RESTART_PIPELINE
    assert await outbox.get(command.uid) is not None
    assert (await analytics.recent(kind=ADMIN_OP))[0].payload['operation'] == 'restart'


async def test_request_catchup_puts_command_with_source() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await request_catchup(outbox, analytics, source_key='memory:acc1:-100', by='a')
    assert command.kind is CommandKind.CATCHUP
    assert command.payload == {'source_key': 'memory:acc1:-100'}
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload['source_key'] == 'memory:acc1:-100'


async def test_request_inject_puts_command_with_serialized_record() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    record = make_record(origin='manual')
    command = await request_inject(outbox, analytics, record=record, by='a')
    assert command.kind is CommandKind.INJECT
    # payload несёт сериализованный Record, восстановимый без потерь
    assert command.payload['record']['uid'] == str(record.uid)
    assert await outbox.get(command.uid) is not None
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload['operation'] == 'inject'
    assert op.payload['record_uid'] == str(record.uid)


async def test_record_admin_op_shape() -> None:
    analytics = MemoryAnalytics()
    await record_admin_op(analytics, operation='x', by='b', details={'k': 'v'})
    op = (await analytics.recent(kind=ADMIN_OP))[0]
    assert op.payload == {'operation': 'x', 'by': 'b', 'k': 'v'}


# --- ASGI-ручки /api/v1/admin (auth="none" → синтетический admin) ---


async def test_api_put_settings_applies(client: AsyncClient) -> None:
    resp = await client.put('/api/v1/admin/settings', json={'log_level': 'ERROR'})
    assert resp.status_code == 200
    assert resp.json()['log_level'] == 'ERROR'
    assert (await client.get('/api/v1/admin/settings')).json()['log_level'] == 'ERROR'


async def test_api_delete_setting_resets(client: AsyncClient) -> None:
    await client.put('/api/v1/admin/settings', json={'log_level': 'ERROR'})
    resp = await client.delete('/api/v1/admin/settings/log_level')
    assert resp.status_code == 200
    assert 'log_level' not in resp.json()


async def test_api_pause_resume(
    client: AsyncClient, runtime_config: MemoryRuntimeConfig
) -> None:
    assert (await client.post('/api/v1/admin/pipelines/digest/pause')).status_code == 204
    assert (await runtime_config.load()).paused_pipelines == frozenset({'digest'})
    assert (await client.post('/api/v1/admin/pipelines/digest/resume')).status_code == 204
    assert (await runtime_config.load()).paused_pipelines == frozenset()


async def test_api_requeue(
    client: AsyncClient, dead_letters: MemoryDeadLetters
) -> None:
    letter = make_dead_letter(error='boom')
    await dead_letters.put(letter)
    resp = await client.post(f'/api/v1/admin/dlq/{letter.uid}/requeue')
    assert resp.status_code == 204
    assert (await client.post(f'/api/v1/admin/dlq/{uuid4()}/requeue')).status_code == 404


async def test_api_restart_and_catchup(
    client: AsyncClient, command_outbox: MemoryCommandOutbox
) -> None:
    assert (await client.post('/api/v1/admin/restart')).status_code == 202
    resp = await client.post(
        '/api/v1/admin/catchup', json={'source_key': 'memory:acc1:-100'}
    )
    assert resp.status_code == 202
    taken = await command_outbox.take(limit=10)
    kinds = {c.kind for c in taken}
    assert kinds == {CommandKind.RESTART_PIPELINE, CommandKind.CATCHUP}


# --- SSR /ui/settings, /ui/dlq ---


async def test_ui_settings_renders(client: AsyncClient) -> None:
    resp = await client.get('/ui/settings')
    assert resp.status_code == 200
    assert 'Settings and operations' in resp.text
    assert 'api.secret' in resp.text


async def test_ui_settings_save_redirects(
    client: AsyncClient, runtime_config: MemoryRuntimeConfig
) -> None:
    resp = await client.post(
        '/ui/settings', data={'log_level': 'WARNING'}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert (await runtime_config.load()).log_level == 'WARNING'


async def test_ui_pipeline_pause_resume(
    client: AsyncClient, runtime_config: MemoryRuntimeConfig
) -> None:
    resp = await client.post(
        '/ui/pipelines/digest/pause', follow_redirects=False
    )
    assert resp.status_code == 303
    assert (await runtime_config.load()).paused_pipelines == frozenset({'digest'})
    resp = await client.post(
        '/ui/pipelines/digest/resume', follow_redirects=False
    )
    assert resp.status_code == 303
    assert (await runtime_config.load()).paused_pipelines == frozenset()


async def test_ui_settings_reset(
    client: AsyncClient, runtime_config: MemoryRuntimeConfig
) -> None:
    await runtime_config.save(DynamicSettings(log_level='DEBUG'))
    resp = await client.post('/ui/settings/log_level/reset', follow_redirects=False)
    assert resp.status_code == 303
    assert (await runtime_config.load()).log_level is None


async def test_ui_restart_and_catchup(
    client: AsyncClient, command_outbox: MemoryCommandOutbox
) -> None:
    assert (
        await client.post('/ui/restart', follow_redirects=False)
    ).status_code == 303
    resp = await client.post(
        '/ui/catchup',
        data={'source_key': 'memory:acc1:-100'},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    kinds = {c.kind for c in await command_outbox.take(limit=10)}
    assert kinds == {CommandKind.RESTART_PIPELINE, CommandKind.CATCHUP}


async def test_ui_trigger_renders(client: AsyncClient) -> None:
    resp = await client.get('/ui/trigger')
    assert resp.status_code == 200
    assert 'Manual trigger' in resp.text
    assert 'digest' in resp.text  # пайплайн из конфига в выпадайке


async def test_ui_trigger_event_split(
    client: AsyncClient, command_outbox: MemoryCommandOutbox
) -> None:
    """Пустой pipeline → event-путь; api-роль (ingest is None) кладёт INJECT."""
    resp = await client.post(
        '/ui/trigger',
        data={'transport': 'memory', 'address': '-100', 'text': 'hi'},
    )
    assert resp.status_code == 200
    assert 'queued' in resp.text
    commands = await command_outbox.take(limit=10)
    assert [c.kind for c in commands] == [CommandKind.INJECT]
    assert commands[0].payload['record']['origin'] == 'manual'


async def test_ui_trigger_run_pipeline(
    client: AsyncClient, deps: AngarionDeps
) -> None:
    """Выбранный pipeline → прямой запуск: сырой QueueEnvelope в очередь."""
    resp = await client.post(
        '/ui/trigger',
        data={
            'transport': 'memory',
            'address': '-100',
            'text': 'hi',
            'pipeline': 'digest',
        },
    )
    assert resp.status_code == 200
    assert 'staged' in resp.text
    assert (await deps.queue.depth()).pending == 1
    item = await deps.queue.get()
    assert item.envelope.pipeline == 'digest'


async def test_ui_trigger_invalid_kind_is_422(client: AsyncClient) -> None:
    """Неизвестный kind валидируется FastAPI как RecordKind → 422, не 500."""
    resp = await client.post(
        '/ui/trigger',
        data={'transport': 'memory', 'address': '-100', 'kind': 'garbage'},
    )
    assert resp.status_code == 422


async def test_ui_dlq_renders_and_requeue(
    client: AsyncClient, dead_letters: MemoryDeadLetters, command_outbox: MemoryCommandOutbox
) -> None:
    letter = make_dead_letter(error='boom')
    await dead_letters.put(letter)
    page = await client.get('/ui/dlq')
    assert page.status_code == 200
    assert 'boom' in page.text
    resp = await client.post(
        f'/ui/dlq/{letter.uid}/requeue', follow_redirects=False
    )
    assert resp.status_code == 303
