"""
HTTP-ручки ручного триггера (T038, §12.5): API-ключ-авторизация,
combined прямой ingest vs split через ``CommandOutbox``, прямой запуск
пайплайна, валидация payload.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from angarion.adapters.http import AngarionDeps, create_app
from angarion.adapters.http.trigger import API_KEY_HEADER, RUN_PIPELINE_OP
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
from angarion.config import ApiConfig
from angarion.domain.models import CommandKind, Record
from angarion.testing.factories import make_record

from conftest import asgi_client, make_settings

pytestmark = pytest.mark.asyncio

TOKEN = 's3cr3t-token'
EVENT_BODY = {'event': {'source': {'transport': 'memory', 'address': '-100'}}}


class FakeIngest:
    """``IngestService`` для combined-теста: собирает впрыснутые записи."""

    def __init__(self) -> None:
        self.ingested: list[Record] = []

    async def ingest(self, record: Record) -> None:
        self.ingested.append(record)


def build_deps(
    *,
    token: str = TOKEN,
    ingest: FakeIngest | None = None,
    queue: MemoryQueue | None = None,
    command_outbox: MemoryCommandOutbox | None = None,
    analytics: MemoryAnalytics | None = None,
) -> AngarionDeps:
    return AngarionDeps(
        queue=queue or MemoryQueue(),
        analytics=analytics or MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        runtime_config=MemoryRuntimeConfig(),
        command_outbox=command_outbox or MemoryCommandOutbox(),
        dead_letters=MemoryDeadLetters(),
        settings=make_settings(api=ApiConfig(auth='none', trigger_token=token)),
        ingest=ingest,
    )


def client_for(deps: AngarionDeps) -> AsyncClient:
    """ASGI-клиент поверх ``create_app(deps)`` (сам async-context-manager)."""
    return asgi_client(create_app(deps))


# --- авторизация по API-ключу ---


async def test_trigger_without_key_is_401() -> None:
    async with client_for(build_deps()) as client:
        resp = await client.post('/api/v1/trigger', json=EVENT_BODY)
        assert resp.status_code == 401


async def test_trigger_wrong_key_is_403() -> None:
    async with client_for(build_deps()) as client:
        resp = await client.post(
            '/api/v1/trigger', json=EVENT_BODY, headers={API_KEY_HEADER: 'nope'}
        )
        assert resp.status_code == 403


async def test_trigger_disabled_when_token_empty_is_503() -> None:
    async with client_for(build_deps(token='')) as client:
        resp = await client.post(
            '/api/v1/trigger', json=EVENT_BODY, headers={API_KEY_HEADER: 'anything'}
        )
        assert resp.status_code == 503


# --- combined прямой ingest vs split через outbox ---


async def test_trigger_combined_calls_ingest_directly() -> None:
    ingest = FakeIngest()
    async with client_for(build_deps(ingest=ingest)) as client:
        resp = await client.post(
            '/api/v1/trigger', json=EVENT_BODY, headers={API_KEY_HEADER: TOKEN}
        )
        assert resp.status_code == 202
        assert resp.json()['mode'] == 'ingested'
    assert len(ingest.ingested) == 1
    assert ingest.ingested[0].origin == 'manual'


async def test_trigger_split_enqueues_inject_command() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    deps = build_deps(ingest=None, command_outbox=outbox, analytics=analytics)
    async with client_for(deps) as client:
        resp = await client.post(
            '/api/v1/trigger', json=EVENT_BODY, headers={API_KEY_HEADER: TOKEN}
        )
        assert resp.status_code == 202
        assert resp.json()['mode'] == 'queued'
    commands = await outbox.take(10)
    assert [c.kind for c in commands] == [CommandKind.INJECT]
    # payload несёт сериализованный Record с origin='manual'
    assert commands[0].payload['record']['origin'] == 'manual'


async def test_trigger_accepts_full_record_body() -> None:
    ingest = FakeIngest()
    record = make_record(origin='manual')
    async with client_for(build_deps(ingest=ingest)) as client:
        resp = await client.post(
            '/api/v1/trigger',
            json={'record': record.model_dump(mode='json')},
            headers={API_KEY_HEADER: TOKEN},
        )
        assert resp.status_code == 202
    assert ingest.ingested[0].uid == record.uid


async def test_trigger_forces_manual_origin_on_full_record() -> None:
    # готовый Record с origin='live' → ручной путь приводит к 'manual'
    ingest = FakeIngest()
    record = make_record(origin='live')
    async with client_for(build_deps(ingest=ingest)) as client:
        resp = await client.post(
            '/api/v1/trigger',
            json={'record': record.model_dump(mode='json')},
            headers={API_KEY_HEADER: TOKEN},
        )
        assert resp.status_code == 202
    assert ingest.ingested[0].origin == 'manual'


# --- прямой запуск именованного пайплайна ---


async def test_run_pipeline_stages_envelope_and_audits() -> None:
    queue, analytics = MemoryQueue(), MemoryAnalytics()
    deps = build_deps(queue=queue, analytics=analytics)
    async with client_for(deps) as client:
        resp = await client.post(
            '/api/v1/run/digest', json=EVENT_BODY, headers={API_KEY_HEADER: TOKEN}
        )
        assert resp.status_code == 202
        assert resp.json()['mode'] == 'staged'
    assert (await queue.depth()).pending == 1
    item = await queue.get()
    assert item.envelope.pipeline == 'digest'
    audit = await analytics.recent(kind='admin_op')
    assert audit[0].payload['operation'] == RUN_PIPELINE_OP


async def test_run_unknown_pipeline_is_422() -> None:
    async with client_for(build_deps()) as client:
        resp = await client.post(
            '/api/v1/run/nope', json=EVENT_BODY, headers={API_KEY_HEADER: TOKEN}
        )
        assert resp.status_code == 422


# --- валидация тела (ровно одно из event/record) ---


async def test_trigger_neither_event_nor_record_is_422() -> None:
    async with client_for(build_deps()) as client:
        resp = await client.post(
            '/api/v1/trigger', json={}, headers={API_KEY_HEADER: TOKEN}
        )
        assert resp.status_code == 422


async def test_trigger_both_event_and_record_is_422() -> None:
    record = make_record()
    async with client_for(build_deps()) as client:
        resp = await client.post(
            '/api/v1/trigger',
            json={
                'event': {'source': {'transport': 'memory', 'address': '-100'}},
                'record': record.model_dump(mode='json'),
            },
            headers={API_KEY_HEADER: TOKEN},
        )
        assert resp.status_code == 422
