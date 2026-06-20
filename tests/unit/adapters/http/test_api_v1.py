"""
Встроенный роутер ``/api/v1`` (§12.5, FR-1): health / diagnostics /
events — read-only, поверх портов, через ASGI на InMemory (TDD).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from conftest import SOURCE_KEY, asgi_client, make_settings
from httpx import AsyncClient

from angarion import __version__
from angarion.adapters.http import AngarionDeps, create_app
from angarion.adapters.memory.storage import MemoryAnalytics, MemoryCursorStore
from angarion.config import EndpointConfig, PipelineConfig
from angarion.domain.models import AnalyticsEvent, RecordKind, SourceCursor

NOW = datetime(2026, 6, 14, 12, 0, tzinfo=UTC)


async def test_health_has_no_port_access(client: AsyncClient) -> None:
    resp = await client.get('/api/v1/health')
    assert resp.status_code == 200
    assert resp.json() == {'status': 'ok', 'version': __version__}


async def test_diagnostics_empty_shape(client: AsyncClient) -> None:
    resp = await client.get('/api/v1/diagnostics')
    assert resp.status_code == 200
    body = resp.json()
    assert body['queue'] == {'pending': 0, 'unacked': 0}
    assert body['events_24h'] == {}
    assert body['pipelines'] == ['digest']
    assert body['cursors'] == [{'source_key': SOURCE_KEY, 'updated_at': None}]
    assert body['uptime_seconds'] >= 0.0


async def test_diagnostics_counts_events_in_window(
    client: AsyncClient, analytics: MemoryAnalytics
) -> None:
    await analytics.record(
        AnalyticsEvent(uid=uuid4(), kind='delivered', at=datetime.now(UTC))
    )
    await analytics.record(
        AnalyticsEvent(uid=uuid4(), kind='delivered', at=datetime.now(UTC))
    )
    await analytics.record(
        AnalyticsEvent(
            uid=uuid4(), kind='ancient', at=datetime.now(UTC) - timedelta(hours=48)
        )
    )
    resp = await client.get('/api/v1/diagnostics')
    assert resp.json()['events_24h'] == {'delivered': 2}


async def test_diagnostics_reports_saved_cursor(
    client: AsyncClient, cursors: MemoryCursorStore
) -> None:
    await cursors.save(SourceCursor(source_key=SOURCE_KEY, updated_at=NOW))
    resp = await client.get('/api/v1/diagnostics')
    (state,) = resp.json()['cursors']
    assert state['source_key'] == SOURCE_KEY
    assert datetime.fromisoformat(state['updated_at']) == NOW


async def test_diagnostics_skips_source_with_unknown_account(
    deps: AngarionDeps,
) -> None:
    settings = make_settings(
        pipelines={
            'digest': PipelineConfig(
                processor='passthrough',
                events=frozenset({RecordKind.NEW}),
                sources=(EndpointConfig(account='ghost', address='-100'),),
                targets=(EndpointConfig(account='acc1', address='-200'),),
            )
        }
    )
    patched = deps.model_copy(update={'settings': settings})
    async with asgi_client(create_app(patched)) as client:
        resp = await client.get('/api/v1/diagnostics')
        assert resp.json()['cursors'] == []


async def test_events_empty(client: AsyncClient) -> None:
    resp = await client.get('/api/v1/events')
    assert resp.status_code == 200
    assert resp.json() == {'events': []}


async def test_events_returns_recent_newest_first(
    client: AsyncClient, analytics: MemoryAnalytics
) -> None:
    await analytics.record(
        AnalyticsEvent(uid=uuid4(), kind='a', pipeline='digest', at=NOW)
    )
    await analytics.record(
        AnalyticsEvent(
            uid=uuid4(), kind='b', pipeline='digest', at=NOW + timedelta(minutes=1)
        )
    )
    resp = await client.get('/api/v1/events')
    assert [e['kind'] for e in resp.json()['events']] == ['b', 'a']


async def test_events_filters_by_kind(
    client: AsyncClient, analytics: MemoryAnalytics
) -> None:
    await analytics.record(
        AnalyticsEvent(uid=uuid4(), kind='keep', pipeline='digest', at=NOW)
    )
    await analytics.record(
        AnalyticsEvent(uid=uuid4(), kind='skip', pipeline='other', at=NOW)
    )
    resp = await client.get('/api/v1/events', params={'kind': 'keep'})
    assert [e['kind'] for e in resp.json()['events']] == ['keep']


async def test_events_limit_out_of_range_is_422(client: AsyncClient) -> None:
    assert (await client.get('/api/v1/events', params={'limit': 999})).status_code == 422
    assert (await client.get('/api/v1/events', params={'limit': 0})).status_code == 422
