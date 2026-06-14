"""
ASGI-тесты SSR Web UI (§12.6, T022 фаза 3): дашборд ``/ui``, журнал
``/ui/events``, htmx-фрагменты ``/ui/fragments/*``, упакованные офлайн-
ассеты ``/ui/static/*`` и контракт расширения (``Page`` + автонавигация).

Проверяем статус-коды и наличие ключевых маркеров в HTML (значения
счётчиков, строки таблиц, пункты навигации) — без графического слоя
(spec §6). Тесты идут тем же ASGI-клиентом на InMemory-портах.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from angarion.adapters.http import AngarionDeps, Page, create_app
from angarion.domain.models import AnalyticsEvent, SourceCursor

from conftest import SOURCE_KEY, asgi_client

if TYPE_CHECKING:
    from httpx import AsyncClient

    from angarion.adapters.memory.storage import MemoryAnalytics, MemoryCursorStore


async def _record(analytics: MemoryAnalytics, kind: str, pipeline: str) -> None:
    await analytics.record(
        AnalyticsEvent(
            uid=uuid4(),
            kind=kind,
            pipeline=pipeline,
            at=datetime.now(UTC),
        )
    )


@pytest.mark.asyncio
async def test_dashboard_renders_html(client: AsyncClient) -> None:
    resp = await client.get('/ui')
    assert resp.status_code == 200
    assert resp.headers['content-type'].startswith('text/html')
    body = resp.text
    assert '<html' in body.lower()
    # навигация со встроенными страницами
    assert 'href="/ui"' in body
    assert 'href="/ui/events"' in body
    # таблица пайплайнов содержит сконфигурированный пайплайн
    assert 'digest' in body


@pytest.mark.asyncio
async def test_dashboard_assets_are_local(client: AsyncClient) -> None:
    """Офлайн-режим (§12.6): ассеты из пакета, без внешних CDN."""
    body = (await client.get('/ui')).text
    assert '/ui/static/pico.min.css' in body
    assert '/ui/static/htmx.min.js' in body
    assert 'cdn.jsdelivr' not in body
    assert 'unpkg' not in body


@pytest.mark.asyncio
async def test_static_assets_served(client: AsyncClient) -> None:
    css = await client.get('/ui/static/pico.min.css')
    assert css.status_code == 200
    assert 'css' in css.headers['content-type']
    assert 'Pico CSS' in css.text
    js = await client.get('/ui/static/htmx.min.js')
    assert js.status_code == 200
    assert 'javascript' in js.headers['content-type']
    assert 'htmx' in js.text


@pytest.mark.asyncio
async def test_fragment_queue(client: AsyncClient) -> None:
    """Фрагмент поллинга — партиал без обёртки <html>, с глубиной очереди."""
    resp = await client.get('/ui/fragments/queue')
    assert resp.status_code == 200
    body = resp.text
    assert '<html' not in body.lower()
    assert 'pending' in body.lower()


@pytest.mark.asyncio
async def test_fragment_cursors_shows_state(
    deps: AngarionDeps, cursors: MemoryCursorStore
) -> None:
    moment = datetime.now(UTC)
    await cursors.save(SourceCursor(source_key=SOURCE_KEY, updated_at=moment))
    async with asgi_client(create_app(deps)) as client:
        resp = await client.get('/ui/fragments/cursors')
        assert resp.status_code == 200
        body = resp.text
        assert SOURCE_KEY in body
        assert moment.isoformat() in body


@pytest.mark.asyncio
async def test_dashboard_event_counts(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    await _record(analytics, 'delivered', 'digest')
    await _record(analytics, 'delivered', 'digest')
    await _record(analytics, 'dropped', 'digest')
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/fragments/event-counts')).text
        assert 'delivered' in body
        assert '2' in body
        assert 'dropped' in body


@pytest.mark.asyncio
async def test_events_page_lists_events(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    await _record(analytics, 'delivered', 'digest')
    async with asgi_client(create_app(deps)) as client:
        resp = await client.get('/ui/events')
        assert resp.status_code == 200
        body = resp.text
        assert '<table' in body.lower()
        assert 'delivered' in body


@pytest.mark.asyncio
async def test_events_filter_by_kind(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    await _record(analytics, 'delivered', 'digest')
    await _record(analytics, 'dropped', 'digest')
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/fragments/events-table?kind=delivered')).text
        assert 'delivered' in body
        assert 'dropped' not in body


@pytest.mark.asyncio
async def test_custom_page_appears_in_nav(deps: AngarionDeps) -> None:
    ui = APIRouter()

    @ui.get('/ui/digest', response_class=HTMLResponse)
    async def digest_page() -> HTMLResponse:
        return HTMLResponse('<p>digest body</p>')

    app = create_app(deps, pages=[Page(title='Digest', path='/ui/digest', router=ui)])
    async with asgi_client(app) as client:
        home = (await client.get('/ui')).text
        assert 'Digest' in home
        assert 'href="/ui/digest"' in home
        page = await client.get('/ui/digest')
        assert page.status_code == 200
        assert 'digest body' in page.text
