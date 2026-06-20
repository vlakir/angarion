"""
ASGI-тесты страницы топологии ``/ui/pipelines`` (§12.6, T025 фаза 1):
трёхдольный граф «источники → пайплайны → получатели», SVG генерируется
на сервере (Jinja, без графических JS-библиотек), цвет узла = статус
(активен / пауза / failed за час), аннотации delivered/depth и админская
htmx-форма pause/resume по узлу.

Проверяем статус-коды и маркеры в HTML/SVG (теги ``<svg>``, имена узлов,
``data-status``, аннотации, ``hx-post``) — без графического слоя
(spec §6). Тот же ASGI-клиент на InMemory-портах.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from angarion.adapters.http import create_app
from angarion.adapters.http.viz import build_pipeline_graph
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
)
from angarion.domain.models import AnalyticsEvent, DynamicSettings, RecordKind

from conftest import asgi_client, make_settings

if TYPE_CHECKING:
    from httpx import AsyncClient

    from angarion.adapters.http import AngarionDeps
    from angarion.adapters.memory.storage import (
        MemoryAnalytics,
        MemoryRuntimeConfig,
    )


async def _record(
    analytics: MemoryAnalytics,
    kind: str,
    pipeline: str,
    *,
    at: datetime | None = None,
) -> None:
    await analytics.record(
        AnalyticsEvent(
            uid=uuid4(),
            kind=kind,
            pipeline=pipeline,
            at=at or datetime.now(UTC),
        )
    )


@pytest.mark.asyncio
async def test_pipelines_page_renders_server_side_svg(client: AsyncClient) -> None:
    """Граф рендерится на сервере: страница содержит ``<svg>`` и узлы."""
    resp = await client.get('/ui/pipelines')
    assert resp.status_code == 200
    assert resp.headers['content-type'].startswith('text/html')
    body = resp.text
    assert '<svg' in body
    # средняя доля — сконфигурированный пайплайн
    assert 'digest' in body
    # крайние доли — адреса источника (-100) и получателя (-200)
    assert '-100' in body
    assert '-200' in body
    # офлайн: SVG рисуется разметкой, без внешних CDN-библиотек
    assert 'cdn' not in body
    assert 'unpkg' not in body


@pytest.mark.asyncio
async def test_pipelines_in_builtin_nav(client: AsyncClient) -> None:
    """Пункт навигации ``Pipelines`` доступен со всех страниц (base.html)."""
    body = (await client.get('/ui')).text
    assert 'href="/ui/pipelines"' in body


@pytest.mark.asyncio
async def test_node_active_by_default(client: AsyncClient) -> None:
    """Без событий и паузы узел пайплайна — статус ``active``."""
    body = (await client.get('/ui/pipelines')).text
    assert 'data-status="active"' in body


@pytest.mark.asyncio
async def test_node_paused_status(
    deps: AngarionDeps, runtime_config: MemoryRuntimeConfig
) -> None:
    """Пауза из ``runtime_config`` отражается статусом ``paused``."""
    await runtime_config.save(DynamicSettings(paused_pipelines=frozenset({'digest'})))
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'data-status="paused"' in body


@pytest.mark.asyncio
async def test_node_failed_status_within_hour(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    """``failed`` за последний час → статус узла ``failed``."""
    await _record(analytics, 'failed', 'digest')
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'data-status="failed"' in body


@pytest.mark.asyncio
async def test_delivery_failed_also_marks_failed(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    """Терминальный ``delivery_failed`` тоже окрашивает узел в failed."""
    await _record(analytics, 'delivery_failed', 'digest')
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'data-status="failed"' in body


@pytest.mark.asyncio
async def test_old_failure_does_not_mark_failed(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    """``failed`` старше часа не влияет на текущий статус (окно — час)."""
    old = datetime.now(UTC) - timedelta(hours=2)
    await _record(analytics, 'failed', 'digest', at=old)
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'data-status="active"' in body
        assert 'data-status="failed"' not in body


@pytest.mark.asyncio
async def test_paused_precedes_failed(
    deps: AngarionDeps,
    analytics: MemoryAnalytics,
    runtime_config: MemoryRuntimeConfig,
) -> None:
    """Пауза — осознанное состояние оператора, важнее свежего failed."""
    await _record(analytics, 'failed', 'digest')
    await runtime_config.save(DynamicSettings(paused_pipelines=frozenset({'digest'})))
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'data-status="paused"' in body
        assert 'data-status="failed"' not in body


@pytest.mark.asyncio
async def test_delivered_annotation(
    deps: AngarionDeps, analytics: MemoryAnalytics
) -> None:
    """Аннотация delivered считает доставки за окно дашборда."""
    await _record(analytics, 'delivered', 'digest')
    await _record(analytics, 'delivered', 'digest')
    async with asgi_client(create_app(deps)) as client:
        body = (await client.get('/ui/pipelines')).text
        assert 'delivered: 2' in body


@pytest.mark.asyncio
async def test_depth_annotation(client: AsyncClient) -> None:
    """Глубина очереди отображается аннотацией на странице."""
    body = (await client.get('/ui/pipelines')).text
    assert 'pending' in body.lower()


@pytest.mark.asyncio
async def test_admin_sees_pause_controls(client: AsyncClient) -> None:
    """Под ``auth="none"`` (синтетический админ) узел кликабелен (htmx)."""
    body = (await client.get('/ui/pipelines')).text
    # клик по узлу = htmx-pause активного пайплайна
    assert 'hx-post="/ui/pipelines/digest/pause"' in body
    assert 'hx-target="#pipeline-graph"' in body


@pytest.mark.asyncio
async def test_node_pause_via_htmx_returns_graph_fragment(
    client: AsyncClient,
) -> None:
    """htmx-pause узла возвращает обновлённый фрагмент графа (а не редирект)."""
    resp = await client.post(
        '/ui/pipelines/digest/pause', headers={'HX-Request': 'true'}
    )
    assert resp.status_code == 200
    body = resp.text
    assert 'id="pipeline-graph"' in body
    assert 'data-status="paused"' in body
    # теперь активный пайплайн кликается на resume
    assert 'hx-post="/ui/pipelines/digest/resume"' in body


@pytest.mark.asyncio
async def test_settings_form_pause_still_redirects(client: AsyncClient) -> None:
    """Обычная (не-htmx) форма ``/ui/settings`` сохраняет редирект (T024)."""
    resp = await client.post(
        '/ui/pipelines/digest/pause', follow_redirects=False
    )
    assert resp.status_code == 303
    assert resp.headers['location'] == '/ui/settings'


@pytest.mark.asyncio
async def test_pipelines_fragment_polling_endpoint(client: AsyncClient) -> None:
    """Фрагмент поллинга — граф без обёртки ``<html>``."""
    resp = await client.get('/ui/fragments/pipelines')
    assert resp.status_code == 200
    body = resp.text
    assert '<html' not in body.lower()
    assert 'id="pipeline-graph"' in body
    assert '<svg' in body


# --- T037 фаза 3: визуализация внутренних цепочек ---


def _chain_settings() -> AngarionSettings:
    """``first`` (mem:-100 → wire:ch1) ⇒ ``second`` (wire:ch1 → mem:-200)."""
    return make_settings(
        accounts={
            'mem': AccountConfig(transport='memory'),
            'wire': AccountConfig(transport='internal'),
        },
        pipelines={
            'first': PipelineConfig(
                processor='passthrough',
                events=frozenset({RecordKind.NEW}),
                sources=(EndpointConfig(account='mem', address='-100'),),
                targets=(EndpointConfig(account='wire', address='ch1'),),
            ),
            'second': PipelineConfig(
                processor='passthrough',
                events=frozenset({RecordKind.NEW}),
                sources=(EndpointConfig(account='wire', address='ch1'),),
                targets=(EndpointConfig(account='mem', address='-200'),),
            ),
        },
    )


@pytest.mark.asyncio
async def test_internal_channel_collapses_to_edge(deps: AngarionDeps) -> None:
    """Внутренний канал не висит коробкой — схлопнут в одно ребро цепочки."""
    graph = await build_pipeline_graph(
        deps.model_copy(update={'settings': _chain_settings()})
    )
    # колонки несут только внешние эндпоинты (mem:-100 / mem:-200), не канал
    assert len(graph.sources) == 1
    assert len(graph.targets) == 1
    labels = [n.label for n in (*graph.sources, *graph.targets)]
    assert not any(lbl.startswith('internal:') for lbl in labels)
    # ровно одно внутреннее ребро поверх двух внешних (source→first, second→target)
    internal = [e for e in graph.edges if e.internal]
    assert len(internal) == 1
    assert sum(1 for e in graph.edges if not e.internal) == 2


@pytest.mark.asyncio
async def test_internal_edge_connects_pipeline_nodes(deps: AngarionDeps) -> None:
    """Внутреннее ребро соединяет узлы-пайплайны (producer→consumer), не коробки."""
    graph = await build_pipeline_graph(
        deps.model_copy(update={'settings': _chain_settings()})
    )
    nodes = {n.key: n for n in graph.pipelines}
    first, second = nodes['first'], nodes['second']
    (edge,) = [e for e in graph.edges if e.internal]
    # оба конца — в колонке пайплайнов, на центрах узлов producer/consumer
    assert edge.x1 == first.x == edge.x2 == second.x
    assert edge.y1 == first.y + first.height // 2
    assert edge.y2 == second.y + second.height // 2
    # кривая выгибается влево — визуально отличимая маршрутизация
    assert edge.cx < edge.x1


@pytest.mark.asyncio
async def test_chain_edge_rendered_distinctly(deps: AngarionDeps) -> None:
    """SVG рисует ребро цепочки пунктиром со стрелкой направления."""
    chain_deps = deps.model_copy(update={'settings': _chain_settings()})
    async with asgi_client(create_app(chain_deps)) as client:
        body = (await client.get('/ui/pipelines')).text
    assert 'data-role="chain-edge"' in body
    assert 'stroke-dasharray' in body
    assert 'marker-end="url(#chain-arrow)"' in body
    # оба пайплайна — узлы; внутренний канал коробкой не висит
    assert '>first<' in body
    assert '>second<' in body
    assert 'internal:' not in body
