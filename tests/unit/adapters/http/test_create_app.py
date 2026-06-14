"""
Фабрика ``create_app`` (§12.5, FR-1): контейнер портов в ``app.state``,
публичные DI-зависимости поверх портов для пользовательских ручек,
монтирование пользовательских и webhook-роутеров, заголовок (TDD).
"""

from __future__ import annotations

from conftest import asgi_client, make_settings
from fastapi import APIRouter

from angarion.adapters.http import AngarionDeps, create_app
from angarion.adapters.http.deps import (
    AnalyticsDep,
    CursorsDep,
    QueueDep,
    RegistryDep,
    StateDep,
)
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryMessageRegistry,
    MemoryStateStore,
)

probe = APIRouter(prefix='/api/my')


@probe.get('/ports')
async def _ports(
    analytics: AnalyticsDep,
    registry: RegistryDep,
    state: StateDep,
    queue: QueueDep,
    cursors: CursorsDep,
) -> dict[str, object]:
    """Пользовательская ручка, дотягивающаяся до всех опубликованных портов."""
    await state.set('probe', 'k', 'v')
    return {
        'state': await state.get('probe', 'k'),
        'registry_known': await registry.get('src', 'ext') is not None,
        'cursor_known': await cursors.load('src') is not None,
        'depth': (await queue.depth()).pending,
        'events': await analytics.recent(limit=1),
    }


hook = APIRouter()


@hook.post('/webhook/demo')
async def _webhook() -> dict[str, str]:
    """Заглушка webhook-роутера адаптера (push_transport=webhook, §12.11)."""
    return {'received': 'ok'}


def _make_deps(**overrides: object) -> AngarionDeps:
    fields: dict[str, object] = {
        'queue': MemoryQueue(),
        'analytics': MemoryAnalytics(),
        'registry': MemoryMessageRegistry(),
        'state': MemoryStateStore(),
        'cursors': MemoryCursorStore(),
        'settings': make_settings(),
    }
    fields.update(overrides)
    return AngarionDeps(**fields)


async def test_user_router_resolves_all_published_ports() -> None:
    app = create_app(_make_deps(), routers=[probe])
    async with asgi_client(app) as client:
        resp = await client.get('/api/my/ports')
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        'state': 'v',
        'registry_known': False,
        'cursor_known': False,
        'depth': 0,
        'events': [],
    }


async def test_webhook_routers_are_mounted() -> None:
    app = create_app(_make_deps(webhook_routers=(hook,)))
    async with asgi_client(app) as client:
        resp = await client.post('/webhook/demo')
    assert resp.status_code == 200
    assert resp.json() == {'received': 'ok'}


async def test_no_webhook_routers_by_default() -> None:
    app = create_app(_make_deps())
    async with asgi_client(app) as client:
        resp = await client.post('/webhook/demo')
    assert resp.status_code == 404


async def test_title_is_configurable() -> None:
    app = create_app(_make_deps(), title='my-service')
    assert app.title == 'my-service'
    assert app.state.angarion_deps.settings.pipelines == make_settings().pipelines


async def test_default_title() -> None:
    app = create_app(_make_deps())
    assert app.title == 'angarion'
