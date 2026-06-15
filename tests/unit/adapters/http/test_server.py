"""
ASGI-раннер ролей (§12.9, T024, фаза 4): serve_api / serve_combined +
вспомогательные ``_wait_any`` / ``_bootstrap_admin``. uvicorn-сервер
подменяется фейком (без реального bind порта).
"""

from __future__ import annotations

import asyncio

import pytest

from angarion.adapters.http import server
from angarion.config import AngarionSettings

pytestmark = pytest.mark.asyncio


def _memory_settings() -> AngarionSettings:
    return AngarionSettings.model_validate(
        {
            'storage': {'backend': 'memory'},
            'queue': {'backend': 'memory'},
            'api': {'auth': 'none', 'host': '127.0.0.1', 'port': 8099},
            'accounts': {'acc1': {'messenger': 'memory'}},
            'pipelines': {
                'digest': {
                    'processor': 'passthrough',
                    'events': ['message_new'],
                    'sources': [{'account': 'acc1', 'chat_id': '-100'}],
                    'targets': [{'account': 'acc1', 'chat_id': '-200'}],
                }
            },
        }
    )


class FakeServer:
    """``uvicorn.Server``-двойник: крутится до ``should_exit`` без сети."""

    def __init__(self) -> None:
        self.should_exit = False
        self.served = False

    async def serve(self) -> None:
        self.served = True
        while not self.should_exit:
            await asyncio.sleep(0.005)


def _patch_server(monkeypatch: pytest.MonkeyPatch) -> FakeServer:
    fake = FakeServer()
    monkeypatch.setattr(server, '_make_server', lambda _deps: fake)
    return fake


async def test_serve_api_starts_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _patch_server(monkeypatch)
    stop = asyncio.Event()
    stop.set()
    await server.serve_api(_memory_settings(), stop)
    assert fake.served is True
    assert fake.should_exit is True


async def test_serve_combined_starts_pipeline_and_web(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _patch_server(monkeypatch)
    stop = asyncio.Event()
    stop.set()
    await server.serve_combined(_memory_settings(), stop)
    assert fake.served is True
    assert fake.should_exit is True


async def test_serve_combined_stops_on_restart_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _patch_server(monkeypatch)
    real_build = server.build_app
    captured: list[object] = []

    def spy_build(settings: AngarionSettings) -> object:
        app = real_build(settings)
        captured.append(app)
        return app

    monkeypatch.setattr(server, 'build_app', spy_build)
    stop = asyncio.Event()  # не взводим — остановит restart_event
    task = asyncio.create_task(server.serve_combined(_memory_settings(), stop))
    for _ in range(200):
        if captured:
            break
        await asyncio.sleep(0.005)
    app = captured[0]
    app.restart_event.set()  # type: ignore[attr-defined]
    await asyncio.wait_for(task, timeout=2)
    assert fake.should_exit is True


async def test_wait_any_returns_on_first() -> None:
    a, b = asyncio.Event(), asyncio.Event()
    b.set()
    await asyncio.wait_for(server._wait_any(a, b), timeout=1)


async def test_bootstrap_admin_skips_without_sessionmaker() -> None:
    from angarion.adapters.http.composition import build_web_deps
    from angarion.adapters.memory.plugin import STORAGE_BACKEND
    from angarion.adapters.memory.queue import MemoryQueue
    from angarion.config import StorageConfig

    storage = STORAGE_BACKEND.make(StorageConfig.model_validate({'backend': 'memory'}))
    deps = build_web_deps(_memory_settings(), storage, MemoryQueue())
    await server._bootstrap_admin(deps)  # auth=none → sessionmaker None → no-op
