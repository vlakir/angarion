"""
Фикстуры HTTP-адаптера (§12.5): контейнер портов на InMemory + ASGI-клиент
через ``httpx.ASGITransport`` поверх ``create_app`` — без сети, БД и
Telegram (TDD-стратегия spec §6).
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from angarion.adapters.http import AngarionDeps, create_app
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryMessageRegistry,
    MemoryStateStore,
)
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
)
from angarion.domain.models import EventKind

SOURCE_KEY = 'memory:acc1:-100'


def make_settings(**overrides: object) -> AngarionSettings:
    """Минимальная конфигурация с одним пайплайном memory:acc1 → acc1."""
    fields: dict[str, object] = {
        'accounts': {'acc1': AccountConfig(messenger='memory')},
        'pipelines': {
            'digest': PipelineConfig(
                processor='passthrough',
                events=frozenset({EventKind.MESSAGE_NEW}),
                sources=(EndpointConfig(account='acc1', chat_id='-100'),),
                targets=(EndpointConfig(account='acc1', chat_id='-200'),),
            )
        },
    }
    fields.update(overrides)
    return AngarionSettings(**fields)


def asgi_client(app: FastAPI) -> AsyncClient:
    """ASGI-клиент httpx поверх приложения (без сети)."""
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


@pytest.fixture
def analytics() -> MemoryAnalytics:
    return MemoryAnalytics()


@pytest.fixture
def cursors() -> MemoryCursorStore:
    return MemoryCursorStore()


@pytest.fixture
def state() -> MemoryStateStore:
    return MemoryStateStore()


@pytest.fixture
def registry() -> MemoryMessageRegistry:
    return MemoryMessageRegistry()


@pytest.fixture
def deps(
    analytics: MemoryAnalytics,
    cursors: MemoryCursorStore,
    state: MemoryStateStore,
    registry: MemoryMessageRegistry,
) -> AngarionDeps:
    return AngarionDeps(
        queue=MemoryQueue(),
        analytics=analytics,
        registry=registry,
        state=state,
        cursors=cursors,
        settings=make_settings(),
    )


@pytest.fixture
async def client(deps: AngarionDeps) -> AsyncIterator[AsyncClient]:
    async with asgi_client(create_app(deps)) as c:
        yield c
