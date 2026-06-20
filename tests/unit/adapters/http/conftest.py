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
    MemoryCommandOutbox,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryMessageRegistry,
    MemoryRuntimeConfig,
    MemoryStateStore,
)
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    ApiConfig,
    EndpointConfig,
    PipelineConfig,
)
from angarion.domain.models import RecordKind

SOURCE_KEY = 'memory:acc1:-100'


def make_settings(**overrides: object) -> AngarionSettings:
    """
    Минимальная конфигурация с одним пайплайном memory:acc1 → acc1.

    По умолчанию ``api.auth="none"`` — тесты не про auth работают без
    логина (синтетический локальный админ); auth-тесты задают свою
    ``api``-секцию явно через ``overrides``.
    """
    fields: dict[str, object] = {
        'accounts': {'acc1': AccountConfig(transport='memory')},
        'api': ApiConfig(auth='none'),
        'pipelines': {
            'digest': PipelineConfig(
                processor='passthrough',
                events=frozenset({RecordKind.NEW}),
                sources=(EndpointConfig(account='acc1', address='-100'),),
                targets=(EndpointConfig(account='acc1', address='-200'),),
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
def runtime_config() -> MemoryRuntimeConfig:
    return MemoryRuntimeConfig()


@pytest.fixture
def command_outbox() -> MemoryCommandOutbox:
    return MemoryCommandOutbox()


@pytest.fixture
def dead_letters() -> MemoryDeadLetters:
    return MemoryDeadLetters()


@pytest.fixture
def deps(
    analytics: MemoryAnalytics,
    cursors: MemoryCursorStore,
    state: MemoryStateStore,
    registry: MemoryMessageRegistry,
    runtime_config: MemoryRuntimeConfig,
    command_outbox: MemoryCommandOutbox,
    dead_letters: MemoryDeadLetters,
) -> AngarionDeps:
    return AngarionDeps(
        queue=MemoryQueue(),
        analytics=analytics,
        registry=registry,
        state=state,
        cursors=cursors,
        runtime_config=runtime_config,
        command_outbox=command_outbox,
        dead_letters=dead_letters,
        settings=make_settings(),
    )


@pytest.fixture
async def client(deps: AngarionDeps) -> AsyncIterator[AsyncClient]:
    async with asgi_client(create_app(deps)) as c:
        yield c
