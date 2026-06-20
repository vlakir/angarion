"""
Сквозной тест «внутреннего провода» T037 (фаза 2): цепочка пайплайнов.

Конвейер целиком на InMemory-стеке через ``build_app`` (реальные entry points,
включая ``internal``): listener.emit → P1 (passthrough) → target=(internal,
stage1) → outbox → delivery → InternalSink.re-ingestion → P2 (passthrough) →
target=(memory, dst) → MemorySink. Проверяет, что выход P1 доходит до входа P2
**без** реальной отправки на платформу (SC), сквозную трассировку и инвариант
A1 (loop-guard не душит внутреннее ребро — иначе re-ingested запись была бы
задедуплена и цепочка не поехала бы).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from angarion.adapters.internal.sink import InternalSink
from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.sink import MemorySink
from angarion.bootstrap import build_app
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
    WorkerConfig,
)
from angarion.domain.models import RecordKind

if TYPE_CHECKING:
    from collections.abc import Callable

    from angarion.bootstrap import AngarionApp

SRC = '-100111'
DST = '-100222'
CHANNEL = 'stage1'
NEW = frozenset({RecordKind.NEW})


def make_settings() -> AngarionSettings:
    """Два аккаунта (memory + internal) и цепочка P1 →(internal)→ P2."""
    return AngarionSettings.model_validate(
        {
            'accounts': {
                'main': AccountConfig(transport='memory'),
                'wire': AccountConfig(transport='internal'),
            },
            'worker': WorkerConfig(
                backoff_base=0.001, backoff_cap=0.002, poll_interval=0.01
            ),
            'pipelines': {
                'p1': PipelineConfig(
                    processor='passthrough',
                    events=NEW,
                    sources=(EndpointConfig(account='main', address=SRC),),
                    targets=(EndpointConfig(account='wire', address=CHANNEL),),
                ),
                'p2': PipelineConfig(
                    processor='passthrough',
                    events=NEW,
                    sources=(EndpointConfig(account='wire', address=CHANNEL),),
                    targets=(EndpointConfig(account='main', address=DST),),
                ),
            },
        }
    )


async def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Дождаться условия с шагом 10 мс; таймаут — диагностический fail."""
    for _ in range(int(timeout / 0.01)):
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail('условие не выполнено за отведённый таймаут')


def memory_parts(app: AngarionApp) -> tuple[MemoryListener, MemorySink]:
    listener = app.listeners[0]
    assert isinstance(listener, MemoryListener)
    sink = app.sinks['memory']
    assert isinstance(sink, MemorySink)
    return listener, sink


class TestChain:
    """Выход P1 → вход P2 напрямую через внутренний транспорт."""

    async def test_sink_only_internal_has_no_listener(self) -> None:
        """A2: internal — sink-only; listener один (memory), sink internal есть."""
        app = build_app(make_settings())
        assert len(app.listeners) == 1
        assert isinstance(app.sinks['internal'], InternalSink)

    async def test_outbound_reaches_receiver_without_real_transport(self) -> None:
        """SC: запись проходит цепочку до MemorySink приёмника (A1-регресс)."""
        app = build_app(make_settings())
        await app.start()
        try:
            listener, sink = memory_parts(app)
            await listener.emit({'address': SRC, 'external_id': '1', 'text': 'chain'})
            await wait_until(lambda: len(sink.sent) >= 1)
        finally:
            await app.stop()
        assert [out.target.address for out in sink.sent] == [DST]
        assert sink.sent[0].text == 'chain'

    async def test_trace_id_propagates_through_chain(self) -> None:
        """SC: по записи на выходе цепочки прослеживается корень (trace_id)."""
        app = build_app(make_settings())
        await app.start()
        try:
            listener, sink = memory_parts(app)
            root = await listener.emit(
                {'address': SRC, 'external_id': '7', 'text': 'trace me'}
            )
            await wait_until(lambda: len(sink.sent) >= 1)
        finally:
            await app.stop()
        assert sink.sent[0].trace_id == root.trace_id == str(root.uid)

    async def test_internal_source_not_in_catchup_unavailable(self) -> None:
        """A8: внутренний источник не попадает в catchup-деградацию (ложный шум)."""
        app = build_app(make_settings())
        assert all('internal' not in key for key in app.catchup_unavailable)
