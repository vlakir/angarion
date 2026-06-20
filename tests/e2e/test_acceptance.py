"""
Сквозные acceptance-тесты T002 (Фаза 5; §16 M1, SC-1).

Конвейер целиком на InMemory-адаптерах через bootstrap: listener.emit →
ingest (дедуп → реестр → router) → очередь → worker → outbox →
delivery → sink. Сценарии:

- new/edited/deleted с обогащением из реестра (previous_text у EDITED,
  восстановление текста у DELETED) + multicast на два пайплайна (SC-1);
- ретраи обработки с исчерпанием → DLQ;
- дедуп повторной подачи того же события.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.sink import MemorySink
from angarion.application import processors
from angarion.application.processors import FunctionProcessor
from angarion.bootstrap import build_app
from angarion.config import (
    AccountConfig,
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
    WorkerConfig,
)
from angarion.domain.errors import ProcessingError
from angarion.domain.models import (
    OutboundRecord,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Record,
    RecordKind,
    Verdict,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from angarion.bootstrap import AngarionApp

SRC_CHAT = '-100111'
DST_DIGEST = '-100222'
DST_MIRROR = '-100333'
ALL_KINDS = frozenset(
    {RecordKind.NEW, RecordKind.EDITED, RecordKind.DELETED}
)


async def annotate(
    event: Record, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    """Процессор-свидетель обогащения: текст исходящего = kind|text|previous_text."""
    text = f'{event.kind}|{event.text}|{event.previous_text}'
    outbound = [
        OutboundRecord(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=text,
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


async def explode(
    event: Record, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    """Всегда падающий процессор — для ретрай-сценария до DLQ."""
    msg = 'boom'
    raise ProcessingError(msg)


@pytest.fixture
def sandbox_processors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Изолированная копия реестра процессоров: тестовые имена не утекают."""
    monkeypatch.setattr(processors, '_registry', dict(processors.registered()))
    processors.register(FunctionProcessor(name='annotate', fn=annotate))
    processors.register(FunctionProcessor(name='explode', fn=explode))


def make_pipeline(processor: str, target_chat: str) -> PipelineConfig:
    return PipelineConfig(
        processor=processor,
        events=ALL_KINDS,
        sources=(EndpointConfig(account='main', address=SRC_CHAT),),
        targets=(EndpointConfig(account='main', address=target_chat),),
    )


def make_settings(
    pipelines: dict[str, PipelineConfig], *, max_retries: int = 5
) -> AngarionSettings:
    """Конфиг для E2E: backoff/poll ужаты, чтобы ретраи шли мгновенно."""
    return AngarionSettings.model_validate(
        {
            'accounts': {'main': AccountConfig(transport='memory')},
            'worker': WorkerConfig(
                max_retries=max_retries,
                backoff_base=0.001,
                backoff_cap=0.002,
                poll_interval=0.01,
            ),
            'pipelines': pipelines,
        }
    )


def app_parts(app: AngarionApp) -> tuple[MemoryListener, MemorySink]:
    listener = app.listeners[0]
    assert isinstance(listener, MemoryListener)
    sink = app.sinks['memory']
    assert isinstance(sink, MemorySink)
    return listener, sink


async def wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Дождаться условия с шагом 10 мс; таймаут — диагностический fail."""
    for _ in range(int(timeout / 0.01)):
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail('условие не выполнено за отведённый таймаут')


class TestEndToEndMulticast:
    """SC-1: new/edited/deleted + обогащение + multicast, всё через bootstrap."""

    async def test_new_edited_deleted_with_enrichment_multicast(
        self, sandbox_processors: None
    ) -> None:
        settings = make_settings(
            {
                'digest': make_pipeline('annotate', DST_DIGEST),
                'mirror': make_pipeline('annotate', DST_MIRROR),
            }
        )
        app = build_app(settings)
        await app.start()
        try:
            listener, sink = app_parts(app)
            await listener.emit(
                {'address': SRC_CHAT, 'external_id': '7', 'text': 'hello'}
            )
            await wait_until(lambda: len(sink.sent) >= 2)
            await listener.emit(
                {
                    'address': SRC_CHAT,
                    'external_id': '7',
                    'kind': 'edited',
                    'text': 'hello v2',
                }
            )
            await wait_until(lambda: len(sink.sent) >= 4)
            await listener.emit(
                {
                    'address': SRC_CHAT,
                    'external_id': '7',
                    'kind': 'deleted',
                }
            )
            await wait_until(lambda: len(sink.sent) >= 6)
        finally:
            await app.stop()

        texts_by_target: dict[str, list[str]] = {}
        for msg in sink.sent:
            texts_by_target.setdefault(msg.target.address, []).append(msg.text)
        expected = [
            'new|hello|None',
            # EDITED обогащено вытесненной версией из реестра (§6.1)
            'edited|hello v2|hello',
            # DELETED пришло без текста — текст восстановлен реестром (§6.1)
            'deleted|hello v2|None',
        ]
        assert texts_by_target == {DST_DIGEST: expected, DST_MIRROR: expected}

        ingested = await app.storage.analytics.recent(kind='ingested')
        assert len(ingested) == 3
        assert all(
            event.payload['pipelines'] == ['digest', 'mirror'] for event in ingested
        )


class TestRetryToDeadLetters:
    """§8: исключение процессора → ретраи с backoff → DLQ после исчерпания."""

    async def test_processing_failure_exhausts_retries_into_dlq(
        self, sandbox_processors: None
    ) -> None:
        settings = make_settings(
            {'flaky': make_pipeline('explode', DST_DIGEST)}, max_retries=2
        )
        app = build_app(settings)
        await app.start()
        try:
            listener, sink = app_parts(app)
            await listener.emit(
                {'address': SRC_CHAT, 'external_id': '1', 'text': 'x'}
            )
            letters = await app.storage.dead_letters.list()
            for _ in range(500):
                if letters:
                    break
                await asyncio.sleep(0.01)
                letters = await app.storage.dead_letters.list()
        finally:
            await app.stop()

        assert len(letters) == 1
        assert letters[0].envelope.pipeline == 'flaky'
        assert letters[0].envelope.attempt == 2  # все ретраи исчерпаны
        assert 'boom' in letters[0].error
        failed = await app.storage.analytics.recent(kind='failed')
        assert len(failed) == 1
        assert not sink.sent
        depth = await app.queue.depth()
        assert (depth.pending, depth.unacked) == (0, 0)


class TestInboundDedup:
    """§7.2: повторная подача того же события — дубль, конвейер не трогается."""

    async def test_resubmitted_event_is_deduplicated(
        self, sandbox_processors: None
    ) -> None:
        settings = make_settings({'digest': make_pipeline('annotate', DST_DIGEST)})
        app = build_app(settings)
        await app.start()
        try:
            listener, sink = app_parts(app)
            raw = {'address': SRC_CHAT, 'external_id': '42', 'text': 'однажды'}
            await listener.emit(raw)
            await wait_until(lambda: len(sink.sent) == 1)
            await listener.emit(raw)
            duplicates = await app.storage.analytics.recent(kind='duplicate')
            assert len(duplicates) == 1
            await asyncio.sleep(0.05)  # дубль не должен дойти до доставки
            assert len(sink.sent) == 1
        finally:
            await app.stop()
