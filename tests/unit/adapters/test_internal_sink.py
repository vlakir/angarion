"""
InternalSink (T037): re-ingestion внутреннего провода.

``OutboundRecord`` пайплайна-источника замыкается обратно на ``IngestService``
как ``Record(kind=new, source=(internal, канал))``. Проверяется построение
записи (источник/аккаунт/ключи/hops/trace_id/origin), детерминизм ключей стыка
(повтор ребра → дубль гасит dedup) и инъективность при fan-out по разным
каналам, а также проброс медиа.
"""

from __future__ import annotations

import pytest

from angarion.adapters.internal.sink import InternalSink
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.domain.keys import make_dedup_key, make_internal_keys, make_source_key
from angarion.domain.models import (
    INTERNAL_TRANSPORT,
    AccountRef,
    Endpoint,
    MediaRef,
    OutboundRecord,
    QueueEnvelope,
    RecordKind,
)

CHANNEL = 'stage1'
WIRE = 'wire'
SOURCE_KEY = make_source_key(INTERNAL_TRANSPORT, WIRE, CHANNEL)


def _route(channel: str = CHANNEL, pipeline: str = 'p2') -> RouteSpec:
    """Маршрут приёмника на внутренний канал."""
    return RouteSpec(
        pipeline=pipeline,
        events=frozenset({RecordKind.NEW}),
        sources=(Endpoint(transport=INTERNAL_TRANSPORT, address=channel),),
    )


def _make_sink(*routes: RouteSpec) -> tuple[InternalSink, MemoryQueue]:
    """InternalSink поверх реального ingest; очередь — точка наблюдения."""
    queue = MemoryQueue()
    ingest = IngestService(
        dedup=MemoryDedupStore(),
        registry=MemoryMessageRegistry(),
        router=Router(routes or (_route(),)),
        queue=queue,
        analytics=MemoryAnalytics(),
        dead_letters=MemoryDeadLetters(),
    )
    return InternalSink(ingest=ingest), queue


def _outbound(
    *,
    channel: str = CHANNEL,
    idempotency_key: str = 'ik-1',
    text: str = 'payload',
    hops: int = 0,
    trace_id: str | None = 'root-trace',
    media: list[MediaRef] | None = None,
) -> OutboundRecord:
    return OutboundRecord(
        idempotency_key=idempotency_key,
        target=Endpoint(transport=INTERNAL_TRANSPORT, address=channel),
        send_via=AccountRef(transport=INTERNAL_TRANSPORT, account_id=WIRE),
        text=text,
        hops=hops,
        trace_id=trace_id,
        media=media or [],
    )


async def _drain(queue: MemoryQueue) -> list[QueueEnvelope]:
    out: list[QueueEnvelope] = []
    while (await queue.depth()).pending:
        item = await queue.get()
        out.append(item.envelope)
        await queue.ack(item)
    return out


class TestReingestion:
    """Sink преобразует OutboundRecord в входную Record приёмника."""

    async def test_builds_reingested_record(self) -> None:
        sink, queue = _make_sink()
        receipt = await sink.send(_outbound())
        (envelope,) = await _drain(queue)
        record = envelope.record
        assert envelope.pipeline == 'p2'
        assert record.kind is RecordKind.NEW
        assert record.origin == 'internal'
        assert record.source == Endpoint(
            transport=INTERNAL_TRANSPORT, address=CHANNEL
        )
        assert record.received_by == AccountRef(
            transport=INTERNAL_TRANSPORT, account_id=WIRE
        )
        external_id, dedup_key = make_internal_keys('ik-1', SOURCE_KEY)
        assert record.external_id == external_id == 'ik-1'
        assert record.dedup_key == dedup_key
        assert record.dedup_key == make_dedup_key(RecordKind.NEW, SOURCE_KEY, 'ik-1')
        assert receipt.external_id == external_id

    async def test_increments_hops_and_carries_trace(self) -> None:
        sink, queue = _make_sink()
        await sink.send(_outbound(hops=2, trace_id='root-trace'))
        (envelope,) = await _drain(queue)
        assert envelope.record.hops == 3
        assert envelope.record.trace_id == 'root-trace'

    async def test_trace_id_defaults_to_own_uid_when_absent(self) -> None:
        """Без trace_id (не звено цепочки) запись — корень собственной трассы."""
        sink, queue = _make_sink()
        await sink.send(_outbound(trace_id=None))
        (envelope,) = await _drain(queue)
        assert envelope.record.trace_id == str(envelope.record.uid)

    async def test_carries_media_through(self) -> None:
        sink, queue = _make_sink()
        media = [MediaRef(kind='photo', ref='abc', file_name='p.jpg')]
        await sink.send(_outbound(media=media))
        (envelope,) = await _drain(queue)
        assert envelope.record.media == media
        assert envelope.record.media_hash is not None


class TestStitchKeyDeterminism:
    """Q3: ключи стыка детерминированы из idempotency_key (at-least-once)."""

    async def test_redelivered_edge_deduped_to_single_record(self) -> None:
        """Повтор доставки ребра (тот же idempotency_key) → ровно одна запись."""
        sink, queue = _make_sink()
        await sink.send(_outbound(idempotency_key='ik-dup'))
        await sink.send(_outbound(idempotency_key='ik-dup'))
        assert len(await _drain(queue)) == 1

    async def test_fanout_distinct_channels_not_deduped(self) -> None:
        """A6: fan-out по разным каналам — разные dedup_key, обе записи едут."""
        sink, queue = _make_sink(_route('chA', 'pa'), _route('chB', 'pb'))
        await sink.send(_outbound(channel='chA', idempotency_key='ik->chA'))
        await sink.send(_outbound(channel='chB', idempotency_key='ik->chB'))
        envelopes = await _drain(queue)
        assert {e.pipeline for e in envelopes} == {'pa', 'pb'}


@pytest.mark.parametrize('hops', [0, 5, 9])
async def test_hops_always_incremented_by_one(hops: int) -> None:
    sink, queue = _make_sink()
    await sink.send(_outbound(hops=hops))
    (envelope,) = await _drain(queue)
    assert envelope.record.hops == hops + 1
