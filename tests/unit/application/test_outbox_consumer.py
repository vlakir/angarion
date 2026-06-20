"""
OutboxConsumer (§12.9, FR-5; T024, фаза 4): диспетчеризация
notify/catchup/restart_pipeline, пометка done/failed, аналитика сбоя.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCommandOutbox,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.application.ingest import IngestService
from angarion.application.manual import ManualEvent, build_manual_record
from angarion.application.outbox_consumer import (
    COMMAND_FAILED,
    NOTIFY_FAILED,
    OutboxConsumer,
)
from angarion.application.router import Router, RouteSpec
from angarion.domain.models import (
    CommandKind,
    CommandStatus,
    DeliveryReceipt,
    OutboundRecord,
    Record,
    RecordKind,
)
from angarion.testing.factories import make_endpoint, make_outbound, make_record

pytestmark = pytest.mark.asyncio


class FakeSink:
    """``SinkPort`` для тестов: запоминает отправки или падает."""

    def __init__(self, *, fail: bool = False) -> None:
        self.sent: list[OutboundRecord] = []
        self._fail = fail

    async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
        if self._fail:
            msg_text = 'sink boom'
            raise RuntimeError(msg_text)
        self.sent.append(msg)
        return DeliveryReceipt(external_id='ext-1', delivered_at=datetime.now(UTC))


class FakeIngest:
    """``IngestService`` для тестов: собирает записи или падает."""

    def __init__(self, *, fail: bool = False) -> None:
        self.ingested: list[Record] = []
        self._fail = fail

    async def ingest(self, record: Record) -> None:
        if self._fail:
            msg = 'ingest boom'
            raise RuntimeError(msg)
        self.ingested.append(record)


def _make_consumer(
    outbox: MemoryCommandOutbox,
    analytics: MemoryAnalytics,
    *,
    sink: FakeSink | None = None,
    catchups: list[str] | None = None,
    catchup_fail: bool = False,
    restarts: list[bool] | None = None,
    ingest: FakeIngest | None = None,
) -> OutboxConsumer:
    async def catchup(source_key: str) -> None:
        if catchup_fail:
            raise KeyError(source_key)
        if catchups is not None:
            catchups.append(source_key)

    def request_restart() -> None:
        if restarts is not None:
            restarts.append(True)

    return OutboxConsumer(
        command_outbox=outbox,
        sink=sink or FakeSink(),
        analytics=analytics,
        catchup=catchup,
        request_restart=request_restart,
        ingest=(ingest or FakeIngest()).ingest,
        poll_seconds=0.0,
    )


async def test_notify_dispatch_sends_and_marks_done() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    sink = FakeSink()
    message = make_outbound()
    command = await outbox.put(
        CommandKind.NOTIFY, payload={'record': message.model_dump(mode='json')}
    )
    consumer = _make_consumer(outbox, analytics, sink=sink)
    processed = await consumer.poll_once()
    assert processed == 1
    assert sink.sent[0].idempotency_key == message.idempotency_key
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.DONE
    assert stored.result == 'sent:ext-1'


async def test_notify_failure_marks_failed_and_records_notify_failed() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await outbox.put(
        CommandKind.NOTIFY,
        payload={'record': make_outbound().model_dump(mode='json')},
    )
    consumer = _make_consumer(outbox, analytics, sink=FakeSink(fail=True))
    await consumer.poll_once()
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.FAILED
    assert 'sink boom' in (stored.error or '')
    failures = await analytics.recent(kind=NOTIFY_FAILED)
    assert failures
    assert failures[0].payload['command_uid'] == str(command.uid)


async def test_catchup_dispatch_invokes_callback() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    catchups: list[str] = []
    await outbox.put(CommandKind.CATCHUP, payload={'source_key': 'memory:a:-1'})
    consumer = _make_consumer(outbox, analytics, catchups=catchups)
    await consumer.poll_once()
    assert catchups == ['memory:a:-1']


async def test_catchup_failure_records_command_failed() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await outbox.put(
        CommandKind.CATCHUP, payload={'source_key': 'memory:a:-1'}
    )
    consumer = _make_consumer(outbox, analytics, catchup_fail=True)
    await consumer.poll_once()
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.FAILED
    assert (await analytics.recent(kind=COMMAND_FAILED))[0].payload[
        'command_kind'
    ] == 'catchup'


async def test_restart_dispatch_requests_restart_and_marks_done() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    restarts: list[bool] = []
    command = await outbox.put(CommandKind.RESTART_PIPELINE)
    consumer = _make_consumer(outbox, analytics, restarts=restarts)
    await consumer.poll_once()
    assert restarts == [True]
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.DONE


async def test_inject_dispatch_calls_ingest_and_marks_done() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    ingest = FakeIngest()
    record = make_record(origin='manual')
    command = await outbox.put(
        CommandKind.INJECT, payload={'record': record.model_dump(mode='json')}
    )
    consumer = _make_consumer(outbox, analytics, ingest=ingest)
    await consumer.poll_once()
    # Record восстановлен из JSON-payload без потерь (ключи, origin)
    assert [r.uid for r in ingest.ingested] == [record.uid]
    assert ingest.ingested[0].dedup_key == record.dedup_key
    assert ingest.ingested[0].origin == 'manual'
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.DONE
    assert stored.result == f'injected:{record.uid}'


async def test_inject_failure_marks_failed_and_records_command_failed() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    command = await outbox.put(
        CommandKind.INJECT,
        payload={'record': make_record().model_dump(mode='json')},
    )
    consumer = _make_consumer(outbox, analytics, ingest=FakeIngest(fail=True))
    await consumer.poll_once()
    stored = await outbox.get(command.uid)
    assert stored is not None
    assert stored.status is CommandStatus.FAILED
    assert 'ingest boom' in (stored.error or '')
    failures = await analytics.recent(kind=COMMAND_FAILED)
    assert failures[0].payload['command_kind'] == 'inject'


async def test_inject_idempotency_key_dedups_second_command_at_seam() -> None:
    # Сквозной стык split-моста: producer-payload → consumer → реальный
    # IngestService. Два впрыска одного ManualEvent с клиентским ключом
    # дают одинаковый dedup_key → второй гасится dedup.seen() (FR §3, A2).
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    queue, dedup = MemoryQueue(), MemoryDedupStore()
    router = Router(
        [
            RouteSpec(
                pipeline='digest',
                events=frozenset(RecordKind),
                sources=(make_endpoint(),),
            )
        ]
    )
    ingest = IngestService(
        dedup=dedup,
        registry=MemoryMessageRegistry(),
        router=router,
        queue=queue,
        analytics=analytics,
        dead_letters=MemoryDeadLetters(),
    )
    event = ManualEvent(
        source=make_endpoint(), text='manual hi', idempotency_key='ext-key-1'
    )
    first, second = build_manual_record(event), build_manual_record(event)
    assert first.dedup_key == second.dedup_key  # детерминизм по клиентскому ключу
    for record in (first, second):
        await outbox.put(
            CommandKind.INJECT, payload={'record': record.model_dump(mode='json')}
        )
    consumer = OutboxConsumer(
        command_outbox=outbox,
        sink=FakeSink(),
        analytics=analytics,
        catchup=_noop_catchup,
        request_restart=lambda: None,
        ingest=ingest.ingest,
        poll_seconds=0.0,
    )
    await consumer.poll_once()
    # один envelope стейджнут (первый), второй — duplicate
    assert (await queue.depth()).pending == 1


async def _noop_catchup(source_key: str) -> None:
    """Заглушка catch-up для сборки consumer'а в стыковом тесте."""


async def test_poll_once_empty_returns_zero() -> None:
    outbox, analytics = MemoryCommandOutbox(), MemoryAnalytics()
    consumer = _make_consumer(outbox, analytics)
    assert await consumer.poll_once() == 0
