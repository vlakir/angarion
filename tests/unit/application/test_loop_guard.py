"""
LoopGuardSink (T009 / M6, фаза 1): декоратор ``SinkPort``, гасящий
петлю ``source == target``.

После успешной доставки в цель, совпадающую с прослушиваемым источником
``(transport, address, thread_id)``, guard помечает в ``DedupStorePort``
``dedup_key`` будущего входящего сообщения (NEW и DEL) по произведённому
``external_id`` — так возврат собственной доставки (live или catch-up)
отбрасывается ``IngestService`` как ``duplicate``. Совпадения нет —
декоратор прозрачен (no-op). Механизм transport-agnostic, переиспользует
персистентный dedup (без новых портов/таблиц).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from angarion.adapters.memory.sink import MemorySink
from angarion.adapters.memory.storage import MemoryDedupStore
from angarion.application.loop_guard import GuardedSource, LoopGuardSink
from angarion.domain.keys import make_dedup_key, make_source_key
from angarion.domain.models import (
    AccountRef,
    DeliveryReceipt,
    Endpoint,
    OutboundRecord,
    RecordKind,
)
from angarion.domain.ports import SinkPort

if TYPE_CHECKING:
    from collections.abc import Sequence

TRANSPORT = 'telegram'


class _SpyDedup(MemoryDedupStore):
    """MemoryDedupStore + журнал ключей, переданных в mark_inbound."""

    def __init__(self) -> None:
        super().__init__()
        self.marked: list[str] = []

    async def mark_inbound(self, dedup_key: str) -> bool:
        self.marked.append(dedup_key)
        return await super().mark_inbound(dedup_key)


def _outbound(address: str, thread_id: str | None = None) -> OutboundRecord:
    return OutboundRecord(
        idempotency_key=f'k:{address}:{thread_id}',
        target=Endpoint(transport=TRANSPORT, address=address, thread_id=thread_id),
        send_via=AccountRef(transport=TRANSPORT, account_id='main'),
        text='hello',
    )


def _source(
    address: str, account_id: str = 'main', thread_id: str | None = None
) -> GuardedSource:
    return GuardedSource(
        transport=TRANSPORT,
        account_id=account_id,
        address=address,
        thread_id=thread_id,
    )


def _guard(
    sources: Sequence[GuardedSource],
    *,
    inner: MemorySink | None = None,
    dedup: _SpyDedup | None = None,
) -> tuple[LoopGuardSink, MemorySink, _SpyDedup]:
    sink = inner if inner is not None else MemorySink()
    store = dedup if dedup is not None else _SpyDedup()
    return LoopGuardSink(inner=sink, dedup=store, sources=sources), sink, store


async def test_target_matches_source_marks_new_and_del() -> None:
    """Цель = источник: помечаются NEW и DEL dedup_key произведённого id."""
    guard, sink, dedup = _guard([_source('-100123')])
    receipt = await guard.send(_outbound('-100123'))

    assert sink.sent  # доставка делегирована inner
    external_id = receipt.external_id
    assert external_id is not None
    source_key = make_source_key(TRANSPORT, 'main', '-100123')
    assert await dedup.seen(make_dedup_key(RecordKind.NEW, source_key, external_id))
    assert await dedup.seen(make_dedup_key(RecordKind.DELETED, source_key, external_id))


async def test_target_not_a_source_is_noop() -> None:
    """Цель не среди источников: dedup не трогается, receipt сквозной."""
    guard, sink, dedup = _guard([_source('-100123')])
    receipt = await guard.send(_outbound('-100999'))

    assert sink.sent
    assert receipt.external_id is not None
    assert dedup.marked == []


async def test_no_external_id_skips_marking() -> None:
    """Платформа не сообщила id доставки — пометить нечего, тихо пропускаем."""

    class _NoIdSink(MemorySink):
        async def send(self, msg: OutboundRecord) -> DeliveryReceipt:
            await super().send(msg)
            return DeliveryReceipt(external_id=None, delivered_at=datetime.now(UTC))

    guard, _sink, dedup = _guard([_source('-100123')], inner=_NoIdSink())
    receipt = await guard.send(_outbound('-100123'))

    assert receipt.external_id is None
    assert dedup.marked == []


async def test_thread_id_discriminates_match() -> None:
    """Источник в другом топике той же группы — не совпадение, no-op."""
    guard, _sink, dedup = _guard([_source('-100123', thread_id='7')])
    receipt = await guard.send(_outbound('-100123', thread_id=None))

    assert receipt.external_id is not None
    assert dedup.marked == []


async def test_multiple_accounts_same_chat_mark_each() -> None:
    """Два источника (один чат, разные аккаунты) — помечаем ключ каждого."""
    guard, _sink, dedup = _guard([_source('-100123', 'a'), _source('-100123', 'b')])
    receipt = await guard.send(_outbound('-100123'))

    assert receipt.external_id is not None
    for account_id in ('a', 'b'):
        source_key = make_source_key(TRANSPORT, account_id, '-100123')
        assert await dedup.seen(
            make_dedup_key(RecordKind.NEW, source_key, receipt.external_id)
        )


@pytest.mark.parametrize('sources', [[], [_source('-100777')]])
def test_guard_is_message_sink_port(sources: Sequence[GuardedSource]) -> None:
    """LoopGuardSink удовлетворяет SinkPort (structural)."""
    guard, _sink, _dedup = _guard(sources)
    assert isinstance(guard, SinkPort)
