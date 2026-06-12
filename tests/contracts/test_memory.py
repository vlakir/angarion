"""
Контрактные наборы портов на InMemory-реализациях (§12.4; SC-5)
плюс частности самих InMemory-адаптеров.
"""

from __future__ import annotations

import pytest
from angarion.testing import (
    AnalyticsContract,
    CursorStoreContract,
    DeadLetterContract,
    DedupStoreContract,
    EventQueueContract,
    MessageRegistryContract,
    MessageSinkContract,
    OutboxContract,
    StateStoreContract,
    make_outbound,
)

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.sink import MemorySink
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
    MemoryOutbox,
    MemoryStateStore,
)
from angarion.domain import ports


class TestMemoryQueue(EventQueueContract):
    @pytest.fixture
    def queue(self) -> MemoryQueue:
        return MemoryQueue()


class TestMemoryDedupStore(DedupStoreContract):
    @pytest.fixture
    def dedup(self) -> MemoryDedupStore:
        return MemoryDedupStore()


class TestMemoryOutbox(OutboxContract):
    @pytest.fixture
    def outbox(self) -> MemoryOutbox:
        return MemoryOutbox()


class TestMemoryMessageRegistry(MessageRegistryContract):
    @pytest.fixture
    def registry(self) -> MemoryMessageRegistry:
        return MemoryMessageRegistry()


class TestMemoryCursorStore(CursorStoreContract):
    @pytest.fixture
    def cursors(self) -> MemoryCursorStore:
        return MemoryCursorStore()


class TestMemoryStateStore(StateStoreContract):
    @pytest.fixture
    def state(self) -> MemoryStateStore:
        return MemoryStateStore()


class TestMemoryAnalytics(AnalyticsContract):
    @pytest.fixture
    def analytics(self) -> MemoryAnalytics:
        return MemoryAnalytics()


class TestMemoryDeadLetters(DeadLetterContract):
    @pytest.fixture
    def dead_letters(self) -> MemoryDeadLetters:
        return MemoryDeadLetters()


class TestMemorySink(MessageSinkContract):
    @pytest.fixture
    def sink(self) -> MemorySink:
        return MemorySink()

    async def test_journal_keeps_sent_messages_in_order(self) -> None:
        sink = MemorySink()
        first = make_outbound()
        second = make_outbound(text='second')
        await sink.send(first)
        await sink.send(second)
        assert sink.sent == [first, second]

    async def test_receipts_have_unique_external_ids(self) -> None:
        sink = MemorySink()
        first = await sink.send(make_outbound())
        second = await sink.send(make_outbound())
        assert first.external_id != second.external_id


PORT_CONFORMANCE = [
    (MemoryQueue, ports.EventQueuePort),
    (MemorySink, ports.MessageSinkPort),
    (MemoryDedupStore, ports.DedupStorePort),
    (MemoryOutbox, ports.OutboxPort),
    (MemoryMessageRegistry, ports.MessageRegistryPort),
    (MemoryCursorStore, ports.CursorStorePort),
    (MemoryStateStore, ports.StateStorePort),
    (MemoryAnalytics, ports.AnalyticsPort),
    (MemoryDeadLetters, ports.DeadLetterPort),
]


@pytest.mark.parametrize(
    ('impl', 'port'), PORT_CONFORMANCE, ids=lambda p: p.__name__
)
def test_memory_adapters_satisfy_their_ports(
    impl: type, port: type
) -> None:
    assert isinstance(impl(), port)
