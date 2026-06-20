"""
MemoryListener (§12.4, plan 2.6): программная инжекция событий.

Поведенчески — настоящий driving-адаптер: ``emit(raw)`` маппит сырое
событие в ``Record`` публичными хелперами ключей (§7.2) и
подаёт его в ``IngestService``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from angarion.adapters.memory.listener import MemoryListener
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryDeadLetters,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.domain.errors import NotSupportedError
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import Endpoint, RecordKind

CHAT = '-100123'


def make_ingest() -> tuple[IngestService, MemoryQueue, MemoryAnalytics]:
    """Настоящий IngestService на InMemory-адаптерах."""
    queue = MemoryQueue()
    analytics = MemoryAnalytics()
    router = Router(
        [
            RouteSpec(
                pipeline='digest',
                events=frozenset(RecordKind),
                sources=(Endpoint(transport='memory', address=CHAT),),
            )
        ]
    )
    ingest = IngestService(
        dedup=MemoryDedupStore(),
        registry=MemoryMessageRegistry(),
        router=router,
        queue=queue,
        analytics=analytics,
        dead_letters=MemoryDeadLetters(),
    )
    return ingest, queue, analytics


def make_pipeline_env() -> tuple[MemoryListener, MemoryQueue, MemoryAnalytics]:
    """Listener поверх настоящего IngestService."""
    ingest, queue, analytics = make_ingest()
    listener = MemoryListener(ingest=ingest, account_ids=('main', 'backup'))
    return listener, queue, analytics


class TestLifecycle:
    async def test_start_stop_toggle_started(self) -> None:
        listener, _, _ = make_pipeline_env()
        assert not listener.started
        await listener.start()
        assert listener.started
        await listener.stop()
        assert not listener.started

    async def test_catchup_is_not_supported(self) -> None:
        """history_fetch=False у платформы memory — честно (plan 2.6)."""
        listener, _, _ = make_pipeline_env()
        with pytest.raises(NotSupportedError):
            await listener.catchup('memory:main:-100123')

    async def test_requires_at_least_one_account(self) -> None:
        ingest, _, _ = make_ingest()
        with pytest.raises(ValueError, match='аккаунт'):
            MemoryListener(ingest=ingest, account_ids=())


class TestEmit:
    async def test_emit_new_message_builds_normalized_event(self) -> None:
        listener, queue, _ = make_pipeline_env()
        event = await listener.emit(
            {
                'address': CHAT,
                'external_id': '42',
                'text': 'hello\r\nworld',
                'sender_id': 'u1',
                'sender_name': 'Alice',
            }
        )
        source_key = make_source_key('memory', 'main', CHAT)
        content_hash = normalize_and_hash('hello\r\nworld')
        assert event.kind is RecordKind.NEW
        assert event.origin == 'live'
        assert event.dedup_key == make_dedup_key(
            RecordKind.NEW, source_key, '42', content_hash
        )
        assert event.content_hash == content_hash
        assert event.source == Endpoint(transport='memory', address=CHAT)
        assert event.received_by.transport == 'memory'
        assert event.received_by.account_id == 'main'
        assert event.sender_name == 'Alice'
        assert event.raw['address'] == CHAT
        assert event.received_at.tzinfo is not None
        item = await queue.get()
        assert item.envelope.record == event

    async def test_emit_edited_uses_content_hash_in_dedup_key(self) -> None:
        listener, _, _ = make_pipeline_env()
        event = await listener.emit(
            {
                'kind': 'edited',
                'address': CHAT,
                'external_id': '42',
                'text': 'v2',
            }
        )
        assert event.kind is RecordKind.EDITED
        assert event.dedup_key.endswith(f':edit:{normalize_and_hash("v2")}')

    async def test_emit_deleted_without_text(self) -> None:
        listener, _, _ = make_pipeline_env()
        event = await listener.emit(
            {'kind': 'deleted', 'address': CHAT, 'external_id': '42'}
        )
        assert event.kind is RecordKind.DELETED
        assert event.text is None
        assert event.content_hash is None
        assert event.dedup_key.endswith(':del')

    async def test_emit_honours_thread_account_reply_and_event_at(self) -> None:
        listener, _, _ = make_pipeline_env()
        at = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
        event = await listener.emit(
            {
                'account': 'backup',
                'address': CHAT,
                'thread_id': '7',
                'external_id': '43',
                'text': 'reply',
                'reply_to_external_id': '42',
                'event_at': at,
            }
        )
        assert event.received_by.account_id == 'backup'
        assert event.source.thread_id == '7'
        assert event.reply_to_external_id == '42'
        assert event.event_at == at
        assert event.dedup_key.startswith(
            make_source_key('memory', 'backup', CHAT, '7')
        )

    async def test_emit_with_unknown_account_fails(self) -> None:
        listener, _, _ = make_pipeline_env()
        with pytest.raises(ValueError, match='аккаунт'):
            await listener.emit(
                {'account': 'ghost', 'address': CHAT, 'external_id': '1', 'text': 'x'}
            )

    async def test_duplicate_emit_is_deduplicated_by_ingest(self) -> None:
        listener, queue, analytics = make_pipeline_env()
        raw = {'address': CHAT, 'external_id': '42', 'text': 'same'}
        await listener.emit(raw)
        await listener.emit(raw)
        depth = await queue.depth()
        assert depth.pending == 1
        duplicates = await analytics.recent(kind='duplicate')
        assert len(duplicates) == 1
