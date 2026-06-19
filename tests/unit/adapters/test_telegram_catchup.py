"""
Алгоритм catch-up §9.3 на синтетической истории (M3, фаза 3).

Сценарии §13.2: новые за простой, правка по расхождению хэша, удаление,
обрезка по лимиту/возрасту (``catchup_truncated``), дедуп при повторном
catch-up, сдвиг курсора, источник-топик (удаления не детектируются).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from telegram_fakes import FakeTelegramClient, RecordingIngest, raw_message

from angarion.adapters.telegram.client import RawMedia

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.adapters.telegram.catchup import (
    LAST_SCAN_KEY,
    LAST_SEEN_KEY,
    run_catchup,
)
from angarion.config import MediaConfig
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.domain.keys import make_media_hash, normalize_and_hash
from angarion.domain.models import (
    Address,
    EventKind,
    MediaRef,
    RegistryRecord,
    SourceCursor,
)
from angarion.log import get_logger

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)
CHAT = -100123
SOURCE_KEY = f'telegram:main:{CHAT}'


def _hist(message_id: int, text: str | None = 'текст', **over: object) -> object:
    return raw_message(
        chat_id=CHAT,
        message_id=message_id,
        text=text,
        event_at=over.pop('event_at', NOW),
        **over,
    )


async def _seed(
    registry: MemoryMessageRegistry, external_id: int, text: str | None
) -> None:
    await registry.upsert(
        RegistryRecord(
            source_key=SOURCE_KEY,
            external_id=str(external_id),
            text=text,
            content_hash=normalize_and_hash(text) if text is not None else None,
            event_at=NOW,
        )
    )


async def _run(
    *,
    history: list[object],
    registry: MemoryMessageRegistry | None = None,
    cursor: SourceCursor | None = None,
    ingest: RecordingIngest | None = None,
    analytics: MemoryAnalytics | None = None,
    cursors: MemoryCursorStore | None = None,
    max_messages: int = 2000,
    max_age_days: int = 7,
    record_truncation: bool = True,
    thread_id: str | None = None,
    media_policy: MediaConfig | None = None,
    client: FakeTelegramClient | None = None,
) -> tuple[RecordingIngest, MemoryCursorStore, MemoryAnalytics, FakeTelegramClient]:
    registry = registry or MemoryMessageRegistry()
    ingest = ingest or RecordingIngest()
    analytics = analytics or MemoryAnalytics()
    cursors = cursors or MemoryCursorStore()
    if cursor is not None:
        await cursors.save(cursor)
    client = client or FakeTelegramClient(history={CHAT: history})  # type: ignore[arg-type]
    await run_catchup(
        client=client,
        account_id='main',
        chat_id=CHAT,
        thread_id=thread_id,
        registry=registry,
        cursors=cursors,
        ingest=ingest,  # type: ignore[arg-type]
        analytics=analytics,
        log=get_logger('test'),
        media_policy=media_policy or MediaConfig(),
        max_messages=max_messages,
        max_age=timedelta(days=max_age_days),
        now=NOW,
        record_truncation=record_truncation,
    )
    return ingest, cursors, analytics, client


def _cursor(last_seen: int) -> SourceCursor:
    return SourceCursor(
        source_key=SOURCE_KEY,
        payload={LAST_SEEN_KEY: str(last_seen)},
        updated_at=NOW,
    )


async def test_new_messages_above_cursor_emitted() -> None:
    ingest, *_ = await _run(
        history=[_hist(9), _hist(11), _hist(12)],
        cursor=_cursor(10),
    )
    assert [(e.kind, e.external_id) for e in ingest.events] == [
        (EventKind.MESSAGE_NEW, '11'),
        (EventKind.MESSAGE_NEW, '12'),
    ]
    assert all(e.origin == 'catchup' for e in ingest.events)


async def test_old_unknown_below_cursor_not_reemitted() -> None:
    """Сообщение ниже курсора и неизвестное реестру — уже обработано."""
    registry = MemoryMessageRegistry()
    await _seed(registry, 5, 'старое')  # известно реестру (в окне)
    ingest, *_ = await _run(
        history=[_hist(5, 'старое'), _hist(6)],  # 6 — ниже курсора, неизвестно
        registry=registry,
        cursor=_cursor(10),
    )
    assert ingest.events == []  # 5 unchanged, 6 не выше курсора → ничего


async def test_edit_detected_by_hash_divergence() -> None:
    registry = MemoryMessageRegistry()
    await _seed(registry, 5, 'было')
    ingest, *_ = await _run(
        history=[_hist(5, 'стало')],
        registry=registry,
        cursor=_cursor(5),
    )
    assert [(e.kind, e.external_id) for e in ingest.events] == [
        (EventKind.MESSAGE_EDITED, '5')
    ]
    assert ingest.events[0].origin == 'catchup'


async def test_media_edit_detected_during_catchup() -> None:
    """M7 A3: подмена вложения за простой при том же тексте → EDITED по media_hash."""
    registry = MemoryMessageRegistry()
    await registry.upsert(
        RegistryRecord(
            source_key=SOURCE_KEY,
            external_id='5',
            text='подпись',
            content_hash=normalize_and_hash('подпись'),
            media_hash=make_media_hash([MediaRef(kind='photo', size=10)]),
            event_at=NOW,
        )
    )
    ingest, *_ = await _run(
        history=[_hist(5, 'подпись', media=(RawMedia(kind='photo', size=20),))],
        registry=registry,
        cursor=_cursor(5),
    )
    assert [(e.kind, e.external_id) for e in ingest.events] == [
        (EventKind.MESSAGE_EDITED, '5')
    ]


async def test_same_media_not_reemitted_during_catchup() -> None:
    """Те же текст и медиа за простой — не ложная правка."""
    registry = MemoryMessageRegistry()
    await registry.upsert(
        RegistryRecord(
            source_key=SOURCE_KEY,
            external_id='5',
            text='подпись',
            content_hash=normalize_and_hash('подпись'),
            media_hash=make_media_hash([MediaRef(kind='photo', size=10)]),
            event_at=NOW,
        )
    )
    ingest, *_ = await _run(
        history=[_hist(5, 'подпись', media=(RawMedia(kind='photo', size=10),))],
        registry=registry,
        cursor=_cursor(5),
    )
    assert ingest.events == []


async def test_catchup_media_downloaded_when_policy_on() -> None:
    """M7 A3: catch-up качает медиа нового сообщения при включённой политике."""
    client = FakeTelegramClient(
        history={CHAT: [_hist(11, 'подпись', media=(RawMedia(kind='photo'),))]},
        download_effects=['/blobs/-100123_11.bin'],
    )
    ingest, *_ = await _run(
        history=[],
        client=client,
        cursor=_cursor(10),
        media_policy=MediaConfig(download=True, storage_dir='/blobs'),
    )
    assert ingest.events[0].media[0].local_path == '/blobs/-100123_11.bin'
    assert client.downloads == [{'source_ref': '-100123:11', 'dest_dir': '/blobs'}]


async def test_unchanged_message_skipped() -> None:
    registry = MemoryMessageRegistry()
    await _seed(registry, 5, 'текст')
    ingest, *_ = await _run(
        history=[_hist(5, 'текст')],
        registry=registry,
        cursor=_cursor(5),
    )
    assert ingest.events == []


async def test_deletion_within_covered_range() -> None:
    registry = MemoryMessageRegistry()
    for i in (5, 6, 7):
        await _seed(registry, i, 'текст')
    ingest, *_ = await _run(
        history=[_hist(5, 'текст'), _hist(7, 'текст')],  # 6 удалён
        registry=registry,
        cursor=_cursor(7),
    )
    assert [(e.kind, e.external_id) for e in ingest.events] == [
        (EventKind.MESSAGE_DELETED, '6')
    ]


async def test_truncation_by_max_messages_suppresses_low_deletions() -> None:
    registry = MemoryMessageRegistry()
    for i in range(1, 11):  # реестр знает 1..10
        await _seed(registry, i, 'текст')
    # реально существуют только 8,9,10 (1..7 удалены за простой)
    ingest, _cur, analytics, _client = await _run(
        history=[_hist(8, 'текст'), _hist(9, 'текст'), _hist(10, 'текст')],
        registry=registry,
        cursor=_cursor(10),
        max_messages=3,
    )
    # покрытый диапазон — только [8..], удаления 1..7 НЕ эмитируются (§9.3.4)
    assert ingest.events == []
    truncated = await analytics.recent(kind='catchup_truncated')
    assert len(truncated) == 1
    assert truncated[0].payload['source_key'] == SOURCE_KEY


async def test_recent_window_poll_detects_edit_and_suppresses_truncation() -> None:
    """T032: лёгкий проход по узкому окну ловит правку, но не шумит truncation."""
    registry = MemoryMessageRegistry()
    for i in (8, 9, 10):
        await _seed(registry, i, 'текст')
    ingest, _cur, analytics, _client = await _run(
        history=[_hist(8, 'текст'), _hist(9, 'правка'), _hist(10, 'текст')],
        registry=registry,
        cursor=_cursor(10),
        max_messages=3,  # узкое окно → fetch усечён
        record_truncation=False,  # лёгкий режим (T032)
    )
    # правка в окне поймана сверкой по реестру
    assert [(e.kind, e.external_id) for e in ingest.events] == [
        (EventKind.MESSAGE_EDITED, '9')
    ]
    # узкое окно «усечено» by design — catchup_truncated подавлен
    assert await analytics.recent(kind='catchup_truncated') == []


async def test_truncation_by_age_stops_fetch() -> None:
    old = NOW - timedelta(days=10)
    ingest, _cur, analytics, _client = await _run(
        history=[
            _hist(3, 'старое', event_at=old),
            _hist(4),
            _hist(5),
        ],
        cursor=_cursor(2),
        max_age_days=7,
    )
    # 4,5 — свежие NEW; 3 — за окном возраста, фетч прерван
    assert [e.external_id for e in ingest.events] == ['4', '5']
    assert len(await analytics.recent(kind='catchup_truncated')) == 1


async def test_cursor_advances_to_newest_fetched() -> None:
    _ingest, cursors, *_ = await _run(
        history=[_hist(11), _hist(12), _hist(13)],
        cursor=_cursor(10),
    )
    saved = await cursors.load(SOURCE_KEY)
    assert saved is not None
    assert saved.payload[LAST_SEEN_KEY] == '13'
    assert LAST_SCAN_KEY in saved.payload


async def test_empty_history_keeps_cursor_and_scans() -> None:
    _ingest, cursors, *_ = await _run(history=[], cursor=_cursor(10))
    saved = await cursors.load(SOURCE_KEY)
    assert saved is not None
    assert saved.payload[LAST_SEEN_KEY] == '10'  # не откатился назад
    assert LAST_SCAN_KEY in saved.payload


async def test_service_message_not_emitted() -> None:
    ingest, *_ = await _run(
        history=[_hist(11, is_service=True), _hist(12)],
        cursor=_cursor(10),
    )
    assert [e.external_id for e in ingest.events] == ['12']


async def test_thread_source_skips_deletion_detection() -> None:
    """Источник-топик: удаления live тоже не детектируются (§9.4)."""
    registry = MemoryMessageRegistry()
    thread_key = f'{SOURCE_KEY}:55'
    await registry.upsert(
        RegistryRecord(
            source_key=thread_key,
            external_id='6',
            text='текст',
            content_hash=normalize_and_hash('текст'),
            event_at=NOW,
        )
    )
    ingest, *_ = await _run(
        history=[_hist(5, thread_id=55)],  # 6 отсутствует, но топик → не удаляем
        registry=registry,
        cursor=_cursor(7),
        thread_id='55',
    )
    assert all(e.kind is not EventKind.MESSAGE_DELETED for e in ingest.events)


async def test_fetch_passes_thread_filter() -> None:
    _ingest, _cur, _an, client = await _run(
        history=[_hist(5, thread_id=55)],
        cursor=_cursor(0),
        thread_id='55',
    )
    assert client.fetch_calls[0][1] == 55  # thread_id передан в fetch_history


async def test_redelivery_deduped_end_to_end() -> None:
    """Повторный catch-up: дедуп гасит уже виденные события (§9.3.5)."""
    dedup = MemoryDedupStore()
    registry = MemoryMessageRegistry()
    queue = MemoryQueue()
    analytics = MemoryAnalytics()
    cursors = MemoryCursorStore()
    ingest = IngestService(
        dedup=dedup,
        registry=registry,
        router=Router(
            [
                RouteSpec(
                    pipeline='p',
                    events=frozenset(EventKind),
                    sources=(Address(messenger='telegram', chat_id=str(CHAT)),),
                )
            ]
        ),
        queue=queue,
        analytics=analytics,
    )
    client = FakeTelegramClient(history={CHAT: [_hist(11), _hist(12)]})  # type: ignore[dict-item]
    kwargs = {
        'client': client,
        'account_id': 'main',
        'chat_id': CHAT,
        'thread_id': None,
        'registry': registry,
        'cursors': cursors,
        'ingest': ingest,
        'analytics': analytics,
        'log': get_logger('test'),
        'media_policy': MediaConfig(),
        'max_messages': 2000,
        'max_age': timedelta(days=7),
        'now': NOW,
    }
    await run_catchup(**kwargs)  # type: ignore[arg-type]
    first = (await queue.depth()).pending
    await run_catchup(**kwargs)  # type: ignore[arg-type]
    second = (await queue.depth()).pending
    assert first == 2
    assert second == 2  # второй прогон ничего не добавил (дедуп)
