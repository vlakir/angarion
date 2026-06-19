"""TelegramListener: live-подписки + буфер → ingest (M3, фаза 2)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest
from telegram_fakes import (
    FakePool,
    FakeTelegramClient,
    RecordingIngest,
    raw_deletion,
    raw_message,
)

from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryDedupStore,
    MemoryMessageRegistry,
)
from angarion.adapters.telegram.client import RawMedia
from angarion.adapters.telegram.listener import TelegramListener
from angarion.application.ingest import IngestService
from angarion.application.router import Router, RouteSpec
from angarion.config import EndpointConfig, MediaConfig
from angarion.domain.models import Address, EventKind
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from angarion.adapters.telegram.client import TelegramClientPort


def _ep(account: str, chat_id: str) -> EndpointConfig:
    return EndpointConfig(account=account, chat_id=chat_id)


def _listener(
    clients: dict[str, FakeTelegramClient],
    sources: Sequence[EndpointConfig],
    ingest: RecordingIngest,
    media_policy: MediaConfig | None = None,
) -> TelegramListener:
    return TelegramListener(
        ingest=cast('IngestService', ingest),
        pool=FakePool(clients),
        sources=sources,
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
        media_policy=media_policy or MediaConfig(),
    )


def test_requires_at_least_one_client() -> None:
    with pytest.raises(ValueError, match='хотя бы один клиент'):
        _listener({}, [], RecordingIngest())


async def test_start_warms_and_marks_started() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    listener = _listener({'main': client}, [_ep('main', '@grp')], RecordingIngest())
    assert listener.started is False
    await listener.start()
    assert listener.started is True
    assert client.warmed == 1
    await listener.stop()
    assert listener.started is False


async def test_new_message_reaches_ingest() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message())
    await listener.stop()
    assert [e.external_id for e in ingest.events] == ['42']
    assert ingest.events[0].kind is EventKind.MESSAGE_NEW


async def test_live_media_downloaded_when_policy_on() -> None:
    """Включённая [media] качает вложение live-события до ingest (A3)."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123}, download_effects=['/blobs/-100123_42.bin']
    )
    ingest = RecordingIngest()
    listener = _listener(
        {'main': client},
        [_ep('main', '@grp')],
        ingest,
        media_policy=MediaConfig(download=True, storage_dir='/blobs'),
    )
    await listener.start()
    await client.fire_new(raw_message(media=(RawMedia(kind='photo'),)))
    await listener.stop()
    assert ingest.events[0].media[0].local_path == '/blobs/-100123_42.bin'
    assert client.downloads == [{'source_ref': '-100123:42', 'dest_dir': '/blobs'}]


async def test_live_media_not_downloaded_by_default() -> None:
    """Без opt-in'а медиа остаётся метаданными (только ref, без local_path)."""
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message(media=(RawMedia(kind='photo'),)))
    await listener.stop()
    assert ingest.events[0].media[0].local_path is None
    assert client.downloads == []


async def test_running_consumer_drains_live() -> None:
    """Работающий консьюмер обрабатывает live-событие до stop()."""
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message())
    for _ in range(10):
        if ingest.events:
            break
        await asyncio.sleep(0)
    assert [e.external_id for e in ingest.events] == ['42']
    await listener.stop()


async def test_unconfigured_chat_filtered() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message(chat_id=-100999))
    await listener.stop()
    assert ingest.events == []


async def test_edited_message_reaches_ingest() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_edit(raw_message(kind=EventKind.MESSAGE_EDITED, text='ред'))
    await listener.stop()
    assert [e.kind for e in ingest.events] == [EventKind.MESSAGE_EDITED]


async def test_service_message_dropped() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message(is_service=True))
    await listener.stop()
    assert ingest.events == []


async def test_deletion_reaches_ingest() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_delete(raw_deletion(message_ids=(7, 8)))
    await listener.stop()
    assert [e.external_id for e in ingest.events] == ['7', '8']
    assert {e.kind for e in ingest.events} == {EventKind.MESSAGE_DELETED}


async def test_deletion_unconfigured_chat_filtered() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_delete(raw_deletion(chat_id=-100999))
    await client.fire_delete(raw_deletion(chat_id=None))
    await listener.stop()
    assert ingest.events == []


async def test_unresolved_source_events_filtered() -> None:
    client = FakeTelegramClient(peer_ids={'@ok': -100123}, fail_peers=('@bad',))
    ingest = RecordingIngest()
    listener = _listener(
        {'main': client}, [_ep('main', '@ok'), _ep('main', '@bad')], ingest
    )
    await listener.start()
    await client.fire_new(raw_message(chat_id=-100123))  # @ok — пройдёт
    await client.fire_new(raw_message(chat_id=-100777))  # @bad не резолвился
    await listener.stop()
    assert [e.source.chat_id for e in ingest.events] == ['-100123']


async def test_multi_account_routing() -> None:
    a = FakeTelegramClient(peer_ids={'@a': -100111})
    b = FakeTelegramClient(peer_ids={'@b': -100222})
    ingest = RecordingIngest()
    listener = _listener(
        {'acc_a': a, 'acc_b': b}, [_ep('acc_a', '@a'), _ep('acc_b', '@b')], ingest
    )
    await listener.start()
    await a.fire_new(raw_message(chat_id=-100111, message_id=1))
    await b.fire_new(raw_message(chat_id=-100222, message_id=2))
    await listener.stop()
    received = {(e.received_by.account_id, e.external_id) for e in ingest.events}
    assert received == {('acc_a', '1'), ('acc_b', '2')}


async def test_catchup_unknown_source_raises() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    listener = _listener({'main': client}, [_ep('main', '@grp')], RecordingIngest())
    await listener.start()
    with pytest.raises(KeyError, match='не резолвлен'):
        await listener.catchup('telegram:main:-999999')
    await listener.stop()


async def test_start_runs_catchup_emitting_new() -> None:
    """start(): catch-up по истории эмитит NEW до live."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await listener.stop()
    assert [e.external_id for e in ingest.events] == ['5']
    assert ingest.events[0].origin == 'catchup'


async def test_live_during_catchup_buffered_then_drained() -> None:
    """Live, пришедший во время catch-up, обрабатывается после (буфер)."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await client.fire_new(raw_message(chat_id=-100123, message_id=7))
    await listener.stop()
    kinds = {(e.origin, e.external_id) for e in ingest.events}
    assert kinds == {('catchup', '5'), ('live', '7')}


def _recent_listener(
    client: FakeTelegramClient,
    ingest: RecordingIngest,
    *,
    recent_endpoints: frozenset[EndpointConfig],
    recent_interval: float,
) -> TelegramListener:
    return TelegramListener(
        ingest=cast('IngestService', ingest),
        pool=FakePool({'main': client}),
        sources=[_ep('main', '@grp')],
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
        recent_poll_endpoints=recent_endpoints,
        recent_interval=recent_interval,
        recent_window_messages=10,
        recent_window_minutes=60,
    )


async def test_recent_poll_timer_polls_window_for_enabled_source() -> None:
    """T032: таймер лёгкого поллинга фетчит recent-poll источник по окну."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    listener = _recent_listener(
        client,
        RecordingIngest(),
        recent_endpoints=frozenset({_ep('main', '@grp')}),
        recent_interval=0.01,
    )
    await listener.start()
    for _ in range(200):  # ждём первый проход таймера (фетч с window-лимитом)
        if any(limit == 10 for *_rest, limit in client.fetch_calls):
            break
        await asyncio.sleep(0.01)
    await listener.stop()
    window_fetches = [call for call in client.fetch_calls if call[3] == 10]
    assert window_fetches  # лёгкий поллинг отработал по узкому окну (limit=10)


async def test_recent_poll_timer_absent_without_enabled_sources() -> None:
    """T032: без recent-poll источников лёгкий таймер не поднимается."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    listener = _recent_listener(
        client,
        RecordingIngest(),
        recent_endpoints=frozenset(),  # ни один источник не включён
        recent_interval=0.01,
    )
    await listener.start()
    await asyncio.sleep(0.05)  # дать шанс таймеру (его не должно быть)
    await listener.stop()
    # все фетчи — только глубокий catch-up на старте (limit=2000), окна нет
    assert client.fetch_calls
    assert all(call[3] != 10 for call in client.fetch_calls)


async def test_catchup_disabled_skips_history() -> None:
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    ingest = RecordingIngest()
    listener = TelegramListener(
        ingest=cast('IngestService', ingest),
        pool=FakePool({'main': client}),
        sources=[_ep('main', '@grp')],
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
        catchup_enabled=False,
    )
    await listener.start()
    await listener.stop()
    assert ingest.events == []
    assert client.fetch_calls == []


async def test_manual_catchup_reruns_source() -> None:
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    ingest = RecordingIngest()
    listener = _listener({'main': client}, [_ep('main', '@grp')], ingest)
    await listener.start()
    await listener.catchup('telegram:main:-100123')
    await listener.stop()
    assert len(client.fetch_calls) == 2  # старт + ручной прогон


async def test_stop_before_start_is_noop() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    listener = _listener({'main': client}, [_ep('main', '@grp')], RecordingIngest())
    await listener.stop()
    assert listener.started is False


async def test_stop_is_idempotent() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    listener = _listener({'main': client}, [_ep('main', '@grp')], RecordingIngest())
    await listener.start()
    await listener.stop()
    await listener.stop()
    assert listener.started is False


async def test_pool_connected_on_start_and_disconnected_on_stop() -> None:
    """Listener ведёт жизненный цикл пула клиентов (§12.1)."""
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    pool = FakePool({'main': client})
    listener = TelegramListener(
        ingest=cast('IngestService', RecordingIngest()),
        pool=pool,
        sources=[_ep('main', '@grp')],
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
    )
    await listener.start()
    assert pool.connects == 1
    assert pool.disconnects == 0
    await listener.stop()
    assert pool.disconnects == 1


async def test_periodic_catchup_timer_reruns_sources() -> None:
    """catchup_interval поднимает фоновый таймер, добирающий историю (Q9/N1)."""
    client = FakeTelegramClient(
        peer_ids={'@grp': -100123},
        history={-100123: [raw_message(chat_id=-100123, message_id=5)]},
    )
    ingest = RecordingIngest()
    listener = TelegramListener(
        ingest=cast('IngestService', ingest),
        pool=FakePool({'main': client}),
        sources=[_ep('main', '@grp')],
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
        catchup_interval=0.01,
    )
    await listener.start()
    for _ in range(200):
        if len(client.fetch_calls) >= 2:
            break
        await asyncio.sleep(0.01)
    await listener.stop()
    assert len(client.fetch_calls) >= 2  # старт + хотя бы один фоновый прогон


async def test_real_ingest_end_to_end_routes() -> None:
    """Сквозь настоящий IngestService событие доходит до очереди."""
    queue = MemoryQueue()
    ingest = IngestService(
        dedup=MemoryDedupStore(),
        registry=MemoryMessageRegistry(),
        router=Router(
            [
                RouteSpec(
                    pipeline='p',
                    events=frozenset({EventKind.MESSAGE_NEW}),
                    sources=(Address(messenger='telegram', chat_id='-100123'),),
                )
            ]
        ),
        queue=queue,
        analytics=MemoryAnalytics(),
    )
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    listener = TelegramListener(
        ingest=ingest,
        pool=FakePool({'main': client}),
        sources=[_ep('main', '@grp')],
        registry=MemoryMessageRegistry(),
        cursors=MemoryCursorStore(),
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
    )
    await listener.start()
    await client.fire_new(raw_message())
    await listener.stop()
    depth = await queue.depth()
    assert depth.pending == 1
