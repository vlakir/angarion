"""
MatrixListener: sync-loop подписки + next_batch-курсор + UTD → ingest
(M7 B2, T010). Fake-клиент без nio: ручной fire событий.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

import pytest
from matrix_fakes import (
    ROOM,
    FakeMatrixClient,
    RecordingIngest,
    raw_deletion,
    raw_message,
    raw_undecryptable,
)

from angarion.adapters.matrix.client import MESSENGER, MatrixHistoryPage, RawMatrixMedia
from angarion.adapters.matrix.listener import (
    SYNC_CURSOR_CHAT,
    MatrixListener,
)
from angarion.adapters.memory.storage import MemoryAnalytics, MemoryCursorStore
from angarion.config import EndpointConfig, MediaConfig
from angarion.domain.keys import make_source_key
from angarion.domain.models import EventKind, SourceCursor
from angarion.log import get_logger

if TYPE_CHECKING:
    from angarion.application.ingest import IngestService
    from angarion.domain.ports import CursorStorePort

OTHER_ROOM = '!other:matrix.example'


def _ep(account: str, chat_id: str, thread_id: str | None = None) -> EndpointConfig:
    return EndpointConfig(account=account, chat_id=chat_id, thread_id=thread_id)


def _make(
    *,
    client: FakeMatrixClient,
    ingest: RecordingIngest,
    cursors: CursorStorePort,
    sources: list[EndpointConfig] | None = None,
    analytics: MemoryAnalytics | None = None,
    catchup_enabled: bool = False,
    media_policy: MediaConfig | None = None,
) -> MatrixListener:
    return MatrixListener(
        ingest=cast('IngestService', ingest),
        clients={'main': client},
        sources=sources if sources is not None else [_ep('main', ROOM)],
        cursors=cursors,
        analytics=analytics or MemoryAnalytics(),
        log=get_logger('test.matrix.listener'),
        catchup_enabled=catchup_enabled,
        media_policy=media_policy or MediaConfig(),
        catchup_max_age_days=36500,  # без отсечки по возрасту в тестах
    )


class TestLifecycle:
    async def test_start_restores_subscribes_and_syncs(self) -> None:
        client = FakeMatrixClient()
        listener = _make(
            client=client, ingest=RecordingIngest(), cursors=MemoryCursorStore()
        )
        await listener.start()
        assert client.restored == 1
        assert listener.started is True
        assert client.synced_since == [None]  # первый запуск — без курсора
        await listener.stop()
        assert client.stopped == 1
        assert listener.started is False

    async def test_start_resumes_from_cursor(self) -> None:
        cursors = MemoryCursorStore()
        sync_key = make_source_key(MESSENGER, 'main', SYNC_CURSOR_CHAT)
        await cursors.save(
            SourceCursor(
                source_key=sync_key,
                payload={'next_batch': 's-99'},
                updated_at=raw_message().event_at,
            )
        )
        client = FakeMatrixClient()
        listener = _make(client=client, ingest=RecordingIngest(), cursors=cursors)
        await listener.start()
        assert client.synced_since == ['s-99']
        await listener.stop()

    async def test_stop_is_idempotent_before_start(self) -> None:
        listener = _make(
            client=FakeMatrixClient(),
            ingest=RecordingIngest(),
            cursors=MemoryCursorStore(),
        )
        await listener.stop()  # не падает
        assert listener.started is False

    async def test_requires_at_least_one_client(self) -> None:
        with pytest.raises(ValueError, match='хотя бы один клиент'):
            MatrixListener(
                ingest=cast('IngestService', RecordingIngest()),
                clients={},
                sources=[],
                cursors=MemoryCursorStore(),
                analytics=MemoryAnalytics(),
                log=get_logger('test.matrix.listener'),
            )

    async def test_resolves_only_own_account_sources(self) -> None:
        """Источник чужого аккаунта в резолв этого клиента не попадает."""
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(
            client=client,
            ingest=ingest,
            cursors=MemoryCursorStore(),
            sources=[_ep('main', ROOM), _ep('other', OTHER_ROOM)],
        )
        await listener.start()
        await client.fire_new(raw_message(room_id=OTHER_ROOM))
        assert ingest.events == []  # OTHER_ROOM принадлежит чужому аккаунту
        await client.fire_new(raw_message())
        assert len(ingest.events) == 1
        await listener.stop()

    async def test_catchup_unknown_source_raises(self) -> None:
        client = FakeMatrixClient()
        listener = _make(
            client=client, ingest=RecordingIngest(), cursors=MemoryCursorStore()
        )
        await listener.start()
        with pytest.raises(KeyError, match='не резолвлен'):
            await listener.catchup(make_source_key(MESSENGER, 'main', '!nope:s'))
        await listener.stop()


class TestCatchup:
    def _page(self) -> MatrixHistoryPage:
        return MatrixHistoryPage(
            messages=(
                raw_message(event_id='$e1'),
                raw_message(kind=EventKind.MESSAGE_EDITED, event_id='$e2', text='пр'),
            ),
            redactions=(raw_deletion(redacts_event_id='$gone'),),
        )

    async def test_catchup_emits_history_new_edited_deleted(self) -> None:
        client = FakeMatrixClient(history_page=self._page())
        ingest = RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await listener.catchup(make_source_key(MESSENGER, 'main', ROOM))
        kinds = {e.kind for e in ingest.events}
        assert kinds == {
            EventKind.MESSAGE_NEW,
            EventKind.MESSAGE_EDITED,
            EventKind.MESSAGE_DELETED,
        }
        assert all(e.origin == 'catchup' for e in ingest.events)
        await listener.stop()

    async def test_catchup_runs_at_start_when_enabled(self) -> None:
        client = FakeMatrixClient(history_page=self._page())
        ingest = RecordingIngest()
        listener = _make(
            client=client,
            ingest=ingest,
            cursors=MemoryCursorStore(),
            catchup_enabled=True,
        )
        await listener.start()
        assert len(ingest.events) == 3  # история догнана на старте
        assert client.fetch_calls[0][0] == ROOM
        await listener.stop()

    async def test_catchup_disabled_skips_history(self) -> None:
        client = FakeMatrixClient(history_page=self._page())
        ingest = RecordingIngest()
        listener = _make(
            client=client,
            ingest=ingest,
            cursors=MemoryCursorStore(),
            catchup_enabled=False,
        )
        await listener.start()
        assert ingest.events == []
        assert client.fetch_calls == []
        await listener.stop()


class TestRecentPoll:
    """T032 фаза 2: лёгкий поллинг недавнего окна Matrix-комнат."""

    def _page(self) -> MatrixHistoryPage:
        return MatrixHistoryPage(
            messages=(
                raw_message(kind=EventKind.MESSAGE_EDITED, event_id='$e2', text='пр'),
            ),
            redactions=(raw_deletion(redacts_event_id='$gone'),),
        )

    def _listener(
        self,
        client: FakeMatrixClient,
        ingest: RecordingIngest,
        *,
        recent_endpoints: frozenset[EndpointConfig],
        recent_interval: float,
    ) -> MatrixListener:
        return MatrixListener(
            ingest=cast('IngestService', ingest),
            clients={'main': client},
            sources=[_ep('main', ROOM)],
            cursors=MemoryCursorStore(),
            analytics=MemoryAnalytics(),
            log=get_logger('test.matrix.recent'),
            catchup_enabled=False,  # только лёгкий поллинг, без глубокого на старте
            recent_poll_endpoints=recent_endpoints,
            recent_interval=recent_interval,
            recent_window_messages=7,
            recent_window_minutes=52_560_000,  # ~100 лет: без отсечки по возрасту
        )

    async def test_recent_poll_timer_polls_window_and_emits(self) -> None:
        client = FakeMatrixClient(history_page=self._page())
        ingest = RecordingIngest()
        listener = self._listener(
            client,
            ingest,
            recent_endpoints=frozenset({_ep('main', ROOM)}),
            recent_interval=0.01,
        )
        await listener.start()
        for _ in range(200):  # ждём первый проход таймера (фетч с window-лимитом)
            if any(limit == 7 for _room, limit in client.fetch_calls):
                break
            await asyncio.sleep(0.01)
        await listener.stop()
        assert any(limit == 7 for _room, limit in client.fetch_calls)
        # правка (m.replace) и удаление (redaction) в окне доехали
        kinds = {e.kind for e in ingest.events}
        assert EventKind.MESSAGE_EDITED in kinds
        assert EventKind.MESSAGE_DELETED in kinds

    async def test_recent_poll_absent_without_enabled_rooms(self) -> None:
        client = FakeMatrixClient(history_page=self._page())
        listener = self._listener(
            client,
            RecordingIngest(),
            recent_endpoints=frozenset(),  # ни одна комната не включена
            recent_interval=0.01,
        )
        await listener.start()
        await asyncio.sleep(0.05)  # дать шанс таймеру (его быть не должно)
        await listener.stop()
        assert client.fetch_calls == []  # ни глубокого, ни лёгкого фетча


class TestMediaEnrich:
    async def test_download_sets_local_path_when_policy_on(self) -> None:
        client = FakeMatrixClient()
        ingest = RecordingIngest()
        listener = _make(
            client=client,
            ingest=ingest,
            cursors=MemoryCursorStore(),
            media_policy=MediaConfig(download=True, storage_dir='data/m'),
        )
        await listener.start()
        await client.fire_new(
            raw_message(media=(RawMatrixMedia(kind='photo', ref='mxc://s/x'),))
        )
        assert client.downloads[0]['mxc'] == 'mxc://s/x'
        assert ingest.events[0].media[0].local_path == 'data/m/x'
        await listener.stop()

    async def test_no_download_when_policy_off(self) -> None:
        client = FakeMatrixClient()
        ingest = RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_new(
            raw_message(media=(RawMatrixMedia(kind='photo', ref='mxc://s/x'),))
        )
        assert client.downloads == []
        assert ingest.events[0].media[0].local_path is None
        await listener.stop()


class TestMessageRouting:
    async def test_new_message_in_source_room_ingested(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_new(raw_message())
        assert len(ingest.events) == 1
        assert ingest.events[0].kind is EventKind.MESSAGE_NEW
        assert ingest.events[0].external_id == '$evt-1'
        await listener.stop()

    async def test_message_in_other_room_ignored(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_new(raw_message(room_id=OTHER_ROOM))
        assert ingest.events == []
        await listener.stop()

    async def test_edit_ingested_as_edited(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_edit(
            raw_message(kind=EventKind.MESSAGE_EDITED, text='правка')
        )
        assert len(ingest.events) == 1
        assert ingest.events[0].kind is EventKind.MESSAGE_EDITED
        await listener.stop()

    async def test_redaction_in_source_room_ingested(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_delete(raw_deletion())
        assert len(ingest.events) == 1
        assert ingest.events[0].kind is EventKind.MESSAGE_DELETED
        await listener.stop()

    async def test_redaction_in_other_room_ignored(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        listener = _make(client=client, ingest=ingest, cursors=MemoryCursorStore())
        await listener.start()
        await client.fire_delete(raw_deletion(room_id=OTHER_ROOM))
        assert ingest.events == []
        await listener.stop()

    async def test_resolves_alias_for_filtering(self) -> None:
        """Источник задан alias'ом — фильтр работает по резолвленному id."""
        client = FakeMatrixClient(rooms={'#angarion:matrix.example': ROOM})
        ingest = RecordingIngest()
        listener = _make(
            client=client,
            ingest=ingest,
            cursors=MemoryCursorStore(),
            sources=[_ep('main', '#angarion:matrix.example')],
        )
        await listener.start()
        await client.fire_new(raw_message())  # событие в резолвленной комнате
        assert len(ingest.events) == 1
        await listener.stop()


class TestUndecryptable:
    async def test_utd_skipped_and_recorded(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        analytics = MemoryAnalytics()
        listener = _make(
            client=client, ingest=ingest, cursors=MemoryCursorStore(),
            analytics=analytics,
        )
        await listener.start()
        await client.fire_undecryptable(raw_undecryptable())
        assert ingest.events == []  # UTD не доходит до пайплайна
        recorded = await analytics.recent(kind='matrix_undecryptable')
        assert len(recorded) == 1
        assert recorded[0].payload['event_id'] == '$utd-1'
        await listener.stop()

    async def test_utd_in_other_room_ignored(self) -> None:
        client, ingest = FakeMatrixClient(), RecordingIngest()
        analytics = MemoryAnalytics()
        listener = _make(
            client=client, ingest=ingest, cursors=MemoryCursorStore(),
            analytics=analytics,
        )
        await listener.start()
        await client.fire_undecryptable(raw_undecryptable(room_id=OTHER_ROOM))
        assert await analytics.recent(kind='matrix_undecryptable') == []
        await listener.stop()


class TestCursor:
    async def test_sync_callback_persists_next_batch(self) -> None:
        cursors = MemoryCursorStore()
        client = FakeMatrixClient()
        listener = _make(client=client, ingest=RecordingIngest(), cursors=cursors)
        await listener.start()
        await client.fire_sync('s-next-42')
        sync_key = make_source_key(MESSENGER, 'main', SYNC_CURSOR_CHAT)
        cursor = await cursors.load(sync_key)
        assert cursor is not None
        assert cursor.payload['next_batch'] == 's-next-42'
        await listener.stop()
