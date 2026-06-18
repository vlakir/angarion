"""Фейки и фабрики для юнит-тестов Matrix-адаптера (M7 B2, T010)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.adapters.matrix.client import (
    MatrixHistoryPage,
    RawDeletionHandler,
    RawMatrixDeletion,
    RawMatrixMessage,
    RawMatrixUndecryptable,
    RawMessageHandler,
    SyncHandler,
    UndecryptableHandler,
)
from angarion.domain.models import EventKind

if TYPE_CHECKING:
    from angarion.domain.models import InboundEvent

NOW = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
ROOM = '!room:matrix.example'


def raw_message(**overrides: object) -> RawMatrixMessage:
    fields: dict[str, object] = {
        'kind': EventKind.MESSAGE_NEW,
        'room_id': ROOM,
        'event_id': '$evt-1',
        'text': 'привет',
        'sender_id': '@alice:matrix.example',
        'sender_name': 'Алиса',
        'event_at': NOW,
    }
    fields.update(overrides)
    return RawMatrixMessage.model_validate(fields)


def raw_deletion(**overrides: object) -> RawMatrixDeletion:
    fields: dict[str, object] = {
        'room_id': ROOM,
        'redacts_event_id': '$evt-1',
        'redacted_at': NOW,
    }
    fields.update(overrides)
    return RawMatrixDeletion.model_validate(fields)


def raw_undecryptable(**overrides: object) -> RawMatrixUndecryptable:
    fields: dict[str, object] = {
        'room_id': ROOM,
        'event_id': '$utd-1',
        'sender_id': '@alice:matrix.example',
        'event_at': NOW,
    }
    fields.update(overrides)
    return RawMatrixUndecryptable.model_validate(fields)


class FakeMatrixClient:
    """In-memory ``MatrixClientPort``: ручной fire событий + учёт lifecycle."""

    def __init__(
        self,
        *,
        rooms: dict[str, str] | None = None,
        fail_rooms: tuple[str, ...] = (),
        send_effects: list[Exception | None] | None = None,
        history_page: MatrixHistoryPage | None = None,
        download_effects: list[str | None] | None = None,
    ) -> None:
        self.restored = 0
        self.stopped = 0
        self.synced_since: list[str | None] = []
        self._rooms = dict(rooms or {})
        self._fail_rooms = set(fail_rooms)
        self._on_new: list[RawMessageHandler] = []
        self._on_edit: list[RawMessageHandler] = []
        self._on_delete: list[RawDeletionHandler] = []
        self._on_utd: list[UndecryptableHandler] = []
        self._on_sync: list[SyncHandler] = []
        self._stop_event = asyncio.Event()
        # отправка: запись попыток + очередь эффектов (None — успех)
        self.sent: list[dict[str, object]] = []
        self._send_effects = list(send_effects or [])
        self._next_event = 0
        # история (catch-up) и скачивание (enrich)
        self.history_page = history_page or MatrixHistoryPage()
        self.fetch_calls: list[tuple[str, int]] = []
        self.downloads: list[dict[str, str]] = []
        self._download_effects = list(download_effects or [])

    async def restore(self) -> None:
        self.restored += 1

    async def resolve_room(self, alias_or_id: str) -> str:
        if alias_or_id in self._fail_rooms:
            msg = f'нет доступа к {alias_or_id}'
            raise ValueError(msg)
        return self._rooms.get(alias_or_id, alias_or_id)

    def _play_send_effect(self) -> None:
        if self._send_effects:
            effect = self._send_effects.pop(0)
            if effect is not None:
                raise effect

    async def send_text(
        self,
        room_id: str,
        body: str,
        *,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        self.sent.append(
            {
                'room_id': room_id,
                'body': body,
                'thread_root': thread_root,
                'reply_to': reply_to,
                'media': False,
            }
        )
        self._play_send_effect()
        self._next_event += 1
        return f'$sent-{self._next_event}'

    async def send_media(
        self,
        room_id: str,
        *,
        body: str,
        kind: str,
        mxc_ref: str | None = None,
        local_path: str | None = None,
        mime_type: str | None = None,
        file_name: str | None = None,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        self.sent.append(
            {
                'room_id': room_id,
                'body': body,
                'kind': kind,
                'mxc_ref': mxc_ref,
                'local_path': local_path,
                'thread_root': thread_root,
                'media': True,
            }
        )
        self._play_send_effect()
        self._next_event += 1
        return f'$sent-{self._next_event}'

    async def fetch_history(self, room_id: str, *, limit: int) -> MatrixHistoryPage:
        self.fetch_calls.append((room_id, limit))
        return self.history_page

    async def download_media(self, *, mxc: str, dest_dir: str) -> str | None:
        self.downloads.append({'mxc': mxc, 'dest_dir': dest_dir})
        if self._download_effects:
            return self._download_effects.pop(0)
        return f'{dest_dir}/{mxc.rsplit("/", 1)[-1]}'

    async def sync_forever(self, *, since: str | None) -> None:
        self.synced_since.append(since)
        await self._stop_event.wait()

    async def stop(self) -> None:
        self.stopped += 1
        self._stop_event.set()

    def on_new_message(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        self._on_edit.append(handler)

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        self._on_delete.append(handler)

    def on_undecryptable(self, handler: UndecryptableHandler) -> None:
        self._on_utd.append(handler)

    def on_sync(self, handler: SyncHandler) -> None:
        self._on_sync.append(handler)

    async def fire_new(self, raw: RawMatrixMessage) -> None:
        for handler in self._on_new:
            await handler(raw)

    async def fire_edit(self, raw: RawMatrixMessage) -> None:
        for handler in self._on_edit:
            await handler(raw)

    async def fire_delete(self, raw: RawMatrixDeletion) -> None:
        for handler in self._on_delete:
            await handler(raw)

    async def fire_undecryptable(self, raw: RawMatrixUndecryptable) -> None:
        for handler in self._on_utd:
            await handler(raw)

    async def fire_sync(self, next_batch: str) -> None:
        for handler in self._on_sync:
            await handler(next_batch)


class RecordingIngest:
    """Дублёр ``IngestService``: копит принятые события."""

    def __init__(self) -> None:
        self.events: list[InboundEvent] = []

    async def ingest(self, event: InboundEvent) -> None:
        self.events.append(event)
