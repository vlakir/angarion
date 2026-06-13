"""Фейки и фабрики для юнит-тестов Telegram-адаптера (M3, фазы 2–3)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.adapters.telegram.client import (
    RawDeletionHandler,
    RawMessageHandler,
    RawTelegramDeletion,
    RawTelegramMessage,
)
from angarion.domain.models import EventKind

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from angarion.domain.models import InboundEvent

NOW = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)


def raw_message(**overrides: object) -> RawTelegramMessage:
    fields: dict[str, object] = {
        'kind': EventKind.MESSAGE_NEW,
        'chat_id': -100123,
        'message_id': 42,
        'text': 'привет',
        'sender_id': 777,
        'sender_name': 'Алиса',
        'event_at': NOW,
    }
    fields.update(overrides)
    return RawTelegramMessage.model_validate(fields)


def raw_deletion(**overrides: object) -> RawTelegramDeletion:
    fields: dict[str, object] = {
        'chat_id': -100123,
        'message_ids': (42,),
        'deleted_at': NOW,
    }
    fields.update(overrides)
    return RawTelegramDeletion.model_validate(fields)


class FakeTelegramClient:
    """In-memory ``TelegramClientPort``: резолв по карте + ручной fire."""

    def __init__(
        self,
        *,
        peer_ids: dict[str, int] | None = None,
        fail_peers: tuple[str, ...] = (),
        history: dict[int, list[RawTelegramMessage]] | None = None,
        send_effects: list[Exception | None] | None = None,
    ) -> None:
        self.warmed = 0
        self._peer_ids = dict(peer_ids or {})
        self._fail_peers = set(fail_peers)
        # история per chat_id: хронологический порядок (старые первыми)
        self._history = {chat: list(msgs) for chat, msgs in (history or {}).items()}
        self.fetch_calls: list[tuple[int, int | None, int, int]] = []
        # отправка: запись попыток + очередь эффектов (None — успех)
        self.sent: list[dict[str, object]] = []
        self._send_effects = list(send_effects or [])
        self._next_message_id = 1000
        self._on_new: list[RawMessageHandler] = []
        self._on_edit: list[RawMessageHandler] = []
        self._on_delete: list[RawDeletionHandler] = []

    async def warm_entity_cache(self) -> None:
        self.warmed += 1

    async def resolve_peer(self, peer: str) -> int:
        if peer in self._fail_peers:
            msg = f'нет доступа к {peer}'
            raise ValueError(msg)
        return self._peer_ids[peer]

    async def fetch_history(
        self,
        chat_id: int,
        *,
        limit: int,
        thread_id: int | None = None,
        min_id: int = 0,
    ) -> AsyncIterator[RawTelegramMessage]:
        """Новые→старые, id > min_id, до limit; фильтр по топику если задан."""
        self.fetch_calls.append((chat_id, thread_id, min_id, limit))
        ordered = sorted(
            self._history.get(chat_id, ()),
            key=lambda m: m.message_id,
            reverse=True,
        )
        yielded = 0
        for raw in ordered:
            if raw.message_id <= min_id:
                continue
            if thread_id is not None and raw.thread_id != thread_id:
                continue
            if yielded >= limit:
                break
            yielded += 1
            yield raw

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to: int | None = None,
        parse_mode: str | None = None,
        silent: bool = False,
        link_preview: bool = True,
    ) -> int:
        """Записать попытку; разыграть очередной эффект (None — успех)."""
        self.sent.append(
            {
                'chat_id': chat_id,
                'text': text,
                'reply_to': reply_to,
                'parse_mode': parse_mode,
                'silent': silent,
                'link_preview': link_preview,
            }
        )
        if self._send_effects:
            effect = self._send_effects.pop(0)
            if effect is not None:
                raise effect
        self._next_message_id += 1
        return self._next_message_id

    def on_new_message(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        self._on_edit.append(handler)

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        self._on_delete.append(handler)

    async def fire_new(self, raw: RawTelegramMessage) -> None:
        for handler in self._on_new:
            await handler(raw)

    async def fire_edit(self, raw: RawTelegramMessage) -> None:
        for handler in self._on_edit:
            await handler(raw)

    async def fire_delete(self, raw: RawTelegramDeletion) -> None:
        for handler in self._on_delete:
            await handler(raw)


class FakePool:
    """Лёгкий ``ClientPool``: фиксированная мапа клиентов + учёт lifecycle."""

    def __init__(self, clients: dict[str, FakeTelegramClient]) -> None:
        self._clients = dict(clients)
        self.connects = 0
        self.disconnects = 0

    @property
    def account_ids(self) -> tuple[str, ...]:
        return tuple(self._clients)

    @property
    def clients(self) -> dict[str, FakeTelegramClient]:
        return self._clients

    async def connect_all(self) -> None:
        self.connects += 1

    async def disconnect_all(self) -> None:
        self.disconnects += 1


class RecordingIngest:
    """Дублёр ``IngestService``: копит принятые события."""

    def __init__(self) -> None:
        self.events: list[InboundEvent] = []

    async def ingest(self, event: InboundEvent) -> None:
        self.events.append(event)
