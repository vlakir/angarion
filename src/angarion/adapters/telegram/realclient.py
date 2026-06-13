"""
Реальная обёртка над ``telethon.TelegramClient`` (граница Telethon, M3).

Единственный telethon-зависимый модуль адаптера: реализует
``TelegramClientPort`` чистой делегацией и раскладывает сырые события
Telethon в нормализованные DTO (``client.RawTelegram*``). Вся логика —
над портом (mapping/resolver/listener), здесь — бритвенно-тонкое
извлечение полей (W1 спеки T005): сетевые вызовы в CI не покрываются,
поэтому модуль держим максимально «глупым», а корректность извлечения
проверяется юнит-тестами на duck-typed-заглушках и ручным суточным
прогоном (фаза 6).

Подключение/дисконнект клиента и пул сессий (``ClientRegistry``) —
composition root (фаза 5); здесь обёртка лишь оборачивает уже
подключённый клиент. Telethon без ``py.typed`` — под
``ignore_missing_imports`` его типы суть ``Any`` (W2).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from telethon import TelegramClient, errors, events, utils
from telethon.sessions import StringSession
from telethon.tl.types import MessageReplyHeader, MessageService

from angarion.adapters.telegram.client import (
    FloodWaitError,
    RawTelegramDeletion,
    RawTelegramMessage,
    TransientSendError,
    as_peer,
)
from angarion.domain.models import EventKind

_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    errors.ServerError,
    errors.TimedOutError,
    ConnectionError,
    TimeoutError,
)
"""Сетевые/серверные сбои Telethon — повторяемы sender'ом (§8)."""

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from telethon.tl.custom import Message

    from angarion.adapters.telegram.client import (
        RawDeletionHandler,
        RawMessageHandler,
    )


def _topic_id(reply: MessageReplyHeader | None) -> int | None:
    """Id форум-топика сообщения (thread_id), если это сообщение в топике."""
    if reply is None or not getattr(reply, 'forum_topic', False):
        return None
    top_id: int | None = reply.reply_to_top_id
    return top_id if top_id is not None else reply.reply_to_msg_id


def _reply_id(reply: MessageReplyHeader | None) -> int | None:
    """Id сообщения-цели ответа; маркер топика (не ответ) — ``None``."""
    if reply is None or reply.reply_to_msg_id is None:
        return None
    if getattr(reply, 'forum_topic', False) and reply.reply_to_top_id is None:
        return None
    reply_id: int = reply.reply_to_msg_id
    return reply_id


def to_raw_message(
    event: events.NewMessage.Event, kind: EventKind
) -> RawTelegramMessage:
    """Сырое событие Telethon NewMessage/MessageEdited → DTO."""
    message = event.message
    service = isinstance(message, MessageService)
    event_at = (message.edit_date if kind is EventKind.MESSAGE_EDITED else None) or (
        message.date
    )
    return RawTelegramMessage(
        kind=kind,
        chat_id=event.chat_id,
        message_id=message.id,
        thread_id=_topic_id(message.reply_to),
        text=None if service else message.message,
        sender_id=message.sender_id,
        sender_name=utils.get_display_name(message.sender) or None,
        reply_to_message_id=_reply_id(message.reply_to),
        has_media=message.media is not None,
        is_service=service,
        event_at=event_at,
    )


def to_raw_history_message(message: Message) -> RawTelegramMessage:
    """Сообщение из ``iter_messages`` (catch-up §9.3) → DTO (kind=NEW)."""
    service = isinstance(message, MessageService)
    return RawTelegramMessage(
        kind=EventKind.MESSAGE_NEW,
        chat_id=message.chat_id,
        message_id=message.id,
        thread_id=_topic_id(message.reply_to),
        text=None if service else message.message,
        sender_id=message.sender_id,
        sender_name=utils.get_display_name(message.sender) or None,
        reply_to_message_id=_reply_id(message.reply_to),
        has_media=message.media is not None,
        is_service=service,
        event_at=message.date,
    )


def to_raw_deletion(event: events.MessageDeleted.Event) -> RawTelegramDeletion:
    """Сырое событие Telethon MessageDeleted → DTO (timestamp — приём)."""
    return RawTelegramDeletion(
        chat_id=event.chat_id,
        message_ids=tuple(event.deleted_ids),
        deleted_at=datetime.now(UTC),
    )


class TelethonClient:
    """``TelegramClientPort`` поверх подключённого ``TelegramClient``."""

    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    async def warm_entity_cache(self) -> None:
        """Прогреть кэш сущностей одним ``get_dialogs`` (Q5)."""
        await self._client.get_dialogs()

    async def resolve_peer(self, peer: str) -> int:
        """
        Резолв peer → знаковый chat id.

        Числовой id приводим к ``int`` (``as_peer``): на строке вида
        ``"-100…"`` ``get_peer_id`` падает ``Cannot find any entity``
        (gotcha T004 §3) — Telethon принимает числовой id только как int.
        """
        chat_id: int = await self._client.get_peer_id(as_peer(peer))
        return chat_id

    async def fetch_history(
        self,
        chat_id: int,
        *,
        limit: int,
        thread_id: int | None = None,
        min_id: int = 0,
    ) -> AsyncIterator[RawTelegramMessage]:
        """История чата/топика, новые→старые, id > ``min_id`` (catch-up §9.3)."""
        async for message in self._client.iter_messages(
            chat_id, limit=limit, min_id=min_id, reply_to=thread_id
        ):
            yield to_raw_history_message(message)

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
        """Отправить текст; ошибки Telethon → port-исключения (FR «Sender»)."""
        try:
            sent = await self._client.send_message(
                chat_id,
                text,
                reply_to=reply_to,
                parse_mode=parse_mode,
                silent=silent,
                link_preview=link_preview,
            )
        except errors.FloodWaitError as exc:
            raise FloodWaitError(seconds=exc.seconds) from exc
        except _TRANSIENT_ERRORS as exc:
            raise TransientSendError(str(exc)) from exc
        message_id: int = sent.id
        return message_id

    async def disconnect(self) -> None:
        """Закрыть соединение клиента (``ClientRegistry.disconnect_all``)."""
        await self._client.disconnect()

    def on_new_message(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на ``events.NewMessage``."""
        self._client.add_event_handler(
            self._message_callback(handler, EventKind.MESSAGE_NEW),
            events.NewMessage(),
        )

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на ``events.MessageEdited``."""
        self._client.add_event_handler(
            self._message_callback(handler, EventKind.MESSAGE_EDITED),
            events.MessageEdited(),
        )

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        """Подписать колбэк на ``events.MessageDeleted``."""

        async def callback(event: events.MessageDeleted.Event) -> None:
            await handler(to_raw_deletion(event))

        self._client.add_event_handler(callback, events.MessageDeleted())

    @staticmethod
    def _message_callback(
        handler: RawMessageHandler, kind: EventKind
    ) -> Callable[[events.NewMessage.Event], Awaitable[None]]:
        async def callback(event: events.NewMessage.Event) -> None:
            await handler(to_raw_message(event, kind))

        return callback


async def connect_client(
    api_id: int, api_hash: str, session_string: str
) -> TelethonClient:
    """
    Подключить реальный Telethon-клиент по ``StringSession`` (дефолтный
    ``ClientRegistry.connect``, M3, фаза 5). Единственный сетевой путь —
    бритвенно-тонкий (W1): построить клиент, ``connect``, обернуть в порт.
    """
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.connect()
    return TelethonClient(client)


async def login_and_export_session(api_id: int, api_hash: str) -> str:
    """
    Интерактивная авторизация (``angarion login``): ``client.start``
    спросит номер/код/2FA, на выходе — строка ``StringSession`` (M3,
    фаза 5). Сетевой/интерактивный путь — бритвенно-тонкий (W1).
    """
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()
    session_string: str = client.session.save()
    await client.disconnect()
    return session_string
