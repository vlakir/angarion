"""
Тонкая Telethon-обёртка (W1): делегация на Mock + извлечение полей на
duck-typed-заглушках. Сетевых вызовов нет; реальная корректность
извлечения подтверждается суточным прогоном (фаза 6).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon import errors, events

from angarion.adapters.telegram.client import FloodWaitError, TransientSendError
from angarion.adapters.telegram import realclient
from angarion.adapters.telegram.realclient import (
    TelethonClient,
    connect_client,
    login_and_export_session,
    to_raw_deletion,
    to_raw_history_message,
    to_raw_message,
)
from angarion.domain.models import EventKind

if TYPE_CHECKING:
    from angarion.adapters.telegram.client import RawTelegramMessage

NOW = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)


def _message(**overrides: object) -> SimpleNamespace:
    fields: dict[str, object] = {
        'id': 42,
        'message': 'привет',
        'date': NOW,
        'edit_date': None,
        'sender_id': 777,
        'sender': None,
        'media': None,
        'file': None,
        'reply_to': None,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _event(message: SimpleNamespace, chat_id: int = -100123) -> SimpleNamespace:
    return SimpleNamespace(message=message, chat_id=chat_id)


def test_to_raw_message_basic_new() -> None:
    raw: RawTelegramMessage = to_raw_message(
        _event(_message()), EventKind.MESSAGE_NEW
    )
    assert raw.kind is EventKind.MESSAGE_NEW
    assert raw.chat_id == -100123
    assert raw.message_id == 42
    assert raw.text == 'привет'
    assert raw.sender_id == 777
    assert raw.sender_name is None
    assert raw.media == ()
    assert raw.is_service is False
    assert raw.thread_id is None
    assert raw.reply_to_message_id is None
    assert raw.event_at == NOW


def test_edited_uses_edit_date() -> None:
    edited = NOW.replace(hour=13)
    raw = to_raw_message(
        _event(_message(edit_date=edited)), EventKind.MESSAGE_EDITED
    )
    assert raw.event_at == edited


def test_media_extracted_from_file() -> None:
    file = SimpleNamespace(
        mime_type='image/jpeg',
        name='photo.jpg',
        size=2048,
        width=800,
        height=600,
        duration=None,
    )
    message = _message(message=None, file=file, photo=object())
    raw = to_raw_message(_event(message), EventKind.MESSAGE_NEW)
    assert len(raw.media) == 1
    media = raw.media[0]
    assert media.kind == 'photo'
    assert media.mime_type == 'image/jpeg'
    assert media.file_name == 'photo.jpg'
    assert media.size == 2048
    assert media.width == 800
    assert media.height == 600


def test_media_kind_falls_back_to_document() -> None:
    file = SimpleNamespace(
        mime_type='application/pdf', name='doc.pdf', size=10, duration=None
    )
    raw = to_raw_message(_event(_message(file=file)), EventKind.MESSAGE_NEW)
    assert raw.media[0].kind == 'document'


def test_voice_duration_coerced_to_int() -> None:
    file = SimpleNamespace(mime_type='audio/ogg', name=None, size=5, duration=12.7)
    message = _message(file=file, voice=object())
    raw = to_raw_message(_event(message), EventKind.MESSAGE_NEW)
    assert raw.media[0].kind == 'voice'
    assert raw.media[0].duration == 12


def test_no_file_means_no_media() -> None:
    """Превью ссылки/опрос (``message.file is None``) — не вложение."""
    raw = to_raw_message(_event(_message(media=object())), EventKind.MESSAGE_NEW)
    assert raw.media == ()


def test_topic_reply_extracts_thread_and_reply() -> None:
    reply = SimpleNamespace(
        forum_topic=True, reply_to_top_id=55, reply_to_msg_id=50
    )
    raw = to_raw_message(_event(_message(reply_to=reply)), EventKind.MESSAGE_NEW)
    assert raw.thread_id == 55
    assert raw.reply_to_message_id == 50


def test_topic_root_marker_is_not_reply() -> None:
    reply = SimpleNamespace(
        forum_topic=True, reply_to_top_id=None, reply_to_msg_id=55
    )
    raw = to_raw_message(_event(_message(reply_to=reply)), EventKind.MESSAGE_NEW)
    assert raw.thread_id == 55
    assert raw.reply_to_message_id is None


def test_plain_reply_without_topic() -> None:
    reply = SimpleNamespace(
        forum_topic=False, reply_to_top_id=None, reply_to_msg_id=10
    )
    raw = to_raw_message(_event(_message(reply_to=reply)), EventKind.MESSAGE_NEW)
    assert raw.thread_id is None
    assert raw.reply_to_message_id == 10


def test_to_raw_deletion_supergroup() -> None:
    raw = to_raw_deletion(SimpleNamespace(chat_id=-100123, deleted_ids=[7, 8]))
    assert raw.chat_id == -100123
    assert raw.message_ids == (7, 8)


def test_to_raw_deletion_legacy_no_chat() -> None:
    raw = to_raw_deletion(SimpleNamespace(chat_id=None, deleted_ids=[7]))
    assert raw.chat_id is None


def test_to_raw_history_message_maps_fields() -> None:
    message = _message()
    message.chat_id = -100123
    raw = to_raw_history_message(message)
    assert raw.kind is EventKind.MESSAGE_NEW
    assert raw.chat_id == -100123
    assert raw.message_id == 42
    assert raw.text == 'привет'
    assert raw.event_at == NOW


def _mock_client() -> MagicMock:
    client = MagicMock()
    client.get_dialogs = AsyncMock()
    client.get_peer_id = AsyncMock(return_value=-100123)
    client.add_event_handler = MagicMock()
    return client


async def test_fetch_history_delegates_and_maps() -> None:
    client = _mock_client()
    msg_a = _message(id=11)
    msg_a.chat_id = -100123
    msg_b = _message(id=12)
    msg_b.chat_id = -100123

    async def _iter(*_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
        for message in (msg_a, msg_b):
            yield message

    client.iter_messages = _iter
    raws = [
        raw
        async for raw in TelethonClient(client).fetch_history(
            -100123, limit=10, min_id=10
        )
    ]
    assert [raw.message_id for raw in raws] == [11, 12]


async def test_send_message_delegates_and_returns_id() -> None:
    client = _mock_client()
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=999))
    message_id = await TelethonClient(client).send_message(
        -100123, 'привет', reply_to=55, parse_mode='md', silent=True
    )
    assert message_id == 999
    client.send_message.assert_awaited_once_with(
        -100123,
        'привет',
        reply_to=55,
        parse_mode='md',
        silent=True,
        link_preview=True,
    )


async def test_send_media_refetches_source_and_sends_file() -> None:
    client = _mock_client()
    origin = SimpleNamespace(media=object())
    client.get_messages = AsyncMock(return_value=origin)
    client.send_file = AsyncMock(return_value=SimpleNamespace(id=555))
    message_id = await TelethonClient(client).send_media(
        -100999, source_ref='-100123:42', text='подпись', reply_to=7
    )
    assert message_id == 555
    client.get_messages.assert_awaited_once_with(-100123, ids=42)
    client.send_file.assert_awaited_once_with(
        -100999,
        origin.media,
        caption='подпись',
        reply_to=7,
        parse_mode=None,
        silent=False,
    )


async def test_send_media_degrades_to_text_when_source_gone() -> None:
    client = _mock_client()
    client.get_messages = AsyncMock(return_value=None)
    client.send_message = AsyncMock(return_value=SimpleNamespace(id=42))
    client.send_file = AsyncMock()
    message_id = await TelethonClient(client).send_media(
        -100999, source_ref='-100123:42', text='подпись'
    )
    assert message_id == 42
    client.send_file.assert_not_awaited()
    client.send_message.assert_awaited_once()


async def test_send_media_translates_floodwait() -> None:
    client = _mock_client()
    client.get_messages = AsyncMock(
        side_effect=errors.FloodWaitError(request=None, capture=9)
    )
    with pytest.raises(FloodWaitError) as caught:
        await TelethonClient(client).send_media(-1, source_ref='-1:2', text='x')
    assert caught.value.seconds == 9


async def test_send_message_translates_floodwait() -> None:
    client = _mock_client()
    client.send_message = AsyncMock(
        side_effect=errors.FloodWaitError(request=None, capture=42)
    )
    with pytest.raises(FloodWaitError) as caught:
        await TelethonClient(client).send_message(-100123, 'x')
    assert caught.value.seconds == 42


async def test_send_message_translates_transient_server_error() -> None:
    client = _mock_client()
    client.send_message = AsyncMock(
        side_effect=errors.ServerError(request=None, message='500')
    )
    with pytest.raises(TransientSendError):
        await TelethonClient(client).send_message(-100123, 'x')


async def test_send_message_propagates_permanent_error() -> None:
    client = _mock_client()
    client.send_message = AsyncMock(
        side_effect=errors.ChatWriteForbiddenError(request=None)
    )
    with pytest.raises(errors.ChatWriteForbiddenError):
        await TelethonClient(client).send_message(-100123, 'x')


async def test_disconnect_delegates() -> None:
    client = _mock_client()
    client.disconnect = AsyncMock()
    await TelethonClient(client).disconnect()
    client.disconnect.assert_awaited_once()


async def test_connect_client_builds_connects_and_wraps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _mock_client()
    built.connect = AsyncMock()
    built.get_me = AsyncMock()
    built.catch_up = AsyncMock()
    captured: dict[str, object] = {}

    def fake_telegram_client(session: object, api_id: int, api_hash: str) -> object:
        captured['session'] = session
        captured['api_id'] = api_id
        captured['api_hash'] = api_hash
        return built

    monkeypatch.setattr(realclient, 'TelegramClient', fake_telegram_client)
    monkeypatch.setattr(realclient, 'StringSession', lambda s: f'SS:{s}')
    wrapper = await connect_client(123, 'hash', 'SESSION')
    assert isinstance(wrapper, TelethonClient)
    assert captured == {'session': 'SS:SESSION', 'api_id': 123, 'api_hash': 'hash'}
    built.connect.assert_awaited_once()
    # T030: реконсиляция update-state для приёма live-апдейтов
    built.get_me.assert_awaited_once()
    built.catch_up.assert_awaited_once()


async def test_login_and_export_session_starts_and_saves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _mock_client()
    built.start = AsyncMock()
    built.disconnect = AsyncMock()
    built.session = SimpleNamespace(save=lambda: 'EXPORTED')
    monkeypatch.setattr(
        realclient, 'TelegramClient', lambda *_a, **_k: built
    )
    monkeypatch.setattr(realclient, 'StringSession', lambda *_a: object())
    result = await login_and_export_session(2040, 'hash')
    assert result == 'EXPORTED'
    built.start.assert_awaited_once()
    built.disconnect.assert_awaited_once()


async def test_warm_delegates_to_get_dialogs() -> None:
    client = _mock_client()
    await TelethonClient(client).warm_entity_cache()
    client.get_dialogs.assert_awaited_once()


async def test_resolve_delegates_to_get_peer_id() -> None:
    client = _mock_client()
    result = await TelethonClient(client).resolve_peer('@grp')
    assert result == -100123
    client.get_peer_id.assert_awaited_once_with('@grp')


async def test_resolve_numeric_id_passed_as_int() -> None:
    """Числовой id → int: на строке ``-100…`` get_peer_id падает (T004 §3)."""
    client = _mock_client()
    await TelethonClient(client).resolve_peer('-1003385167603')
    client.get_peer_id.assert_awaited_once_with(-1003385167603)


async def test_on_new_message_registers_and_wires_callback() -> None:
    client = _mock_client()
    captured: list[RawTelegramMessage] = []

    async def handler(raw: RawTelegramMessage) -> None:
        captured.append(raw)

    TelethonClient(client).on_new_message(handler)
    callback, event_filter = client.add_event_handler.call_args[0]
    assert isinstance(event_filter, events.NewMessage)
    await callback(_event(_message()))
    assert captured[0].kind is EventKind.MESSAGE_NEW
    assert captured[0].message_id == 42


async def test_on_message_edited_registers_edited_filter() -> None:
    client = _mock_client()
    TelethonClient(client).on_message_edited(AsyncMock())
    _, event_filter = client.add_event_handler.call_args[0]
    assert isinstance(event_filter, events.MessageEdited)


async def test_on_message_deleted_wires_deletion_callback() -> None:
    client = _mock_client()
    captured: list[object] = []

    async def handler(raw: object) -> None:
        captured.append(raw)

    TelethonClient(client).on_message_deleted(handler)
    callback, event_filter = client.add_event_handler.call_args[0]
    assert isinstance(event_filter, events.MessageDeleted)
    await callback(SimpleNamespace(chat_id=-100123, deleted_ids=[5]))
    assert captured[0].message_ids == (5,)
