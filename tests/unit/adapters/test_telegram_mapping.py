"""Маппинг сырых событий Telethon → InboundEvent (M3, фаза 2)."""

from __future__ import annotations

from telegram_fakes import raw_deletion, raw_message

from angarion.adapters.telegram.client import RawMedia
from angarion.adapters.telegram.mapping import map_deletion, map_message
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import EventKind

ACCOUNT = 'main'
SOURCE_KEY = make_source_key('telegram', ACCOUNT, '-100123')


def test_new_message_maps_to_inbound() -> None:
    event = map_message(raw_message(), ACCOUNT)
    assert event is not None
    assert event.kind is EventKind.MESSAGE_NEW
    assert event.origin == 'live'
    assert event.source.messenger == 'telegram'
    assert event.source.chat_id == '-100123'
    assert event.source.thread_id is None
    assert event.received_by.account_id == ACCOUNT
    assert event.external_id == '42'
    assert event.sender_id == '777'
    assert event.sender_name == 'Алиса'
    assert event.text == 'привет'
    assert event.content_hash == normalize_and_hash('привет')
    assert event.dedup_key == make_dedup_key(
        EventKind.MESSAGE_NEW, SOURCE_KEY, '42', event.content_hash
    )


def test_edited_message_dedup_includes_hash() -> None:
    event = map_message(raw_message(kind=EventKind.MESSAGE_EDITED, text='ред'), ACCOUNT)
    assert event is not None
    assert event.kind is EventKind.MESSAGE_EDITED
    assert event.dedup_key == make_dedup_key(
        EventKind.MESSAGE_EDITED, SOURCE_KEY, '42', normalize_and_hash('ред')
    )


def test_service_message_is_filtered() -> None:
    assert map_message(raw_message(is_service=True), ACCOUNT) is None


def test_reply_sets_reply_to_external_id() -> None:
    event = map_message(raw_message(reply_to_message_id=10), ACCOUNT)
    assert event is not None
    assert event.kind is EventKind.MESSAGE_NEW
    assert event.reply_to_external_id == '10'


def test_media_maps_to_media_ref() -> None:
    media = (
        RawMedia(
            kind='photo',
            mime_type='image/jpeg',
            file_name='p.jpg',
            size=2048,
            width=800,
            height=600,
        ),
    )
    event = map_message(raw_message(media=media, text=None), ACCOUNT)
    assert event is not None
    assert event.has_media is True
    assert [m.kind for m in event.media] == ['photo']
    assert event.media[0].mime_type == 'image/jpeg'
    assert event.media[0].size == 2048
    # ref = координаты источника "chat_id:message_id" (refetch-fast-path A2)
    assert event.media[0].ref == '-100123:42'
    assert event.text is None
    assert event.content_hash is None


def test_no_media_yields_empty_list() -> None:
    event = map_message(raw_message(), ACCOUNT)
    assert event is not None
    assert event.media == []
    assert event.has_media is False


def test_media_only_edited_does_not_crash() -> None:
    """M7 must-fix: правка медиа-only сообщения (text=None) маппится, не падая
    в make_dedup_key (раньше: content_hash обязателен для EDITED)."""
    media = (RawMedia(kind='photo', size=2048),)
    event = map_message(
        raw_message(kind=EventKind.MESSAGE_EDITED, text=None, media=media), ACCOUNT
    )
    assert event is not None
    assert event.kind is EventKind.MESSAGE_EDITED
    assert ':edit:media:' in event.dedup_key


def test_thread_id_enters_source_and_key() -> None:
    event = map_message(raw_message(thread_id=55), ACCOUNT)
    assert event is not None
    assert event.source.thread_id == '55'
    threaded_key = make_source_key('telegram', ACCOUNT, '-100123', '55')
    assert event.dedup_key.startswith(threaded_key)


def test_text_none_no_hash() -> None:
    event = map_message(raw_message(text=None), ACCOUNT)
    assert event is not None
    assert event.content_hash is None


def test_raw_payload_preserved_and_json_safe() -> None:
    event = map_message(raw_message(), ACCOUNT)
    assert event is not None
    assert event.raw['chat_id'] == -100123
    assert event.raw['event_at'] == '2026-06-13T12:00:00Z'


def test_deletion_maps_each_id() -> None:
    events = map_deletion(raw_deletion(message_ids=(10, 11)), ACCOUNT)
    assert [e.external_id for e in events] == ['10', '11']
    for event in events:
        assert event.kind is EventKind.MESSAGE_DELETED
        assert event.source.chat_id == '-100123'
        assert event.source.thread_id is None
        assert event.dedup_key == make_dedup_key(
            EventKind.MESSAGE_DELETED, SOURCE_KEY, event.external_id
        )


def test_deletion_without_chat_is_empty() -> None:
    assert map_deletion(raw_deletion(chat_id=None), ACCOUNT) == []
