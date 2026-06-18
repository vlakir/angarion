"""
Маппинг сырых событий matrix-nio → ``InboundEvent`` (M7 B2, T010).

Чистые функции на фикстурах: без сети и без nio. Ключи/хэши — только
публичными хелперами ``angarion.domain.keys`` (§7.2).
"""

from __future__ import annotations

from angarion.adapters.matrix.client import MESSENGER, RawMatrixMedia
from angarion.adapters.matrix.mapping import (
    map_message,
    map_redaction,
    raw_media_hash,
)
from angarion.domain.keys import (
    make_dedup_key,
    make_media_hash,
    make_source_key,
    normalize_and_hash,
)
from angarion.domain.models import EventKind, MediaRef
from matrix_fakes import ROOM, raw_deletion, raw_message

ACCOUNT = 'main'


def _media(**overrides: object) -> RawMatrixMedia:
    fields: dict[str, object] = {
        'kind': 'photo',
        'ref': 'mxc://matrix.example/abc',
        'mime_type': 'image/jpeg',
        'file_name': 'pic.jpg',
        'size': 2048,
        'width': 800,
        'height': 600,
    }
    fields.update(overrides)
    return RawMatrixMedia.model_validate(fields)


class TestMapMessageNew:
    def test_core_fields(self) -> None:
        event = map_message(raw_message(), ACCOUNT)
        assert event.kind is EventKind.MESSAGE_NEW
        assert event.external_id == '$evt-1'
        assert event.source.messenger == MESSENGER
        assert event.source.chat_id == ROOM
        assert event.received_by.account_id == ACCOUNT
        assert event.text == 'привет'
        assert event.sender_id == '@alice:matrix.example'
        assert event.sender_name == 'Алиса'
        assert event.origin == 'live'

    def test_source_key_and_dedup_key(self) -> None:
        event = map_message(raw_message(), ACCOUNT)
        source_key = make_source_key(MESSENGER, ACCOUNT, ROOM)
        assert event.dedup_key == make_dedup_key(
            EventKind.MESSAGE_NEW, source_key, '$evt-1'
        )

    def test_content_hash_set(self) -> None:
        event = map_message(raw_message(text='hi'), ACCOUNT)
        assert event.content_hash == normalize_and_hash('hi')

    def test_text_none_keeps_hash_none(self) -> None:
        event = map_message(raw_message(text=None), ACCOUNT)
        assert event.content_hash is None

    def test_thread_id_flows_into_source(self) -> None:
        event = map_message(raw_message(thread_id='$root'), ACCOUNT)
        assert event.source.thread_id == '$root'
        expected = make_source_key(MESSENGER, ACCOUNT, ROOM, '$root')
        assert event.dedup_key.startswith(expected)

    def test_reply_maps_to_external_id(self) -> None:
        event = map_message(raw_message(reply_to_event_id='$parent'), ACCOUNT)
        assert event.reply_to_external_id == '$parent'

    def test_origin_catchup_propagates(self) -> None:
        event = map_message(raw_message(), ACCOUNT, origin='catchup')
        assert event.origin == 'catchup'

    def test_raw_snapshot_preserved(self) -> None:
        event = map_message(raw_message(), ACCOUNT)
        assert event.raw['event_id'] == '$evt-1'


class TestMapMessageMedia:
    def test_mxc_ref_and_metadata_preserved(self) -> None:
        event = map_message(raw_message(media=(_media(),)), ACCOUNT)
        assert len(event.media) == 1
        ref = event.media[0]
        assert isinstance(ref, MediaRef)
        assert ref.kind == 'photo'
        assert ref.ref == 'mxc://matrix.example/abc'
        assert ref.mime_type == 'image/jpeg'
        assert ref.file_name == 'pic.jpg'
        assert ref.size == 2048
        assert ref.width == 800
        assert ref.height == 600

    def test_media_hash_set(self) -> None:
        event = map_message(raw_message(media=(_media(),)), ACCOUNT)
        expected = make_media_hash([MediaRef(kind='photo', mime_type='image/jpeg',
                                             file_name='pic.jpg', size=2048,
                                             width=800, height=600)])
        assert event.media_hash == expected

    def test_media_only_message(self) -> None:
        """Вложение без текста: hash текста None, медиа несётся."""
        event = map_message(raw_message(text=None, media=(_media(),)), ACCOUNT)
        assert event.content_hash is None
        assert event.media_hash is not None
        assert len(event.media) == 1

    def test_raw_media_hash_matches_event(self) -> None:
        raw = raw_message(media=(_media(),))
        event = map_message(raw, ACCOUNT)
        assert raw_media_hash(raw) == event.media_hash

    def test_no_media_hash_none(self) -> None:
        raw = raw_message()
        assert raw_media_hash(raw) is None
        assert map_message(raw, ACCOUNT).media_hash is None


class TestMapMessageEdited:
    def test_edited_uses_content_hash_slot(self) -> None:
        raw = raw_message(kind=EventKind.MESSAGE_EDITED, text='новый')
        event = map_message(raw, ACCOUNT)
        assert event.kind is EventKind.MESSAGE_EDITED
        source_key = make_source_key(MESSENGER, ACCOUNT, ROOM)
        assert event.dedup_key == make_dedup_key(
            EventKind.MESSAGE_EDITED,
            source_key,
            '$evt-1',
            normalize_and_hash('новый'),
            None,
        )

    def test_edited_external_id_is_original(self) -> None:
        """Обёртка кладёт в event_id id оригинала — external_id совпадает."""
        raw = raw_message(kind=EventKind.MESSAGE_EDITED, event_id='$orig', text='x')
        assert map_message(raw, ACCOUNT).external_id == '$orig'


class TestMapRedaction:
    def test_core_fields(self) -> None:
        event = map_redaction(raw_deletion(), ACCOUNT)
        assert event.kind is EventKind.MESSAGE_DELETED
        assert event.external_id == '$evt-1'
        assert event.source.chat_id == ROOM
        assert event.source.thread_id is None
        assert event.received_by.account_id == ACCOUNT

    def test_dedup_key_del_slot(self) -> None:
        event = map_redaction(raw_deletion(redacts_event_id='$gone'), ACCOUNT)
        source_key = make_source_key(MESSENGER, ACCOUNT, ROOM)
        assert event.dedup_key == make_dedup_key(
            EventKind.MESSAGE_DELETED, source_key, '$gone'
        )

    def test_origin_catchup_propagates(self) -> None:
        event = map_redaction(raw_deletion(), ACCOUNT, origin='catchup')
        assert event.origin == 'catchup'
