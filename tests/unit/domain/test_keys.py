"""Golden-тесты публичных хелперов ключей (§7.2–7.3, SC-6).

Правила нормализации и форматы ключей — публичный контракт,
меняемый только мажорной версией. Изменение golden-значений —
осознанное ломающее изменение.
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime
from uuid import UUID

import pytest

from angarion.domain.keys import (
    make_dedup_key,
    make_idempotency_key,
    make_source_key,
    normalize_and_hash,
)
from angarion.domain.models import Address, AccountRef, EventKind, InboundEvent

SOURCE_KEY = 'telegram:acc1:-1003385167603'


class TestMakeSourceKey:
    def test_without_thread(self) -> None:
        key = make_source_key('telegram', 'acc1', '-1003385167603')
        assert key == SOURCE_KEY

    def test_with_thread(self) -> None:
        key = make_source_key('telegram', 'acc1', '-1003385167603', thread_id='55')
        assert key == 'telegram:acc1:-1003385167603:55'


class TestNormalizeAndHash:
    """Контракт нормализации: NFC + переводы строк → \\n, больше ничего."""

    def test_golden_plain(self) -> None:
        expected = '2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824'
        assert normalize_and_hash('hello') == expected

    def test_newlines_normalized(self) -> None:
        expected = '7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78'
        assert normalize_and_hash('a\nb') == expected
        assert normalize_and_hash('a\r\nb') == expected
        assert normalize_and_hash('a\rb') == expected

    def test_unicode_nfc(self) -> None:
        composed = 'café'
        decomposed = 'café'
        expected = '850f7dc43910ff890f8879c0ed26fe697c93a067ad93a7d50f466a7028a9bf4e'
        assert normalize_and_hash(composed) == expected
        assert normalize_and_hash(decomposed) == expected

    def test_no_trim(self) -> None:
        assert normalize_and_hash(' hello') != normalize_and_hash('hello')

    def test_no_lowercasing(self) -> None:
        assert normalize_and_hash('Hello') != normalize_and_hash('hello')

    def test_empty_string_is_hashable(self) -> None:
        assert normalize_and_hash('') == (
            'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        )


class TestMakeDedupKey:
    def test_new(self) -> None:
        key = make_dedup_key(EventKind.MESSAGE_NEW, SOURCE_KEY, '42')
        assert key == f'{SOURCE_KEY}:42:new'

    def test_deleted(self) -> None:
        key = make_dedup_key(EventKind.MESSAGE_DELETED, SOURCE_KEY, '42')
        assert key == f'{SOURCE_KEY}:42:del'

    def test_edited_includes_content_hash(self) -> None:
        content_hash = normalize_and_hash('v2')
        key = make_dedup_key(
            EventKind.MESSAGE_EDITED, SOURCE_KEY, '42', content_hash=content_hash
        )
        assert key == f'{SOURCE_KEY}:42:edit:{content_hash}'

    def test_edited_requires_content_hash(self) -> None:
        with pytest.raises(ValueError, match='content_hash'):
            make_dedup_key(EventKind.MESSAGE_EDITED, SOURCE_KEY, '42')

    def test_edit_back_to_seen_text_is_duplicate(self) -> None:
        """§7.2: правка, вернувшая прежний текст, даёт тот же ключ (дубль)."""
        key_v1 = make_dedup_key(
            EventKind.MESSAGE_EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('A'),
        )
        key_v2 = make_dedup_key(
            EventKind.MESSAGE_EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('B'),
        )
        key_v3_back = make_dedup_key(
            EventKind.MESSAGE_EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('A'),
        )
        assert key_v1 != key_v2
        assert key_v3_back == key_v1

    def test_kinds_do_not_collide(self) -> None:
        """new/del/edit одного сообщения — три разных ключа."""
        new = make_dedup_key(EventKind.MESSAGE_NEW, SOURCE_KEY, '42')
        deleted = make_dedup_key(EventKind.MESSAGE_DELETED, SOURCE_KEY, '42')
        edited = make_dedup_key(
            EventKind.MESSAGE_EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('x'),
        )
        assert len({new, deleted, edited}) == 3


def _event(dedup_key: str) -> InboundEvent:
    now = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
    return InboundEvent(
        uid=UUID('00000000-0000-0000-0000-000000000001'),
        kind=EventKind.MESSAGE_NEW,
        dedup_key=dedup_key,
        origin='live',
        source=Address(messenger='telegram', chat_id='-100123'),
        received_by=AccountRef(messenger='telegram', account_id='acc1'),
        external_id='42',
        event_at=now,
        received_at=now,
    )


class TestMakeIdempotencyKey:
    def test_golden(self) -> None:
        dedup_key = f'{SOURCE_KEY}:42:new'
        event = _event(dedup_key)
        target = Address(messenger='telegram', chat_id='-100999')
        key = make_idempotency_key('digest', event, target, 0)
        assert key == f'{dedup_key}->digest:-100999:0'

    def test_pipelines_do_not_suppress_each_other(self) -> None:
        """§7.3: multicast в одну группу — разные пайплайны, разные ключи."""
        event = _event(f'{SOURCE_KEY}:42:new')
        target = Address(messenger='telegram', chat_id='-100999')
        key_a = make_idempotency_key('pipe_a', event, target, 0)
        key_b = make_idempotency_key('pipe_b', event, target, 0)
        assert key_a != key_b

    def test_partial_application_matches_processor_services_shape(self) -> None:
        """A-9: worker частично применяет фабрику со своим pipeline."""
        event = _event(f'{SOURCE_KEY}:42:new')
        target = Address(messenger='telegram', chat_id='-100999')
        factory = functools.partial(make_idempotency_key, 'digest')
        assert factory(event, target, 1) == make_idempotency_key(
            'digest', event, target, 1
        )
