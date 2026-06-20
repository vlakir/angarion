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
    make_internal_keys,
    make_media_hash,
    make_source_key,
    normalize_and_hash,
)
from angarion.domain.models import (
    AccountRef,
    Endpoint,
    MediaRef,
    Record,
    RecordKind,
)

SOURCE_KEY = 'telegram:acc1:-1003385167603'


class TestMakeSourceKey:
    def test_without_thread(self) -> None:
        key = make_source_key('telegram', 'acc1', '-1003385167603')
        assert key == SOURCE_KEY

    def test_with_thread(self) -> None:
        key = make_source_key('telegram', 'acc1', '-1003385167603', thread_id='55')
        assert key == 'telegram:acc1:-1003385167603:55'

    def test_matrix_room_id_no_thread_collision(self) -> None:
        """T043: room-id с ':' не должен схлопываться с парой address+thread.

        Старая colon-concat-кодировка давала ''matrix:acc:!room:server'' для
        обоих кортежей — коллизия реестра/курсоров разных источников.
        """
        with_colon_address = make_source_key('matrix', 'acc', '!room:server')
        split_into_thread = make_source_key('matrix', 'acc', '!room', thread_id='server')
        assert with_colon_address != split_into_thread

    def test_colon_in_address_is_injective(self) -> None:
        """Разные address с ':' дают разные ключи (где старо было неоднозначно)."""
        a = make_source_key('matrix', 'acc', '!a:b:c')
        b = make_source_key('matrix', 'acc', '!a:b', thread_id='c')
        c = make_source_key('matrix', 'acc', '!a', thread_id='b:c')
        assert len({a, b, c}) == 3

    def test_escape_char_in_component_is_injective(self) -> None:
        r"""Сам escape-символ '\' в компоненте не ломает инъективность."""
        a = make_source_key('matrix', 'acc', r'x\:y')
        b = make_source_key('matrix', 'acc', 'x', thread_id='y')
        assert a != b

    def test_colon_free_components_keep_legacy_format(self) -> None:
        """Byte-compat: без ':'/'\\' кодировка совпадает со старой colon-concat."""
        assert make_source_key('telegram', 'acc1', '-100') == 'telegram:acc1:-100'
        assert (
            make_source_key('telegram', 'acc1', '-100', thread_id='55')
            == 'telegram:acc1:-100:55'
        )


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
        key = make_dedup_key(RecordKind.NEW, SOURCE_KEY, '42')
        assert key == f'{SOURCE_KEY}:42:new'

    def test_deleted(self) -> None:
        key = make_dedup_key(RecordKind.DELETED, SOURCE_KEY, '42')
        assert key == f'{SOURCE_KEY}:42:del'

    def test_edited_includes_content_hash(self) -> None:
        content_hash = normalize_and_hash('v2')
        key = make_dedup_key(
            RecordKind.EDITED, SOURCE_KEY, '42', content_hash=content_hash
        )
        assert key == f'{SOURCE_KEY}:42:edit:{content_hash}'

    def test_edited_requires_content_hash(self) -> None:
        with pytest.raises(ValueError, match='content_hash'):
            make_dedup_key(RecordKind.EDITED, SOURCE_KEY, '42')

    def test_edit_back_to_seen_text_is_duplicate(self) -> None:
        """§7.2: правка, вернувшая прежний текст, даёт тот же ключ (дубль)."""
        key_v1 = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('A'),
        )
        key_v2 = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('B'),
        )
        key_v3_back = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('A'),
        )
        assert key_v1 != key_v2
        assert key_v3_back == key_v1

    def test_kinds_do_not_collide(self) -> None:
        """new/del/edit одного сообщения — три разных ключа."""
        new = make_dedup_key(RecordKind.NEW, SOURCE_KEY, '42')
        deleted = make_dedup_key(RecordKind.DELETED, SOURCE_KEY, '42')
        edited = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=normalize_and_hash('x'),
        )
        assert len({new, deleted, edited}) == 3

    def test_edited_text_only_key_unchanged_by_media_param(self) -> None:
        """§7.2 (M7): media_hash=None — ключ EDITED байт-в-байт прежний."""
        content_hash = normalize_and_hash('v2')
        assert make_dedup_key(
            RecordKind.EDITED, SOURCE_KEY, '42', content_hash=content_hash
        ) == f'{SOURCE_KEY}:42:edit:{content_hash}'

    def test_edited_media_only_does_not_raise(self) -> None:
        """M7 must-fix: медиа-only правка (нет content_hash) — ключ по media."""
        media_hash = make_media_hash([MediaRef(kind='photo', size=10)])
        key = make_dedup_key(
            RecordKind.EDITED, SOURCE_KEY, '42', media_hash=media_hash
        )
        assert key == f'{SOURCE_KEY}:42:edit:media:{media_hash}'

    def test_edited_media_swap_changes_key_with_same_text(self) -> None:
        """Q5: подмена вложения при том же тексте → другой ключ EDITED."""
        ch = normalize_and_hash('подпись')
        key_a = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=ch,
            media_hash=make_media_hash([MediaRef(kind='photo', size=10)]),
        )
        key_b = make_dedup_key(
            RecordKind.EDITED,
            SOURCE_KEY,
            '42',
            content_hash=ch,
            media_hash=make_media_hash([MediaRef(kind='photo', size=20)]),
        )
        assert key_a != key_b

    def test_edited_requires_text_or_media(self) -> None:
        """Ни текста, ни медиа — опознавать нечего, как и прежде → raise."""
        with pytest.raises(ValueError, match='content_hash'):
            make_dedup_key(RecordKind.EDITED, SOURCE_KEY, '42')

    def test_colon_in_external_id_is_injective(self) -> None:
        """T043: external_id с ':' (Matrix event-id старых версий комнат)
        не должен схлопывать разные сообщения в один dedup-ключ."""
        a = make_dedup_key(RecordKind.NEW, SOURCE_KEY, '$evt:server')
        b = make_dedup_key(RecordKind.NEW, SOURCE_KEY, '$evt')
        c = make_dedup_key(RecordKind.NEW, f'{SOURCE_KEY}:$evt', 'server')
        assert len({a, b, c}) == 3


class TestMakeMediaHash:
    def test_empty_is_none(self) -> None:
        assert make_media_hash([]) is None

    def test_deterministic(self) -> None:
        media = [MediaRef(kind='photo', size=10, mime_type='image/jpeg')]
        assert make_media_hash(media) == make_media_hash(list(media))

    def test_changes_on_metadata(self) -> None:
        assert make_media_hash([MediaRef(kind='photo', size=10)]) != make_media_hash(
            [MediaRef(kind='photo', size=11)]
        )

    def test_ignores_ref_and_local_path(self) -> None:
        """ref постоянен в пределах сообщения, local_path — доставка: не в хэше."""
        base = MediaRef(kind='photo', size=10)
        with_ref = MediaRef(kind='photo', size=10, ref='-100:42', local_path='/tmp/x')
        assert make_media_hash([base]) == make_media_hash([with_ref])


def _event(dedup_key: str) -> Record:
    now = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
    return Record(
        uid=UUID('00000000-0000-0000-0000-000000000001'),
        kind=RecordKind.NEW,
        dedup_key=dedup_key,
        origin='live',
        source=Endpoint(transport='telegram', address='-100123'),
        received_by=AccountRef(transport='telegram', account_id='acc1'),
        external_id='42',
        event_at=now,
        received_at=now,
    )


class TestMakeIdempotencyKey:
    def test_golden(self) -> None:
        dedup_key = f'{SOURCE_KEY}:42:new'
        event = _event(dedup_key)
        target = Endpoint(transport='telegram', address='-100999')
        key = make_idempotency_key('digest', event, target, 0)
        assert key == f'{dedup_key}->digest:-100999:0'

    def test_pipelines_do_not_suppress_each_other(self) -> None:
        """§7.3: multicast в одну группу — разные пайплайны, разные ключи."""
        event = _event(f'{SOURCE_KEY}:42:new')
        target = Endpoint(transport='telegram', address='-100999')
        key_a = make_idempotency_key('pipe_a', event, target, 0)
        key_b = make_idempotency_key('pipe_b', event, target, 0)
        assert key_a != key_b

    def test_partial_application_matches_processor_services_shape(self) -> None:
        """A-9: worker частично применяет фабрику со своим pipeline."""
        event = _event(f'{SOURCE_KEY}:42:new')
        target = Endpoint(transport='telegram', address='-100999')
        factory = functools.partial(make_idempotency_key, 'digest')
        assert factory(event, target, 1) == make_idempotency_key(
            'digest', event, target, 1
        )

    def test_colon_in_target_address_is_injective(self) -> None:
        """T043: target.address с ':' (Matrix-комната) не должен схлопывать
        разные адреса доставки в один ключ идемпотентности."""
        event = _event(f'{SOURCE_KEY}:42:new')
        a = make_idempotency_key('digest', event, Endpoint(transport='matrix', address='!r:srv'), 0)
        b = make_idempotency_key('digest', event, Endpoint(transport='matrix', address='!r'), 0)
        assert a != b

    def test_colon_in_pipeline_is_injective(self) -> None:
        """Имя пайплайна с ':' не должно сливаться с адресом цели."""
        event = _event(f'{SOURCE_KEY}:42:new')
        target = Endpoint(transport='telegram', address='-100999')
        a = make_idempotency_key('a:b', event, target, 0)
        b = make_idempotency_key('a', event, Endpoint(transport='telegram', address='b:-100999'), 0)
        assert a != b


class TestMakeInternalKeys:
    """Деривация ключей внутренней re-ingested записи из idempotency_key (T037)."""

    WIRE_KEY = 'internal:wire:stage1'

    def test_external_id_is_idempotency_key(self) -> None:
        """external_id внутренней записи — сам idempotency_key исходящего."""
        idem = 'telegram:acc1:-100:42:new->norm:stage1:0'
        external_id, _ = make_internal_keys(idem, self.WIRE_KEY)
        assert external_id == idem

    def test_dedup_key_is_new_dedup_of_external_id(self) -> None:
        """dedup_key строится общим make_dedup_key(NEW, source_key, external_id)."""
        idem = 'telegram:acc1:-100:42:new->norm:stage1:0'
        external_id, dedup_key = make_internal_keys(idem, self.WIRE_KEY)
        assert dedup_key == make_dedup_key(RecordKind.NEW, self.WIRE_KEY, external_id)

    def test_deterministic(self) -> None:
        """Повтор доставки ребра (тот же idempotency_key) → те же ключи (Q3)."""
        idem = 'telegram:acc1:-100:42:new->norm:stage1:0'
        assert make_internal_keys(idem, self.WIRE_KEY) == make_internal_keys(
            idem, self.WIRE_KEY
        )

    def test_injective_in_idempotency_key(self) -> None:
        """Разные idempotency_key → разные external_id и разные dedup_key."""
        a = make_internal_keys('k1->norm:stage1:0', self.WIRE_KEY)
        b = make_internal_keys('k2->norm:stage1:0', self.WIRE_KEY)
        assert a[0] != b[0]
        assert a[1] != b[1]

    def test_colon_in_idempotency_key_stays_injective(self) -> None:
        """idempotency_key несёт ':'; escape make_dedup_key держит инъективность."""
        a_ext, a_dedup = make_internal_keys('a:b->p:c:0', self.WIRE_KEY)
        b_ext, b_dedup = make_internal_keys('a->p:b:c:0', self.WIRE_KEY)
        assert a_ext != b_ext
        assert a_dedup != b_dedup

    def test_different_channels_do_not_collide(self) -> None:
        """Один idempotency_key на разных каналах (fan-out) → разные dedup_key."""
        idem = 'telegram:acc1:-100:42:new->norm:stage1:0'
        _, dedup_a = make_internal_keys(idem, 'internal:wire:stage1')
        _, dedup_b = make_internal_keys(idem, 'internal:wire:stage2')
        assert dedup_a != dedup_b
