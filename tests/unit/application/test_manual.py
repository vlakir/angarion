"""
Фабрика ручного триггера (T038, фаза 1): payload → ``Record``.

Проверяем: ``origin='manual'``, согласованные ключи (source/dedup),
идемпотентность (клиентский ключ → детерминированный ``dedup_key``; без
ключа → свежий ``uid`` на каждый вызов), дефолты времени и аккаунта.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from angarion.application.manual import MANUAL_ACCOUNT, ManualEvent, build_manual_record
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import Endpoint, MediaRef, RecordKind

NOW = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)


def make_event(**overrides: object) -> ManualEvent:
    fields: dict[str, object] = {
        'source': Endpoint(transport='memory', address='-100123'),
        'text': 'привет',
    }
    fields.update(overrides)
    return ManualEvent.model_validate(fields)


class TestBuildManualRecord:
    def test_origin_is_manual(self) -> None:
        record = build_manual_record(make_event(), now=NOW)
        assert record.origin == 'manual'

    def test_source_preserved_received_by_synthetic_account(self) -> None:
        record = build_manual_record(make_event(), now=NOW)
        assert record.source == Endpoint(transport='memory', address='-100123')
        assert record.received_by.transport == 'memory'
        assert record.received_by.account_id == MANUAL_ACCOUNT

    def test_custom_account_id_flows_into_source_key(self) -> None:
        record = build_manual_record(make_event(account_id='main'), now=NOW)
        expected = make_dedup_key(
            RecordKind.NEW,
            make_source_key('memory', 'main', '-100123'),
            record.external_id,
            normalize_and_hash('привет'),
        )
        assert record.dedup_key == expected

    def test_content_hash_from_text(self) -> None:
        record = build_manual_record(make_event(text='hi'), now=NOW)
        assert record.content_hash == normalize_and_hash('hi')

    def test_no_text_no_content_hash(self) -> None:
        record = build_manual_record(make_event(text=None), now=NOW)
        assert record.content_hash is None

    def test_media_hash_and_media_copied(self) -> None:
        media = [MediaRef(kind='photo', ref='abc')]
        record = build_manual_record(make_event(media=media), now=NOW)
        assert record.media == media
        assert record.media_hash is not None
        assert record.has_media

    def test_event_at_defaults_to_received_at(self) -> None:
        record = build_manual_record(make_event(), now=NOW)
        assert record.event_at == NOW
        assert record.received_at == NOW

    def test_explicit_event_at_preserved(self) -> None:
        past = datetime(2026, 1, 1, tzinfo=UTC)
        record = build_manual_record(make_event(event_at=past), now=NOW)
        assert record.event_at == past
        assert record.received_at == NOW

    def test_sender_fields_propagated(self) -> None:
        record = build_manual_record(
            make_event(sender_id='42', sender_name='Alice'), now=NOW
        )
        assert record.sender_id == '42'
        assert record.sender_name == 'Alice'

    def test_trace_id_roots_to_uid(self) -> None:
        record = build_manual_record(make_event(), now=NOW)
        assert record.trace_id == str(record.uid)

    def test_without_idempotency_key_external_id_is_uid(self) -> None:
        record = build_manual_record(make_event(), now=NOW)
        assert record.external_id == str(record.uid)

    def test_without_idempotency_key_each_call_unique(self) -> None:
        a = build_manual_record(make_event(), now=NOW)
        b = build_manual_record(make_event(), now=NOW)
        assert a.dedup_key != b.dedup_key
        assert a.uid != b.uid

    def test_idempotency_key_is_deterministic_dedup(self) -> None:
        a = build_manual_record(make_event(idempotency_key='cli-1'), now=NOW)
        b = build_manual_record(make_event(idempotency_key='cli-1'), now=NOW)
        assert a.external_id == 'cli-1'
        assert a.dedup_key == b.dedup_key

    def test_kind_edited_with_text_builds_valid_key(self) -> None:
        record = build_manual_record(
            make_event(kind=RecordKind.EDITED, text='новый'), now=NOW
        )
        assert record.kind is RecordKind.EDITED
        assert ':edit:' in record.dedup_key

    def test_now_defaults_to_wall_clock(self) -> None:
        before = datetime.now(UTC)
        record = build_manual_record(make_event())
        after = datetime.now(UTC)
        assert before <= record.received_at <= after

    def test_extra_field_forbidden(self) -> None:
        with pytest.raises(ValueError, match='extra'):
            ManualEvent.model_validate(
                {'source': Endpoint(transport='memory', address='x'), 'bogus': 1}
            )
