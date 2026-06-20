"""
Фабрики тестовых объектов для контрактных наборов портов.

Наборы пишутся переиспользуемыми (FR-6): в M2 они лягут в основу
публичного пакета ``angarion.testing`` и будут натравлены на
персистентные реализации.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from angarion.domain.models import (
    AccountRef,
    AnalyticsEvent,
    DeadLetter,
    Endpoint,
    OutboundRecord,
    QueueEnvelope,
    Record,
    RecordKind,
    RegistryRecord,
    SourceCursor,
)

NOW = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
LONG_AGO = datetime(2000, 1, 1, tzinfo=UTC)
FAR_FUTURE = datetime(2100, 1, 1, tzinfo=UTC)

SOURCE_KEY = 'memory:acc1:-100123'


def make_endpoint(**overrides: object) -> Endpoint:
    fields: dict[str, object] = {'transport': 'memory', 'address': '-100123'}
    fields.update(overrides)
    return Endpoint.model_validate(fields)


def make_record(**overrides: object) -> Record:
    fields: dict[str, object] = {
        'uid': uuid4(),
        'kind': RecordKind.NEW,
        'dedup_key': f'{SOURCE_KEY}:42:new',
        'origin': 'live',
        'source': make_endpoint(),
        'received_by': AccountRef(transport='memory', account_id='acc1'),
        'external_id': '42',
        'text': 'hello',
        'event_at': NOW,
        'received_at': NOW,
    }
    fields.update(overrides)
    return Record.model_validate(fields)


def make_envelope(**overrides: object) -> QueueEnvelope:
    fields: dict[str, object] = {'pipeline': 'digest', 'record': make_record()}
    fields.update(overrides)
    return QueueEnvelope.model_validate(fields)


def make_registry_record(**overrides: object) -> RegistryRecord:
    fields: dict[str, object] = {
        'source_key': SOURCE_KEY,
        'external_id': '42',
        'text': 'hello',
        'content_hash': 'hash-a',
        'event_at': NOW,
    }
    fields.update(overrides)
    return RegistryRecord.model_validate(fields)


def make_cursor(**overrides: object) -> SourceCursor:
    fields: dict[str, object] = {
        'source_key': SOURCE_KEY,
        'payload': {'last_seen_external_id': '42'},
        'updated_at': NOW,
    }
    fields.update(overrides)
    return SourceCursor.model_validate(fields)


def make_outbound(**overrides: object) -> OutboundRecord:
    fields: dict[str, object] = {
        'idempotency_key': f'{SOURCE_KEY}:42:new->digest:-100999:0',
        'target': make_endpoint(address='-100999'),
        'send_via': AccountRef(transport='memory', account_id='acc1'),
        'text': 'hi',
    }
    fields.update(overrides)
    return OutboundRecord.model_validate(fields)


def make_analytics_event(**overrides: object) -> AnalyticsEvent:
    fields: dict[str, object] = {'uid': uuid4(), 'kind': 'ingested', 'at': NOW}
    fields.update(overrides)
    return AnalyticsEvent.model_validate(fields)


def make_dead_letter(**overrides: object) -> DeadLetter:
    fields: dict[str, object] = {
        'uid': uuid4(),
        'envelope': make_envelope(attempt=5),
        'error': 'ProcessingError: boom',
        'failed_at': NOW,
    }
    fields.update(overrides)
    return DeadLetter.model_validate(fields)
