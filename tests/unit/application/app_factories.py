"""
Фабрики тестовых объектов для тестов application-слоя.

Имя модуля уникально в пределах tests/ (контрактный `factories`
уже занят): тестовые каталоги — не пакеты, pytest кладёт их в
sys.path плоско.
"""

from __future__ import annotations

from datetime import UTC, datetime
from functools import partial
from uuid import uuid4

import structlog

from angarion.adapters.memory.storage import MemoryStateStore
from angarion.application.worker import ScopedStateStore
from angarion.domain.keys import (
    make_dedup_key,
    make_idempotency_key,
    make_source_key,
    normalize_and_hash,
)
from angarion.domain.models import (
    AccountRef,
    Address,
    EventKind,
    InboundEvent,
    OutboundMessage,
    PipelineContextData,
    ProcessorServices,
    QueueEnvelope,
    TargetSpec,
)

NOW = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)

MESSENGER = 'memory'
ACCOUNT = AccountRef(messenger=MESSENGER, account_id='acc1')
SOURCE_CHAT = '-100123'
SOURCE_KEY = make_source_key(MESSENGER, ACCOUNT.account_id, SOURCE_CHAT)


def make_address(**overrides: object) -> Address:
    fields: dict[str, object] = {'messenger': MESSENGER, 'chat_id': SOURCE_CHAT}
    fields.update(overrides)
    return Address.model_validate(fields)


def make_event(
    kind: EventKind = EventKind.MESSAGE_NEW,
    external_id: str = '42',
    text: str | None = 'hello',
    event_at: datetime = NOW,
    **overrides: object,
) -> InboundEvent:
    """Событие с согласованными dedup_key/content_hash от kind/id/text."""
    content_hash = normalize_and_hash(text) if text is not None else None
    fields: dict[str, object] = {
        'uid': uuid4(),
        'kind': kind,
        'dedup_key': make_dedup_key(kind, SOURCE_KEY, external_id, content_hash),
        'origin': 'live',
        'source': make_address(),
        'received_by': ACCOUNT,
        'external_id': external_id,
        'text': text,
        'content_hash': content_hash,
        'event_at': event_at,
        'received_at': event_at,
    }
    fields.update(overrides)
    return InboundEvent.model_validate(fields)


def make_envelope(**overrides: object) -> QueueEnvelope:
    fields: dict[str, object] = {'pipeline': 'digest', 'event': make_event()}
    fields.update(overrides)
    return QueueEnvelope.model_validate(fields)


def make_target(chat_id: str = '-100999') -> TargetSpec:
    return TargetSpec(target=make_address(chat_id=chat_id), send_via=ACCOUNT)


def make_context(
    pipeline: str = 'digest', targets: list[TargetSpec] | None = None
) -> PipelineContextData:
    if targets is None:
        targets = [make_target()]
    return PipelineContextData(pipeline=pipeline, targets=targets)


def make_outbound(**overrides: object) -> OutboundMessage:
    fields: dict[str, object] = {
        'idempotency_key': f'{SOURCE_KEY}:42:new->digest:-100999:0',
        'target': make_address(chat_id='-100999'),
        'send_via': ACCOUNT,
        'text': 'hi',
    }
    fields.update(overrides)
    return OutboundMessage.model_validate(fields)


def make_services(pipeline: str = 'digest') -> ProcessorServices:
    return ProcessorServices(
        log=structlog.get_logger('test'),
        state=ScopedStateStore(MemoryStateStore(), pipeline),
        make_idempotency_key=partial(make_idempotency_key, pipeline),
    )
