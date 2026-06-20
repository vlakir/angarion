"""
Ручной триггер (T038): публичная фабрика payload → ``Record``.

Штатный «запусти сейчас / впрысни событие» обоих API (программного и web):
пользователь даёт упрощённый :class:`ManualEvent` (текст + источник), фабрика
разворачивает его в валидный ``Record`` (``origin='manual'``) с согласованными
ключами идемпотентности — без ручной сборки ``uid``/``dedup_key``/``source_key``.

Место — ``application``, не ``domain`` (A5 спеки): сборка тянет ``received_at =
now()`` (нечистая, часы) — оркестрация, а не чистый доменный хелпер. Часы
инъектируемы (``now=``) для детерминизма тестов, дефолт — ``datetime.now(UTC)``.

Идемпотентность (FR §3): клиентский ``idempotency_key`` → ``external_id`` →
детерминированный ``dedup_key`` (ретрай гасит штатный ``dedup.seen()`` на
event-пути); без ключа → ``external_id = str(uid)``, каждый вызов уникален.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final
from uuid import uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from angarion.domain.keys import (
    make_dedup_key,
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

MANUAL_ACCOUNT: Final = 'manual'
"""Синтетический ``account_id`` ручного события по умолчанию.

Маршрутизация зависит только от ``source`` (transport+address+thread_id), не от
аккаунта, поэтому ручное событие доезжает до пайплайна с любым ``account_id``.
Аккаунт влияет лишь на namespace ключей (``source_key``/``dedup_key``):
по умолчанию ручные события живут в отдельном от live namespace, при желании
совместить с live-состоянием — указать реальный ``account_id`` источника.
"""


class ManualEvent(BaseModel):
    """
    Упрощённый payload ручного события (T038, §5 спеки).

    Минимум — ``source`` + ``text``; ``kind=new`` по умолчанию. ``account_id``
    синтетический (см. :data:`MANUAL_ACCOUNT`). ``idempotency_key`` опционален —
    с ним повтор гасится dedup на event-пути. ``frozen``/``extra='forbid'`` —
    как доменные DTO: payload сериализуем в JSON без сюрпризов (web-путь).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    source: Endpoint
    text: str | None = None
    kind: RecordKind = RecordKind.NEW
    account_id: str = MANUAL_ACCOUNT
    sender_id: str | None = None
    sender_name: str | None = None
    media: list[MediaRef] = Field(default_factory=list)
    event_at: AwareDatetime | None = None
    idempotency_key: str | None = None


def build_manual_record(event: ManualEvent, *, now: datetime | None = None) -> Record:
    """
    Развернуть :class:`ManualEvent` в валидный ``Record`` (``origin='manual'``).

    ``now`` инъектируема (дефолт ``datetime.now(UTC)``) — детерминизм тестов.
    ``event_at`` берётся из payload либо равен ``received_at``. Ключи строятся
    чистыми ``domain/keys`` хелперами; для ``kind=edited`` без текста и медиа
    ``make_dedup_key`` поднимет ``ValueError`` (нечего опознавать, A3).
    """
    received_at = now if now is not None else datetime.now(UTC)
    event_at = event.event_at if event.event_at is not None else received_at
    source_key = make_source_key(
        event.source.transport,
        event.account_id,
        event.source.address,
        event.source.thread_id,
    )
    content_hash = normalize_and_hash(event.text) if event.text else None
    media_hash = make_media_hash(event.media)
    uid = uuid4()
    external_id = (
        event.idempotency_key if event.idempotency_key is not None else str(uid)
    )
    dedup_key = make_dedup_key(
        event.kind, source_key, external_id, content_hash, media_hash
    )
    return Record(
        uid=uid,
        kind=event.kind,
        dedup_key=dedup_key,
        origin='manual',
        source=event.source,
        received_by=AccountRef(
            transport=event.source.transport, account_id=event.account_id
        ),
        external_id=external_id,
        sender_id=event.sender_id,
        sender_name=event.sender_name,
        text=event.text,
        content_hash=content_hash,
        media=list(event.media),
        media_hash=media_hash,
        event_at=event_at,
        received_at=received_at,
    )
