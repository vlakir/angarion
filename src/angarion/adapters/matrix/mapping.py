"""
Маппинг сырых событий matrix-nio → ``Record`` (M7 B2, T010).

Чистые функции над нормализованными DTO (``client.RawMatrix*``): без сети
и без nio, покрыты юнит-тестами на фикстурах. Все ключи и хэши — только
публичными хелперами ``angarion.domain.keys`` (§7.2), без собственного
форматирования. Зеркало telegram ``mapping`` с поправкой на Matrix:

- Правка приходит как новое событие, но обёртка уже подставила в
  ``event_id`` id оригинала и ``kind=EDITED`` — ``external_id``
  совпадает с записью реестра, ``previous_text`` достаётся в ingest.
- Redaction адресует одно событие → один ``DELETED`` (не список).
  Маппится на **chat-уровне** (``thread_id=None``): redaction не несёт
  топика, как и Telegram-удаление (§9.4).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from angarion.adapters.matrix.client import TRANSPORT
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

if TYPE_CHECKING:
    from angarion.adapters.matrix.client import (
        RawMatrixDeletion,
        RawMatrixMedia,
        RawMatrixMessage,
    )

Origin = Literal['live', 'catchup']
"""Источник записи для ``Record.origin`` (live-sync или catch-up §9.3)."""


def _to_media_ref(raw: RawMatrixMedia) -> MediaRef:
    """
    Сырое вложение Matrix → доменный ``MediaRef`` (без логики ключей).

    ``ref`` = ``mxc://``-URI вложения — sender (B3) переотправляет по нему
    без скачивания, где homeserver позволяет reupload-by-reference.
    """
    return MediaRef(
        kind=raw.kind,
        ref=raw.ref,
        mime_type=raw.mime_type,
        file_name=raw.file_name,
        size=raw.size,
        width=raw.width,
        height=raw.height,
        duration=raw.duration,
    )


def raw_media_hash(raw: RawMatrixMessage) -> str | None:
    """
    media_hash сырого сообщения (для catch-up-сверки правок медиа, B3).

    Эквивалентен ``Record.media_hash`` после ``map_message`` того же
    события: ``make_media_hash`` игнорирует ``ref``.
    """
    return make_media_hash([_to_media_ref(m) for m in raw.media])


def map_message(
    raw: RawMatrixMessage, account_id: str, *, origin: Origin = 'live'
) -> Record:
    """
    NEW/EDITED-сообщение Matrix → ``Record``.

    ``origin`` — ``'live'`` для sync-подписки, ``'catchup'`` для дозабора
    (§9.3, B3); ключи/хэш одинаковы, поэтому дедуп гасит пересечения.
    """
    thread_id = raw.thread_id
    content_hash = normalize_and_hash(raw.text) if raw.text is not None else None
    media = [_to_media_ref(m) for m in raw.media]
    media_hash = make_media_hash(media)
    source_key = make_source_key(TRANSPORT, account_id, raw.room_id, thread_id)
    return Record(
        uid=uuid4(),
        kind=raw.kind,
        dedup_key=make_dedup_key(
            raw.kind, source_key, raw.event_id, content_hash, media_hash
        ),
        origin=origin,
        source=Endpoint(transport=TRANSPORT, address=raw.room_id, thread_id=thread_id),
        received_by=AccountRef(transport=TRANSPORT, account_id=account_id),
        external_id=raw.event_id,
        sender_id=raw.sender_id,
        sender_name=raw.sender_name,
        text=raw.text,
        reply_to_external_id=raw.reply_to_event_id,
        content_hash=content_hash,
        media_hash=media_hash,
        media=media,
        event_at=raw.event_at,
        received_at=datetime.now(UTC),
        raw=raw.model_dump(mode='json'),
    )


def map_redaction(
    raw: RawMatrixDeletion, account_id: str, *, origin: Origin = 'live'
) -> Record:
    """Redaction → один ``DELETED`` (chat-уровень, ``thread_id=None``)."""
    source_key = make_source_key(TRANSPORT, account_id, raw.room_id)
    return Record(
        uid=uuid4(),
        kind=RecordKind.DELETED,
        dedup_key=make_dedup_key(RecordKind.DELETED, source_key, raw.redacts_event_id),
        origin=origin,
        source=Endpoint(transport=TRANSPORT, address=raw.room_id),
        received_by=AccountRef(transport=TRANSPORT, account_id=account_id),
        external_id=raw.redacts_event_id,
        event_at=raw.redacted_at,
        received_at=datetime.now(UTC),
        raw=raw.model_dump(mode='json'),
    )
