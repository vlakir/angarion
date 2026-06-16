"""
Маппинг сырых событий Telethon → ``InboundEvent`` (FR «Маппинг», M3).

Чистые функции над нормализованными DTO (`client.RawTelegram*`): без
сети и без Telethon, покрыты юнит-тестами на фикстурах. Все ключи и
хэши — только публичными хелперами ``angarion.domain.keys`` (§7.2), без
собственного форматирования.

Удаления маппятся на **chat-уровне** (``thread_id=None``): update
удаления Telegram не несёт топика (§9.4) — топик-удаления live не
детектируются (router отфильтрует как unrouted для топик-пайплайнов).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from angarion.adapters.telegram.client import MESSENGER
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import (
    AccountRef,
    Address,
    EventKind,
    InboundEvent,
    MediaRef,
)

if TYPE_CHECKING:
    from angarion.adapters.telegram.client import (
        RawMedia,
        RawTelegramDeletion,
        RawTelegramMessage,
    )

Origin = Literal['live', 'catchup']
"""Источник события для ``InboundEvent.origin`` (live-подписка или catch-up §9.3)."""


def _str_or_none(value: int | None) -> str | None:
    return str(value) if value is not None else None


def _to_media_ref(raw: RawMedia) -> MediaRef:
    """Сырое вложение Telethon → доменный ``MediaRef`` (без логики ключей)."""
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


def map_message(
    raw: RawTelegramMessage, account_id: str, *, origin: Origin = 'live'
) -> InboundEvent | None:
    """
    NEW/EDITED-сообщение → ``InboundEvent``; ``None`` — системное
    (``MessageService``), отсеивается на входе адаптера.

    ``origin`` — ``'live'`` для подписки, ``'catchup'`` для дозабора
    (§9.3); ключи/хэш одинаковы, поэтому дедуп гасит пересечения.
    """
    if raw.is_service:
        return None
    chat_id = str(raw.chat_id)
    thread_id = _str_or_none(raw.thread_id)
    external_id = str(raw.message_id)
    content_hash = normalize_and_hash(raw.text) if raw.text is not None else None
    source_key = make_source_key(MESSENGER, account_id, chat_id, thread_id)
    return InboundEvent(
        uid=uuid4(),
        kind=raw.kind,
        dedup_key=make_dedup_key(raw.kind, source_key, external_id, content_hash),
        origin=origin,
        source=Address(messenger=MESSENGER, chat_id=chat_id, thread_id=thread_id),
        received_by=AccountRef(messenger=MESSENGER, account_id=account_id),
        external_id=external_id,
        sender_id=_str_or_none(raw.sender_id),
        sender_name=raw.sender_name,
        text=raw.text,
        reply_to_external_id=_str_or_none(raw.reply_to_message_id),
        content_hash=content_hash,
        media=[_to_media_ref(m) for m in raw.media],
        event_at=raw.event_at,
        received_at=datetime.now(UTC),
        raw=raw.model_dump(mode='json'),
    )


def map_deletion(
    raw: RawTelegramDeletion, account_id: str, *, origin: Origin = 'live'
) -> list[InboundEvent]:
    """
    Удаление → по одному ``MESSAGE_DELETED`` на id (chat-уровень).

    ``chat_id=None`` (legacy-группа, §9.4.2) → пустой список: без chat
    источник не собрать; целевой кейс — супергруппы. ``origin`` — см.
    ``map_message`` (catch-up §9.3 переиспользует тот же маппинг).
    """
    if raw.chat_id is None:
        return []
    chat_id = str(raw.chat_id)
    source_key = make_source_key(MESSENGER, account_id, chat_id)
    received_at = datetime.now(UTC)
    return [
        InboundEvent(
            uid=uuid4(),
            kind=EventKind.MESSAGE_DELETED,
            dedup_key=make_dedup_key(
                EventKind.MESSAGE_DELETED, source_key, str(message_id)
            ),
            origin=origin,
            source=Address(messenger=MESSENGER, chat_id=chat_id),
            received_by=AccountRef(messenger=MESSENGER, account_id=account_id),
            external_id=str(message_id),
            event_at=raw.deleted_at,
            received_at=received_at,
            raw=raw.model_dump(mode='json'),
        )
        for message_id in raw.message_ids
    ]
