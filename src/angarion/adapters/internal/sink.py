"""
InternalSink (``SinkPort``, T037): re-ingestion внутреннего провода.

Вместо доставки наружу sink преобразует ``OutboundRecord`` пайплайна-источника
в ``Record(kind=new, source=(internal, канал))`` и подаёт его обратно в
``IngestService`` — выход одного пайплайна становится входом другого, минуя
реальную платформу. Ключи стыка детерминированы из ``idempotency_key`` (Q3):
повтор доставки внутреннего ребра (ретрай outbox в окне send→mark_sent) гасит
штатный ``dedup.seen()`` на шаге 1 ingest — у приёмника ровно одна запись
(at-least-once без дублей).

Модуль без ``from __future__ import annotations``: единообразно с прочими
sink-модулями адаптеров.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from angarion.domain.keys import (
    make_internal_keys,
    make_media_hash,
    make_source_key,
    normalize_and_hash,
)
from angarion.domain.models import (
    INTERNAL_TRANSPORT,
    AccountRef,
    DeliveryReceipt,
    Endpoint,
    Record,
    RecordKind,
)

if TYPE_CHECKING:
    from angarion.application.ingest import IngestService
    from angarion.domain.models import OutboundRecord


class InternalSink:
    """``SinkPort``, замкнутый на ``IngestService``: re-ingestion (T037)."""

    def __init__(self, *, ingest: IngestService) -> None:
        self._ingest = ingest

    async def send(self, record: OutboundRecord) -> DeliveryReceipt:
        """
        Преобразовать исходящее в входную запись приёмника и подать в ingest.

        ``source`` — внутренний канал цели (``target``), ``received_by`` —
        внутренний аккаунт отправки (``send_via``). ``trace_id`` наследуется от
        корня цепочки (worker штампует его на ``OutboundRecord``); ``hops``
        инкрементируется — рантайм-backstop циклов проверяет лимит в ingest.
        """
        source_key = make_source_key(
            INTERNAL_TRANSPORT,
            record.send_via.account_id,
            record.target.address,
            record.target.thread_id,
        )
        external_id, dedup_key = make_internal_keys(record.idempotency_key, source_key)
        content_hash = normalize_and_hash(record.text) if record.text else None
        now = datetime.now(UTC)
        reingested = Record(
            uid=uuid4(),
            kind=RecordKind.NEW,
            dedup_key=dedup_key,
            origin='internal',
            source=Endpoint(
                transport=INTERNAL_TRANSPORT,
                address=record.target.address,
                thread_id=record.target.thread_id,
            ),
            received_by=AccountRef(
                transport=INTERNAL_TRANSPORT, account_id=record.send_via.account_id
            ),
            external_id=external_id,
            text=record.text,
            content_hash=content_hash,
            media=list(record.media),
            media_hash=make_media_hash(record.media),
            event_at=now,
            received_at=now,
            trace_id=record.trace_id,
            hops=record.hops + 1,
        )
        await self._ingest.ingest(reingested)
        return DeliveryReceipt(external_id=external_id, delivered_at=now)
