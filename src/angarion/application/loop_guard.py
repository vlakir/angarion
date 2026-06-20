"""
LoopGuardSink (T009 / M6): гашение петли ``source == target``.

Когда цель пайплайна совпадает с прослушиваемым источником, доставленное
сообщение возвращается во входной поток как новое (live или при catch-up)
и без защиты порождает бесконечную петлю. Guard — декоратор
``SinkPort``: после успешной доставки в такую цель он помечает в
``DedupStorePort`` ``dedup_key`` будущего входящего события произведённого
сообщения (NEW и DEL по ``DeliveryReceipt.external_id``), и ``IngestService``
отбрасывает его возврат как ``duplicate``.

Почему dedup, а не фильтр по ``sender_id``: драйвер/источник могут писать
тем же аккаунтом, что и пайплайн (интеграционный контур §13.2 self-driving),
поэтому отличать собственную доставку нужно по id произведённого сообщения,
а не по авторству. Маркеры живут в персистентном dedup-хранилище (переживают
рестарт, попадают под общий ``prune`` §17.3) — новых портов и таблиц не
вводим. Механизм transport-agnostic: знание о Telegram не требуется.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from angarion.domain.keys import make_dedup_key, make_source_key
from angarion.domain.models import RecordKind, Transport

if TYPE_CHECKING:
    from collections.abc import Sequence

    from angarion.domain.models import DeliveryReceipt, OutboundRecord
    from angarion.domain.ports import DedupStorePort, SinkPort


class GuardedSource(BaseModel):
    """Идентичность прослушиваемого источника для сверки с целью доставки."""

    model_config = ConfigDict(frozen=True)

    transport: Transport
    account_id: str
    address: str
    thread_id: str | None = None


class LoopGuardSink:
    """Декоратор ``SinkPort``: dedup-пометка собственных доставок."""

    def __init__(
        self,
        *,
        inner: SinkPort,
        dedup: DedupStorePort,
        sources: Sequence[GuardedSource],
    ) -> None:
        self._inner = inner
        self._dedup = dedup
        self._sources = tuple(sources)

    async def send(self, record: OutboundRecord) -> DeliveryReceipt:
        """Доставить через ``inner``; если цель = источник — пометить dedup."""
        receipt = await self._inner.send(record)
        external_id = receipt.external_id
        if external_id is None:
            return receipt
        target = record.target
        for src in self._sources:
            if (
                src.transport == target.transport
                and src.address == target.address
                and src.thread_id == target.thread_id
            ):
                source_key = make_source_key(
                    src.transport, src.account_id, src.address, src.thread_id
                )
                await self._dedup.mark_inbound(
                    make_dedup_key(RecordKind.NEW, source_key, external_id)
                )
                await self._dedup.mark_inbound(
                    make_dedup_key(RecordKind.DELETED, source_key, external_id)
                )
        return receipt
