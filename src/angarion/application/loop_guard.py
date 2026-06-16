"""
LoopGuardSink (T009 / M6): гашение петли ``source == target``.

Когда цель пайплайна совпадает с прослушиваемым источником, доставленное
сообщение возвращается во входной поток как новое (live или при catch-up)
и без защиты порождает бесконечную петлю. Guard — декоратор
``MessageSinkPort``: после успешной доставки в такую цель он помечает в
``DedupStorePort`` ``dedup_key`` будущего входящего события произведённого
сообщения (NEW и DEL по ``DeliveryReceipt.external_id``), и ``IngestService``
отбрасывает его возврат как ``duplicate``.

Почему dedup, а не фильтр по ``sender_id``: драйвер/источник могут писать
тем же аккаунтом, что и пайплайн (интеграционный контур §13.2 self-driving),
поэтому отличать собственную доставку нужно по id произведённого сообщения,
а не по авторству. Маркеры живут в персистентном dedup-хранилище (переживают
рестарт, попадают под общий ``prune`` §17.3) — новых портов и таблиц не
вводим. Механизм messenger-agnostic: знание о Telegram не требуется.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict

from angarion.domain.keys import make_dedup_key, make_source_key
from angarion.domain.models import EventKind, Messenger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from angarion.domain.models import DeliveryReceipt, OutboundMessage
    from angarion.domain.ports import DedupStorePort, MessageSinkPort


class GuardedSource(BaseModel):
    """Идентичность прослушиваемого источника для сверки с целью доставки."""

    model_config = ConfigDict(frozen=True)

    messenger: Messenger
    account_id: str
    chat_id: str
    thread_id: str | None = None


class LoopGuardSink:
    """Декоратор ``MessageSinkPort``: dedup-пометка собственных доставок."""

    def __init__(
        self,
        *,
        inner: MessageSinkPort,
        dedup: DedupStorePort,
        sources: Sequence[GuardedSource],
    ) -> None:
        self._inner = inner
        self._dedup = dedup
        self._sources = tuple(sources)

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        """Доставить через ``inner``; если цель = источник — пометить dedup."""
        receipt = await self._inner.send(msg)
        external_id = receipt.external_id
        if external_id is None:
            return receipt
        target = msg.target
        for src in self._sources:
            if (
                src.messenger == target.messenger
                and src.chat_id == target.chat_id
                and src.thread_id == target.thread_id
            ):
                source_key = make_source_key(
                    src.messenger, src.account_id, src.chat_id, src.thread_id
                )
                await self._dedup.mark_inbound(
                    make_dedup_key(EventKind.MESSAGE_NEW, source_key, external_id)
                )
                await self._dedup.mark_inbound(
                    make_dedup_key(EventKind.MESSAGE_DELETED, source_key, external_id)
                )
        return receipt
