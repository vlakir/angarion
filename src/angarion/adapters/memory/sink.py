"""InMemory-sink (``SinkPort``, §12.4): журнал отправленного."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.domain.models import DeliveryReceipt

if TYPE_CHECKING:
    from angarion.domain.models import OutboundRecord


class MemorySink:
    """«Доставка» в журнал ``sent`` — для тестов и прототипов процессоров."""

    def __init__(self) -> None:
        self.sent: list[OutboundRecord] = []

    async def send(self, record: OutboundRecord) -> DeliveryReceipt:
        """Записать запись в журнал; external_id — порядковый номер."""
        self.sent.append(record)
        return DeliveryReceipt(
            external_id=str(len(self.sent)), delivered_at=datetime.now(UTC)
        )
