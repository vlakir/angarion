"""InMemory-sink (``MessageSinkPort``, §12.4): журнал отправленного."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.domain.models import DeliveryReceipt

if TYPE_CHECKING:
    from angarion.domain.models import OutboundMessage


class MemorySink:
    """«Доставка» в журнал ``sent`` — для тестов и прототипов процессоров."""

    def __init__(self) -> None:
        self.sent: list[OutboundMessage] = []

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        """Записать сообщение в журнал; external_id — порядковый номер."""
        self.sent.append(msg)
        return DeliveryReceipt(
            external_id=str(len(self.sent)), delivered_at=datetime.now(UTC)
        )
