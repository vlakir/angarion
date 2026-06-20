"""Counting/Null-сток для нагрузочных прогонов (T044, §5 «Key Entities»)."""

from __future__ import annotations

from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING

from angarion.domain.models import DeliveryReceipt

if TYPE_CHECKING:
    from collections.abc import Callable

    from angarion.domain.models import OutboundRecord


class CountingSink:
    """
    ``SinkPort``, считающий доставленное без накопления записей.

    Для больших / устойчивых нагрузочных прогонов (T044) растущий
    ``MemorySink.sent`` неприемлем — здесь держим только счётчик
    ``count`` и ``perf_counter``-метку последней доставки
    ``last_delivered_at``. Опциональный ``on_delivery`` — хук, через
    который гарнес считает латентность (не накапливая записи в самом
    стоке): сток вызывает его на каждую доставку с исходящей записью.
    """

    def __init__(
        self, on_delivery: Callable[[OutboundRecord], None] | None = None
    ) -> None:
        self.count = 0
        self.last_delivered_at: float | None = None
        self._on_delivery = on_delivery

    async def send(self, record: OutboundRecord) -> DeliveryReceipt:
        """Учесть доставку: инкремент счётчика, perf-метка, опц. хук."""
        self.count += 1
        self.last_delivered_at = perf_counter()
        if self._on_delivery is not None:
            self._on_delivery(record)
        return DeliveryReceipt(
            external_id=str(self.count), delivered_at=datetime.now(UTC)
        )
