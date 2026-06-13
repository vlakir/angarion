"""
Буфер live-событий на время catch-up (Q8 спеки T005, M3, фаза 2).

In-memory ``asyncio.Queue`` без жёсткого предела: ``put`` неблокирующий
(``put_nowait`` на неограниченной очереди не блокирует), поэтому
update-loop Telethon не застопоривается (W3). При пересечении
**мягкого** лимита — одно предупреждение в лог + событие
``live_buffer_high`` в аналитику на эпизод переполнения (флаг
сбрасывается, когда очередь снова опускается ниже лимита). Ничего не
теряется; цена — рост памяти при патологическом флуде (приемлемо для
single-process v1).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from angarion.domain.models import AnalyticsEvent

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from angarion.domain.models import InboundEvent
    from angarion.domain.ports import AnalyticsPort


class LiveBuffer:
    """Неблокирующий буфер ``InboundEvent`` с warning по мягкому лимиту."""

    def __init__(
        self,
        *,
        soft_limit: int,
        log: FilteringBoundLogger,
        analytics: AnalyticsPort,
    ) -> None:
        self._queue: asyncio.Queue[InboundEvent] = asyncio.Queue()
        self._soft_limit = soft_limit
        self._log = log
        self._analytics = analytics
        self._warned = False

    async def put(self, event: InboundEvent) -> None:
        """Добавить событие (не блокирует); warning при пересечении лимита."""
        self._queue.put_nowait(event)
        depth = self._queue.qsize()
        if depth >= self._soft_limit and not self._warned:
            self._warned = True
            self._log.warning(
                'live_buffer_high', depth=depth, soft_limit=self._soft_limit
            )
            await self._analytics.record(
                AnalyticsEvent(
                    uid=uuid4(),
                    kind='live_buffer_high',
                    payload={'depth': depth, 'soft_limit': self._soft_limit},
                    at=datetime.now(UTC),
                )
            )

    async def get(self) -> InboundEvent:
        """Дождаться и выдать голову буфера (FIFO)."""
        event = await self._queue.get()
        if self._warned and self._queue.qsize() < self._soft_limit:
            self._warned = False
        return event

    def qsize(self) -> int:
        """Текущая глубина буфера."""
        return self._queue.qsize()

    def empty(self) -> bool:
        """Пуст ли буфер."""
        return self._queue.empty()
