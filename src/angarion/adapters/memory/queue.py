"""InMemory-очередь событий (``EventQueuePort``, §12.4)."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import TYPE_CHECKING

from angarion.domain.models import QueueDepth, QueueItem

if TYPE_CHECKING:
    from angarion.domain.models import QueueEnvelope


class MemoryQueue:
    """
    Очередь на deque + ``asyncio.Condition``: FIFO, at-least-once в
    пределах процесса. Receipt — монотонный int (непрозрачен ядру).
    """

    def __init__(self) -> None:
        self._pending: deque[QueueEnvelope] = deque()
        self._unacked: dict[int, QueueEnvelope] = {}
        self._next_receipt = 0
        self._not_empty = asyncio.Condition()

    async def put(self, item: QueueEnvelope) -> None:
        """Положить envelope в хвост; будит ожидающий ``get()``."""
        async with self._not_empty:
            self._pending.append(item)
            self._not_empty.notify()

    async def get(self) -> QueueItem:
        """Ждать голову очереди; элемент переходит в unacked до ack/nack."""
        async with self._not_empty:
            while not self._pending:
                await self._not_empty.wait()
            envelope = self._pending.popleft()
            self._next_receipt += 1
            self._unacked[self._next_receipt] = envelope
            return QueueItem(envelope=envelope, receipt=self._next_receipt)

    async def ack(self, item: QueueItem) -> None:
        """Подтвердить обработку; повторный ack — no-op."""
        self._unacked.pop(item.receipt, None)

    async def nack(self, item: QueueItem) -> None:
        """Аварийный возврат в голову очереди; после ack — no-op."""
        envelope = self._unacked.pop(item.receipt, None)
        if envelope is None:
            return
        async with self._not_empty:
            self._pending.appendleft(envelope)
            self._not_empty.notify()

    async def recover(self) -> int:
        """Вернуть все unacked в голову очереди в исходном порядке."""
        async with self._not_empty:
            recovered = [self._unacked[r] for r in sorted(self._unacked)]
            for envelope in reversed(recovered):
                self._pending.appendleft(envelope)
            self._unacked.clear()
            self._not_empty.notify(len(recovered))
            return len(recovered)

    async def depth(self) -> QueueDepth:
        """Текущие pending/unacked."""
        return QueueDepth(pending=len(self._pending), unacked=len(self._unacked))
