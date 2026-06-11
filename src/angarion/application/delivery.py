"""
DeliveryWorker (C-9 спеки T002): доставка исходящих из outbox.

Отдельный от обработки цикл: pending-записи outbox → ``sink.send`` →
``mark_sent``. Сбой отправки не теряет сообщение: ``reschedule`` с
экспоненциальным backoff (та же формула, что у retry envelope — 2.2
plan.md); после ``max_retries`` — терминальный статус ``failed``
(разбор ручной, аналог DLQ для исходящих).

Остаточное окно: падение процесса между ``send`` и ``mark_sent`` даёт
при повторе дубль, не потерю — допустимо моделью at-least-once §7.1.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import structlog

from angarion.domain.models import AnalyticsEvent

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from angarion.domain.models import OutboundRecord
    from angarion.domain.ports import AnalyticsPort, MessageSinkPort, OutboxPort


class DeliveryWorker:
    """Цикл доставки: due-запись → send → mark_sent / reschedule / failed."""

    def __init__(
        self,
        *,
        outbox: OutboxPort,
        sink: MessageSinkPort,
        analytics: AnalyticsPort,
        max_retries: int = 5,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        poll_interval: float = 1.0,
        log: FilteringBoundLogger | None = None,
    ) -> None:
        self._outbox = outbox
        self._sink = sink
        self._analytics = analytics
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._poll_interval = poll_interval
        self._log: FilteringBoundLogger = (
            log if log is not None else structlog.get_logger('angarion.delivery')
        )

    async def run(self) -> None:
        """
        Бесконечный цикл доставки. При отмене задачи текущая запись
        дообрабатывается (graceful, зеркально ``PipelineWorker.run``);
        отмена срабатывает на poll-ожидании.
        """
        while True:
            handling = asyncio.ensure_future(self.deliver_next())
            try:
                delivered = await asyncio.shield(handling)
            except asyncio.CancelledError:
                await handling
                raise
            if not delivered:
                await asyncio.sleep(self._poll_interval)

    async def deliver_next(self) -> bool:
        """Доставить одну due-запись; False — доставлять нечего."""
        records = await self._outbox.due(limit=1)
        if not records:
            return False
        await self._deliver(records[0])
        return True

    async def drain_due(self) -> int:
        """Доставлять, пока есть due-записи (тесты, дожим перед остановкой)."""
        count = 0
        while await self.deliver_next():
            count += 1
        return count

    async def _deliver(self, rec: OutboundRecord) -> None:
        key = rec.msg.idempotency_key
        try:
            receipt = await self._sink.send(rec.msg)
        except Exception as exc:
            await self._handle_failure(rec, exc)
            return
        await self._outbox.mark_sent(key, receipt)
        await self._record('delivered', rec, {'idempotency_key': key})

    async def _handle_failure(self, rec: OutboundRecord, exc: Exception) -> None:
        """Retry-ветка §8 для доставки: reschedule либо терминальный failed."""
        key = rec.msg.idempotency_key
        error = f'{type(exc).__name__}: {exc}'
        attempts = rec.attempts + 1
        if attempts > self._max_retries:
            await self._outbox.mark_failed(key, error)
            await self._record(
                'delivery_failed', rec, {'idempotency_key': key, 'error': error}
            )
            self._log.error(
                'delivery_failed',
                idempotency_key=key,
                pipeline=rec.pipeline,
                attempts=attempts,
                error=error,
            )
            return
        delay = min(self._backoff_base * 2**rec.attempts, self._backoff_cap)
        await self._outbox.reschedule(
            key,
            not_before=datetime.now(UTC) + timedelta(seconds=delay),
            error=error,
        )
        self._log.warning(
            'delivery_retry_scheduled',
            idempotency_key=key,
            pipeline=rec.pipeline,
            attempts=attempts,
            delay=delay,
            error=error,
        )

    async def _record(
        self, kind: str, rec: OutboundRecord, payload: dict[str, Any] | None = None
    ) -> None:
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                event_uid=rec.event_uid,
                pipeline=rec.pipeline,
                payload=payload or {},
                at=datetime.now(UTC),
            )
        )
