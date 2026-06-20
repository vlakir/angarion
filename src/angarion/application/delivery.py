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

from angarion.application.draining import shielded_drain
from angarion.domain.models import AnalyticsEvent

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from angarion.domain.models import OutboxRecord
    from angarion.domain.ports import AnalyticsPort, OutboxPort, SinkPort


class DeliveryWorker:
    """Цикл доставки: due-запись → send → mark_sent / reschedule / failed."""

    def __init__(
        self,
        *,
        outbox: OutboxPort,
        sink: SinkPort,
        analytics: AnalyticsPort,
        max_retries: int = 5,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        poll_interval: float = 1.0,
        shutdown_drain_seconds: float = 5.0,
        log: FilteringBoundLogger | None = None,
    ) -> None:
        self._outbox = outbox
        self._sink = sink
        self._analytics = analytics
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._poll_interval = poll_interval
        self._shutdown_drain_seconds = shutdown_drain_seconds
        self._log: FilteringBoundLogger = (
            log if log is not None else structlog.get_logger('angarion.delivery')
        )

    async def run(self) -> None:
        """
        Бесконечный цикл доставки. При отмене задачи текущая запись
        дообрабатывается (graceful, зеркально ``PipelineWorker.run``), но не
        дольше ``shutdown_drain_seconds`` (T031: залипший в throttle/FloodWait
        sleep send иначе подвешивал бы стоп); отмена срабатывает на poll-ожидании.
        """
        while True:
            handling = asyncio.ensure_future(self.deliver_next())
            delivered = await shielded_drain(handling, self._shutdown_drain_seconds)
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

    async def _deliver(self, rec: OutboxRecord) -> None:
        key = rec.record.idempotency_key
        try:
            receipt = await self._sink.send(rec.record)
        except Exception as exc:
            await self._handle_failure(rec, exc)
            return
        await self._outbox.mark_sent(key, receipt)
        await self._record('delivered', rec, {'idempotency_key': key})

    async def _handle_failure(self, rec: OutboxRecord, exc: Exception) -> None:
        """Retry-ветка §8 для доставки: reschedule либо терминальный failed."""
        key = rec.record.idempotency_key
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
        self, kind: str, rec: OutboxRecord, payload: dict[str, Any] | None = None
    ) -> None:
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                record_uid=rec.record_uid,
                pipeline=rec.pipeline,
                payload=payload or {},
                at=datetime.now(UTC),
            )
        )
