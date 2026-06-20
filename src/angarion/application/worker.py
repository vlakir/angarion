"""
PipelineWorker (§6.3, §8 ТЗ; FR-9–11) и ScopedStateStore (§10.3).

Конкурентность M1 = 1: единичный worker даёт строгий FIFO бесплатно;
параллелизм — санкционированная эволюция §17.8 без изменения портов.

Инварианты:

- все outbound зафиксированы в outbox строго до ``ack`` (C-9):
  повторная обработка после падения гасится insert-if-absent по
  ``idempotency_key``; доставкой занимается отдельный
  ``DeliveryWorker`` (``application/delivery.py``), сбой отправки не
  теряет сообщение;
- retry — defer-to-tail (C-8): ``put(attempt+1, not_before)`` строго
  до ``ack`` исходного (падение между ними даёт дубль, не потерю);
- «ранний» envelope возвращается в хвост (``put`` + ``ack``) с
  коротким sleep против горячего цикла;
- после ``max_retries`` — полный дамп envelope в DLQ + ``failed``.

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any, Final
from uuid import uuid4

import structlog
from pydantic import BaseModel, ConfigDict
from structlog.typing import FilteringBoundLogger

from angarion.application.draining import shielded_drain
from angarion.domain.keys import make_idempotency_key
from angarion.domain.models import (
    AnalyticsEvent,
    DeadLetter,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    QueueEnvelope,
    QueueItem,
    Verdict,
)
from angarion.domain.ports import (
    AnalyticsPort,
    DeadLetterPort,
    EventQueuePort,
    OutboxPort,
    ProcessorPort,
    RuntimeConfigPort,
    StateStorePort,
)

DEFER_SLEEP_CAP_SECONDS: Final = 1.0
"""Верхняя граница sleep при возврате «раннего» envelope в хвост (C-8)."""

PAUSE_POLL_SECONDS: Final = 1.0
"""Sleep при возврате envelope паузнутого пайплайна в хвост (FR-4, §12.8)."""


class ScopedStateStore:
    """
    Обёртка ``StateStorePort`` с зафиксированным namespace = имя
    пайплайна (FR-11, §10.3); удовлетворяет протоколу ``ScopedState``.
    """

    def __init__(self, store: StateStorePort, namespace: str) -> None:
        self._store = store
        self._namespace = namespace

    async def get(self, key: str) -> str | None:
        """Значение или None."""
        return await self._store.get(self._namespace, key)

    async def set(self, key: str, value: str) -> None:
        """Записать значение (JSON-строка)."""
        await self._store.set(self._namespace, key, value)

    async def delete(self, key: str) -> None:
        """Удалить ключ; отсутствующий — no-op."""
        await self._store.delete(self._namespace, key)

    async def keys(self, prefix: str = '') -> list[str]:
        """Ключи пайплайна с данным префиксом, отсортированы."""
        return await self._store.keys(self._namespace, prefix)


class PipelineBinding(BaseModel):
    """
    Связка пайплайна: процессор + контекст. Конструкция композиции
    (A-2): JSON-контракт DTO на неё не распространяется.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    processor: ProcessorPort
    ctx: PipelineContextData
    forward_media: bool = True
    """Пересылать ли медиа исходящих (T033); ``False`` — worker стрипает ``media``."""


class PipelineWorker:
    """Цикл обработки очереди: process → outbox → ack (§6.3, C-9)."""

    def __init__(
        self,
        *,
        queue: EventQueuePort,
        outbox: OutboxPort,
        analytics: AnalyticsPort,
        dead_letters: DeadLetterPort,
        state: StateStorePort,
        runtime_config: RuntimeConfigPort,
        pipelines: Mapping[str, PipelineBinding],
        max_retries: int = 5,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        shutdown_drain_seconds: float = 5.0,
        log: FilteringBoundLogger | None = None,
    ) -> None:
        self._queue = queue
        self._outbox = outbox
        self._analytics = analytics
        self._dead_letters = dead_letters
        self._state = state
        self._runtime_config = runtime_config
        self._pipelines = dict(pipelines)
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._shutdown_drain_seconds = shutdown_drain_seconds
        self._log: FilteringBoundLogger = (
            log if log is not None else structlog.get_logger('angarion.worker')
        )

    async def run(self) -> None:
        """
        Бесконечный цикл worker'а. При отмене задачи текущий item
        дообрабатывается (graceful-остановка, plan 2.9), но не дольше
        ``shutdown_drain_seconds`` (T031: иначе залипший в throttle/FloodWait
        sleep item подвешивал бы стоп); отмена срабатывает на ожидании очереди.
        """
        while True:
            item = await self._queue.get()
            handling = asyncio.ensure_future(self._handle(item))
            await shielded_drain(handling, self._shutdown_drain_seconds)

    async def process_one(self) -> None:
        """Дождаться и обработать ровно один элемент очереди."""
        item = await self._queue.get()
        await self._handle(item)

    async def _handle(self, item: QueueItem) -> None:
        envelope = item.envelope
        now = datetime.now(UTC)
        if envelope.not_before is not None and envelope.not_before > now:
            await self._defer(item, envelope.not_before - now)
            return
        if await self._is_paused(envelope.pipeline):
            await self._defer_paused(item)
            return
        binding = self._pipelines.get(envelope.pipeline)
        if binding is None:
            error = f'неизвестный пайплайн: {envelope.pipeline!r}'
            await self._fail(item, error=error)
            return
        try:
            result = await self._process_and_stage(envelope, binding)
        except Exception as exc:
            await self._retry_or_fail(item, exc)
            return
        verdict_kind = 'processed' if result.verdict is Verdict.DELIVER else 'dropped'
        await self._record(verdict_kind, envelope)
        await self._queue.ack(item)

    async def _process_and_stage(
        self, envelope: QueueEnvelope, binding: PipelineBinding
    ) -> ProcessingResult:
        """§6.3 (C-9): обработка + фиксация outbound в outbox (до ack)."""
        svc = self._make_services(envelope)
        result = await binding.processor.process(envelope.record, binding.ctx, svc)
        if result.verdict is Verdict.DELIVER:
            for out in result.outbound:
                staged = (
                    out
                    if binding.forward_media or not out.media
                    else out.model_copy(update={'media': []})
                )
                await self._outbox.put(
                    staged,
                    pipeline=envelope.pipeline,
                    record_uid=envelope.record.uid,
                )
        for extra in result.events:
            await self._analytics.record(extra)
        return result

    def _make_services(self, envelope: QueueEnvelope) -> ProcessorServices:
        """Сервисы процессора: лог с correlation id (§14.6), state, ключи."""
        return ProcessorServices(
            log=self._log.bind(
                pipeline=envelope.pipeline, record_uid=str(envelope.record.uid)
            ),
            state=ScopedStateStore(self._state, envelope.pipeline),
            make_idempotency_key=partial(make_idempotency_key, envelope.pipeline),
        )

    async def _defer(self, item: QueueItem, remaining: timedelta) -> None:
        """C-8: вернуть «ранний» envelope в хвост и притормозить цикл."""
        await self._queue.put(item.envelope)
        await self._queue.ack(item)
        await asyncio.sleep(min(remaining.total_seconds(), DEFER_SLEEP_CAP_SECONDS))

    async def _is_paused(self, pipeline: str) -> bool:
        """FR-4 (§12.8): пайплайн в динамическом ``paused_pipelines``."""
        paused = (await self._runtime_config.load()).paused_pipelines
        return paused is not None and pipeline in paused

    async def _defer_paused(self, item: QueueItem) -> None:
        """
        FR-4 (§12.8): пауза ≠ потеря — envelope паузнутого пайплайна
        возвращается в хвост (``put`` строго до ``ack``: дубль возможен,
        потеря — нет), фиксируется ``deferred``, цикл тормозится против
        горячего прокручивания накопленной очереди.
        """
        envelope = item.envelope
        await self._queue.put(envelope)
        await self._queue.ack(item)
        await self._record('deferred', envelope, {'reason': 'paused'})
        await asyncio.sleep(PAUSE_POLL_SECONDS)

    async def _retry_or_fail(self, item: QueueItem, exc: Exception) -> None:
        """Retry-ветка §8: re-enqueue с backoff либо DLQ после исчерпания."""
        envelope = item.envelope
        error = f'{type(exc).__name__}: {exc}'
        if envelope.attempt >= self._max_retries:
            await self._fail(item, error=error)
            return
        delay = min(self._backoff_base * 2**envelope.attempt, self._backoff_cap)
        retry = envelope.model_copy(
            update={
                'attempt': envelope.attempt + 1,
                'not_before': datetime.now(UTC) + timedelta(seconds=delay),
            }
        )
        await self._queue.put(retry)  # строго до ack: дубль возможен, потеря — нет
        await self._queue.ack(item)
        self._log.warning(
            'retry_scheduled',
            pipeline=envelope.pipeline,
            record_uid=str(envelope.record.uid),
            attempt=retry.attempt,
            delay=delay,
            error=error,
        )

    async def _fail(self, item: QueueItem, *, error: str) -> None:
        """Полный дамп envelope в DLQ + ``failed`` + ack (§8)."""
        envelope = item.envelope
        letter = DeadLetter(
            uid=uuid4(), envelope=envelope, error=error, failed_at=datetime.now(UTC)
        )
        await self._dead_letters.put(letter)  # строго до ack
        await self._record('failed', envelope, {'error': error})
        await self._queue.ack(item)
        self._log.error(
            'dead_lettered',
            pipeline=envelope.pipeline,
            record_uid=str(envelope.record.uid),
            attempt=envelope.attempt,
            error=error,
        )

    async def _record(
        self,
        kind: str,
        envelope: QueueEnvelope,
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                record_uid=envelope.record.uid,
                pipeline=envelope.pipeline,
                payload=payload or {},
                at=datetime.now(UTC),
            )
        )
