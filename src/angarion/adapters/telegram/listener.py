"""
TelegramListener (FR «Listener», M3): live-подписки + буфер + catch-up.

Жизненный цикл ``Listener`` (§12.11): ``start`` подключает общий пул
клиентов (``ClientPool.connect_all``, §12.1), греет кэш и резолвит
источники (``resolver``), подписывает live-колбэки, **прогоняет catch-up
§9.3 per source до запуска консьюмера** (live за это время копится в
буфере), затем запускает консьюмер буфера (``LiveBuffer``, Q8) →
``IngestService``; при заданном ``catchup_interval`` поднимает фоновый
таймер периодического страхующего catch-up (Q9/N1, фаза 5). ``stop`` —
graceful (стоп таймера и консьюмера + дослив буфера + отключение пула).
``catchup(source_key)`` — внеочередной прогон по одному источнику
(outbox-команда §12.9 / периодический фоновый таймер).

Live-колбэк маппит сырое событие (``mapping``) и кладёт в буфер; чужие
чаты (не в резолвленном наборе) отсеиваются (страховка к серверному
фильтру Telethon). Удаления — на chat-уровне (§9.4).
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from angarion.adapters.telegram.buffer import LiveBuffer
from angarion.adapters.telegram.catchup import run_catchup
from angarion.adapters.telegram.mapping import map_deletion, map_message
from angarion.adapters.telegram.media import enrich_with_downloads
from angarion.adapters.telegram.resolver import resolve_sources
from angarion.config import MediaConfig

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.telegram.client import (
        RawDeletionHandler,
        RawMessageHandler,
        RawTelegramDeletion,
        RawTelegramMessage,
        TelegramClientPort,
    )
    from angarion.adapters.telegram.registry import ClientPool
    from angarion.adapters.telegram.resolver import ResolvedSource
    from angarion.application.ingest import IngestService
    from angarion.config import EndpointConfig
    from angarion.domain.models import InboundEvent
    from angarion.domain.ports import (
        AnalyticsPort,
        CursorStorePort,
        MessageRegistryPort,
    )

DEFAULT_BUFFER_SOFT_LIMIT = 1000
"""Мягкий лимит буфера live по умолчанию (конфиг-ключ — фаза 5)."""

_NO_MEDIA_POLICY: MediaConfig = MediaConfig()
"""Дефолт-политика медиа: скачивание выключено (метаданные, A3)."""


class TelegramListener:
    """Live-listener Telegram: подписки + буфер + catch-up → ingest."""

    def __init__(
        self,
        *,
        ingest: IngestService,
        pool: ClientPool,
        sources: Sequence[EndpointConfig],
        registry: MessageRegistryPort,
        cursors: CursorStorePort,
        analytics: AnalyticsPort,
        log: FilteringBoundLogger,
        catchup_enabled: bool = True,
        catchup_max_messages: int = 2000,
        catchup_max_age_days: int = 7,
        catchup_interval: float | None = None,
        recent_poll_endpoints: frozenset[EndpointConfig] = frozenset(),
        recent_interval: float = 0.0,
        recent_window_messages: int = 30,
        recent_window_minutes: int = 10,
        buffer_soft_limit: int = DEFAULT_BUFFER_SOFT_LIMIT,
        media_policy: MediaConfig = _NO_MEDIA_POLICY,
    ) -> None:
        if not pool.account_ids:
            msg = 'нужен хотя бы один клиент Telegram'
            raise ValueError(msg)
        self._ingest = ingest
        self._media_policy = media_policy
        self._pool = pool
        self._clients: dict[str, TelegramClientPort] = {}
        self._sources = tuple(sources)
        self._registry = registry
        self._cursors = cursors
        self._analytics = analytics
        self._log = log
        self._catchup_enabled = catchup_enabled
        self._catchup_max_messages = catchup_max_messages
        self._catchup_max_age_days = catchup_max_age_days
        self._catchup_interval = catchup_interval
        self._recent_poll_endpoints = recent_poll_endpoints
        self._recent_interval = recent_interval
        self._recent_window_messages = recent_window_messages
        self._recent_window_minutes = recent_window_minutes
        self._buffer = LiveBuffer(
            soft_limit=buffer_soft_limit, log=log, analytics=analytics
        )
        self._resolved: tuple[ResolvedSource, ...] = ()
        self._chat_ids: dict[str, set[str]] = {}
        self._consumer: asyncio.Task[None] | None = None
        self._catchup_timer: asyncio.Task[None] | None = None
        self._recent_timer: asyncio.Task[None] | None = None

    @property
    def started(self) -> bool:
        """Идёт ли приём (запущен консьюмер буфера)."""
        return self._consumer is not None

    async def start(self) -> None:
        """Подключение + прогрев + резолв + подписка + catch-up + консьюмер."""
        await self._pool.connect_all()
        self._clients = dict(self._pool.clients)
        self._resolved = tuple(
            await resolve_sources(
                clients=self._clients,
                sources=self._sources,
                analytics=self._analytics,
                log=self._log,
                recent_poll_endpoints=self._recent_poll_endpoints,
            )
        )
        self._chat_ids = {account_id: set() for account_id in self._clients}
        for rs in self._resolved:
            self._chat_ids[rs.account_id].add(str(rs.chat_id))
        self._subscribe()
        if self._catchup_enabled:
            for rs in self._resolved:
                await self._catchup_source(rs)
        self._consumer = asyncio.create_task(
            self._consume(), name='telegram-live-buffer'
        )
        if self._catchup_interval:
            self._catchup_timer = asyncio.create_task(
                self._periodic_catchup(self._catchup_interval),
                name='telegram-catchup-timer',
            )
        if self._recent_interval > 0 and any(rs.recent_poll for rs in self._resolved):
            self._recent_timer = asyncio.create_task(
                self._periodic_recent_poll(self._recent_interval),
                name='telegram-recent-poll-timer',
            )

    async def stop(self) -> None:
        """Graceful: стоп таймеров и консьюмера, дослив буфера, отключение пула."""
        if self._consumer is None:
            return
        await self._cancel(self._recent_timer)
        self._recent_timer = None
        await self._cancel(self._catchup_timer)
        self._catchup_timer = None
        await self._cancel(self._consumer)
        self._consumer = None
        # консьюмер снят — мы единственный читатель, get() не зависнет
        while not self._buffer.empty():
            await self._emit(await self._buffer.get())
        await self._pool.disconnect_all()

    @staticmethod
    async def _cancel(task: asyncio.Task[None] | None) -> None:
        if task is None:
            return
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _periodic_catchup(self, interval: float) -> None:
        """Фоновый страхующий catch-up по таймеру (Q9/N1); дедуп гасит пересечения."""
        while True:
            await asyncio.sleep(interval)
            for rs in self._resolved:
                await self._catchup_source(rs)

    async def _periodic_recent_poll(self, interval: float) -> None:
        """
        Лёгкий поллинг недавнего окна по таймеру (T032): частая дешёвая
        сверка узкого окна recent-poll источников. Обход последовательный
        (без залпа в history-API); дедуп гасит пересечения с live/catch-up.
        """
        while True:
            await asyncio.sleep(interval)
            for rs in self._resolved:
                if rs.recent_poll:
                    await self._recent_poll_source(rs)

    async def _recent_poll_source(self, rs: ResolvedSource) -> None:
        """Прогон §9.3 по узкому недавнему окну источника (T032)."""
        await run_catchup(
            client=self._clients[rs.account_id],
            account_id=rs.account_id,
            chat_id=rs.chat_id,
            thread_id=rs.thread_id,
            registry=self._registry,
            cursors=self._cursors,
            ingest=self._ingest,
            analytics=self._analytics,
            log=self._log,
            media_policy=self._media_policy,
            max_messages=self._recent_window_messages,
            max_age=timedelta(minutes=self._recent_window_minutes),
            now=datetime.now(UTC),
            record_truncation=False,
        )

    async def catchup(self, source_key: str) -> None:
        """Внеочередной catch-up §9.3 по одному резолвленному источнику."""
        for rs in self._resolved:
            if rs.source_key == source_key:
                await self._catchup_source(rs)
                return
        msg = f'источник не резолвлен: {source_key}'
        raise KeyError(msg)

    async def _catchup_source(self, rs: ResolvedSource) -> None:
        await run_catchup(
            client=self._clients[rs.account_id],
            account_id=rs.account_id,
            chat_id=rs.chat_id,
            thread_id=rs.thread_id,
            registry=self._registry,
            cursors=self._cursors,
            ingest=self._ingest,
            analytics=self._analytics,
            log=self._log,
            media_policy=self._media_policy,
            max_messages=self._catchup_max_messages,
            max_age=timedelta(days=self._catchup_max_age_days),
            now=datetime.now(UTC),
        )

    def _subscribe(self) -> None:
        for account_id, client in self._clients.items():
            client.on_new_message(self._message_handler(account_id))
            client.on_message_edited(self._message_handler(account_id))
            client.on_message_deleted(self._deletion_handler(account_id))

    def _message_handler(self, account_id: str) -> RawMessageHandler:
        async def handle(raw: RawTelegramMessage) -> None:
            if str(raw.chat_id) not in self._chat_ids.get(account_id, set()):
                return
            event = map_message(raw, account_id)
            if event is not None:
                await self._buffer.put(event)

        return handle

    def _deletion_handler(self, account_id: str) -> RawDeletionHandler:
        async def handle(raw: RawTelegramDeletion) -> None:
            known = self._chat_ids.get(account_id, set())
            if raw.chat_id is None or str(raw.chat_id) not in known:
                return
            for event in map_deletion(raw, account_id):
                await self._buffer.put(event)

        return handle

    async def _consume(self) -> None:
        while True:
            await self._emit(await self._buffer.get())

    async def _emit(self, event: InboundEvent) -> None:
        """Скачать медиа по политике (A3) → ingest (live и дослив буфера)."""
        client = self._clients.get(event.received_by.account_id)
        if client is not None:
            event = await enrich_with_downloads(
                event, client=client, policy=self._media_policy, log=self._log
            )
        await self._ingest.ingest(event)
