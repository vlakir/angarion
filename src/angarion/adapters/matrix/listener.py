"""
MatrixListener (FR «Listener», M7 B2): sync-loop подписки → ingest.

Жизненный цикл ``Listener`` (§12.11): ``start`` восстанавливает сессию
каждого аккаунта (``restore``), резолвит комнаты-источники, подписывает
колбэки (new/edited/deleted/UTD/sync) и поднимает per-account
``sync_forever`` с позиции сохранённого ``next_batch`` (возобновление
после простоя — сервер отдаёт пропущенное за токеном; этого достаточно
для базовой непрерывности, глубокий catch-up по ``/messages`` — фаза B3).
``stop`` — graceful (остановка клиентов + дослив задач). ``catchup`` —
fail-fast до B3.

Колбэк маппит сырое событие (``mapping``) и сразу ингестит: sync-колбэки
nio выполняются inline в цикле sync, дополнительный буфер не нужен (в
отличие от concurrent-апдейтов Telethon). Чужие комнаты (не в наборе
источников аккаунта) отсеиваются. UTD (``MegolmEvent`` без ключа) —
помечается в аналитику и пропускается: исторические шифрованные события
до входа устройства недоступны по природе Matrix E2EE (platform
limitation §17.9, не падение).

``next_batch`` — account-level позиция sync; хранится в
``CursorStorePort`` под зарезервированным ``source_key`` (chat-сегмент
``_sync``), отдельно от per-room курсоров истории (B3).
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from angarion.adapters.matrix.client import MESSENGER
from angarion.adapters.matrix.mapping import map_message, map_redaction
from angarion.config import MediaConfig
from angarion.domain.keys import make_source_key
from angarion.domain.models import AnalyticsEvent, SourceCursor

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.matrix.client import (
        MatrixClientPort,
        RawDeletionHandler,
        RawMatrixDeletion,
        RawMatrixMessage,
        RawMatrixUndecryptable,
        RawMessageHandler,
        SyncHandler,
        UndecryptableHandler,
    )
    from angarion.application.ingest import IngestService
    from angarion.config import EndpointConfig
    from angarion.domain.models import InboundEvent
    from angarion.domain.ports import AnalyticsPort, CursorStorePort

_NO_MEDIA_POLICY: MediaConfig = MediaConfig()
"""Дефолт-политика медиа: скачивание выключено (только метаданные, A3)."""

SYNC_CURSOR_CHAT = '_sync'
"""Зарезервированный chat-сегмент source_key для account-level sync-токена."""

NEXT_BATCH_KEY = 'next_batch'
"""Ключ payload курсора Matrix: непрозрачный ``next_batch`` sync (§9.2)."""

LAST_SCAN_KEY = 'last_scan_at'
"""Ключ payload курсора Matrix: ISO-время последнего сохранения токена."""


class MatrixListener:
    """Live-listener Matrix: sync-подписки + next_batch-курсор → ingest."""

    def __init__(
        self,
        *,
        ingest: IngestService,
        clients: Mapping[str, MatrixClientPort],
        sources: Sequence[EndpointConfig],
        cursors: CursorStorePort,
        analytics: AnalyticsPort,
        log: FilteringBoundLogger,
        media_policy: MediaConfig = _NO_MEDIA_POLICY,
        catchup_enabled: bool = True,
        catchup_max_messages: int = 2000,
        catchup_max_age_days: int = 7,
        recent_poll_endpoints: frozenset[EndpointConfig] = frozenset(),
        recent_interval: float = 0.0,
        recent_window_messages: int = 30,
        recent_window_minutes: int = 10,
    ) -> None:
        if not clients:
            msg = 'нужен хотя бы один клиент Matrix'
            raise ValueError(msg)
        self._ingest = ingest
        self._clients = dict(clients)
        self._sources = tuple(sources)
        self._cursors = cursors
        self._analytics = analytics
        self._log = log
        self._media_policy = media_policy
        self._catchup_enabled = catchup_enabled
        self._catchup_max_messages = catchup_max_messages
        self._catchup_max_age_days = catchup_max_age_days
        self._recent_poll_endpoints = recent_poll_endpoints
        self._recent_interval = recent_interval
        self._recent_window_messages = recent_window_messages
        self._recent_window_minutes = recent_window_minutes
        self._rooms: dict[str, set[str]] = {}
        # source_key (chat-уровень) → (account_id, room_id) для catchup по запросу
        self._catchup_rooms: dict[str, tuple[str, str]] = {}
        # (account_id, room_id) recent-poll комнат (T032 фаза 2)
        self._recent_poll_rooms: set[tuple[str, str]] = set()
        self._tasks: list[asyncio.Task[None]] = []
        self._recent_timer: asyncio.Task[None] | None = None

    @property
    def started(self) -> bool:
        """Идёт ли приём (подняты sync-задачи)."""
        return bool(self._tasks)

    async def start(self) -> None:
        """Восстановление + резолв комнат + подписка + sync per account."""
        for account_id, client in self._clients.items():
            await client.restore()
            rooms = await self._resolve_rooms(account_id, client)
            self._rooms[account_id] = rooms
            for room_id in rooms:
                key = make_source_key(MESSENGER, account_id, room_id)
                self._catchup_rooms[key] = (account_id, room_id)
            handler = self._message_handler(account_id)
            client.on_new_message(handler)
            client.on_message_edited(handler)
            client.on_message_deleted(self._deletion_handler(account_id))
            client.on_undecryptable(self._utd_handler(account_id))
            client.on_sync(self._sync_handler(account_id))
            if self._catchup_enabled:
                for room_id in rooms:
                    await self._catchup_room(
                        account_id,
                        room_id,
                        client,
                        limit=self._catchup_max_messages,
                        max_age=timedelta(days=self._catchup_max_age_days),
                    )
            since = await self._load_since(account_id)
            self._tasks.append(
                asyncio.create_task(
                    client.sync_forever(since=since), name=f'matrix-sync-{account_id}'
                )
            )
        if self._recent_interval > 0 and self._recent_poll_rooms:
            self._recent_timer = asyncio.create_task(
                self._periodic_recent_poll(self._recent_interval),
                name='matrix-recent-poll-timer',
            )
        # дать sync-задачам стартовать (дойти до первого await цикла)
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """Graceful: остановить клиентов и дождаться завершения sync-задач."""
        if not self._tasks:
            return
        if self._recent_timer is not None:
            self._recent_timer.cancel()
            with suppress(asyncio.CancelledError):
                await self._recent_timer
            self._recent_timer = None
        for client in self._clients.values():
            await client.stop()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def catchup(self, source_key: str) -> None:
        """Внеочередной catch-up §9.3 по одному резолвленному источнику (B3)."""
        target = self._catchup_rooms.get(source_key)
        if target is None:
            msg = f'источник не резолвлен: {source_key}'
            raise KeyError(msg)
        account_id, room_id = target
        await self._catchup_room(
            account_id,
            room_id,
            self._clients[account_id],
            limit=self._catchup_max_messages,
            max_age=timedelta(days=self._catchup_max_age_days),
        )

    async def _periodic_recent_poll(self, interval: float) -> None:
        """
        Лёгкий поллинг недавнего окна по таймеру (T032 фаза 2): частая
        дешёвая сверка узкого окна recent-poll комнат. Обход
        последовательный; дедуп гасит пересечения с live/catch-up,
        правки/удаления приходят явными ``m.replace``/redaction в окне.
        """
        while True:
            await asyncio.sleep(interval)
            for account_id, room_id in self._recent_poll_rooms:
                await self._catchup_room(
                    account_id,
                    room_id,
                    self._clients[account_id],
                    limit=self._recent_window_messages,
                    max_age=timedelta(minutes=self._recent_window_minutes),
                )

    async def _catchup_room(
        self,
        account_id: str,
        room_id: str,
        client: MatrixClientPort,
        *,
        limit: int,
        max_age: timedelta,
    ) -> None:
        """
        Дозабор истории комнаты через ``/messages`` (§9.3, B3).

        ``limit`` + ``max_age`` задают окно: глубокий проход — вся история
        (``catchup_max_*``), лёгкий поллинг недавнего окна (T032) — малое
        окно с тем же кодом. Matrix-история несёт **явные** события:
        правка — отдельное ``m.replace``-событие (``kind`` уже размечен в
        границе nio), удаление — redaction-событие. Поэтому, в отличие от
        Telegram, edit/delete не вычисляются по реестру/отсутствию —
        мапятся напрямую; дедуп гасит пересечения с live и повторные
        прогоны, ``previous_text`` правок достаёт ``IngestService`` из
        реестра.
        """
        page = await client.fetch_history(room_id, limit=limit)
        cutoff = datetime.now(UTC) - max_age
        for raw in reversed(page.messages):  # старые первыми (естественный порядок)
            if raw.event_at >= cutoff:
                await self._emit(map_message(raw, account_id, origin='catchup'))
        for redaction in page.redactions:
            await self._emit(map_redaction(redaction, account_id, origin='catchup'))

    async def _resolve_rooms(
        self, account_id: str, client: MatrixClientPort
    ) -> set[str]:
        rooms: set[str] = set()
        for src in self._sources:
            if src.account == account_id:
                room_id = await client.resolve_room(src.chat_id)
                rooms.add(room_id)
                if src in self._recent_poll_endpoints:  # T032: recent-poll комнаты
                    self._recent_poll_rooms.add((account_id, room_id))
        return rooms

    async def _emit(self, event: InboundEvent) -> None:
        """Скачать медиа по политике (A3) → ingest (live и catch-up)."""
        await self._ingest.ingest(await self._enrich(event))

    async def _enrich(self, event: InboundEvent) -> InboundEvent:
        """Скачать ``mxc``-вложения принимающим аккаунтом по политике (A3)."""
        if not event.media:
            return event
        client = self._clients.get(event.received_by.account_id)
        if client is None:
            return event
        updated: list[object] = []
        changed = False
        for ref in event.media:
            enriched = ref
            if (
                ref.local_path is None
                and ref.ref is not None
                and self._media_policy.should_download(ref)
            ):
                path = await client.download_media(
                    mxc=ref.ref, dest_dir=self._media_policy.storage_dir
                )
                if path is not None:
                    enriched = ref.model_copy(update={'local_path': path})
                    changed = True
            updated.append(enriched)
        return event.model_copy(update={'media': updated}) if changed else event

    def _message_handler(self, account_id: str) -> RawMessageHandler:
        async def handle(raw: RawMatrixMessage) -> None:
            if raw.room_id not in self._rooms.get(account_id, set()):
                return
            await self._emit(map_message(raw, account_id))

        return handle

    def _deletion_handler(self, account_id: str) -> RawDeletionHandler:
        async def handle(raw: RawMatrixDeletion) -> None:
            if raw.room_id not in self._rooms.get(account_id, set()):
                return
            await self._emit(map_redaction(raw, account_id))

        return handle

    def _utd_handler(self, account_id: str) -> UndecryptableHandler:
        async def handle(raw: RawMatrixUndecryptable) -> None:
            if raw.room_id not in self._rooms.get(account_id, set()):
                return
            self._log.warning(
                'matrix_undecryptable',
                room_id=raw.room_id,
                event_id=raw.event_id,
                account_id=account_id,
            )
            await self._analytics.record(
                AnalyticsEvent(
                    uid=uuid4(),
                    kind='matrix_undecryptable',
                    payload={
                        'room_id': raw.room_id,
                        'event_id': raw.event_id,
                        'account_id': account_id,
                    },
                    at=datetime.now(UTC),
                )
            )

        return handle

    def _sync_handler(self, account_id: str) -> SyncHandler:
        source_key = make_source_key(MESSENGER, account_id, SYNC_CURSOR_CHAT)

        async def handle(next_batch: str) -> None:
            now = datetime.now(UTC)
            payload = {NEXT_BATCH_KEY: next_batch, LAST_SCAN_KEY: now.isoformat()}
            await self._cursors.save(
                SourceCursor(source_key=source_key, payload=payload, updated_at=now)
            )

        return handle

    async def _load_since(self, account_id: str) -> str | None:
        source_key = make_source_key(MESSENGER, account_id, SYNC_CURSOR_CHAT)
        cursor = await self._cursors.load(source_key)
        if cursor is None:
            return None
        raw = cursor.payload.get(NEXT_BATCH_KEY)
        return raw if isinstance(raw, str) else None
