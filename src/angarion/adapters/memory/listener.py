"""
MemoryListener (§12.4, plan 2.6): driving-адаптер платформы «memory».

Тестовая обвязка с поведением настоящего адаптера: записи подаются
программно через ``emit(raw)``, маппятся в ``Record`` публичными
хелперами ключей (§7.2) и уходят в ``IngestService``. Истории у
платформы нет — ``catchup()`` честно поднимает ``NotSupportedError``
(``history_fetch=False``), постоянно прогоняя ветку деградации FR-13.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from angarion.domain.errors import NotSupportedError
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import AccountRef, Endpoint, Record, RecordKind

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from angarion.application.ingest import IngestService

TRANSPORT = 'memory'
"""Идентификатор транспорта InMemory-плагина."""


class MemoryListener:
    """Программная инжекция событий в конвейер через ``emit(raw)``."""

    def __init__(self, *, ingest: IngestService, account_ids: Sequence[str]) -> None:
        if not account_ids:
            msg = 'нужен хотя бы один аккаунт платформы memory'
            raise ValueError(msg)
        self._ingest = ingest
        self._account_ids = tuple(account_ids)
        self._started = False

    @property
    def started(self) -> bool:
        """Идёт ли «приём» (между start и stop)."""
        return self._started

    async def start(self) -> None:
        """Подключение и live-подписка (§12.11) — для memory условные."""
        self._started = True

    async def stop(self) -> None:
        """Graceful shutdown (вызывается ядром)."""
        self._started = False

    async def catchup(self, source_key: str) -> None:
        """История недоступна (``history_fetch=False``, plan 2.6)."""
        msg = f'платформа memory не хранит историю, catch-up невозможен: {source_key}'
        raise NotSupportedError(msg)

    async def emit(self, raw: Mapping[str, Any]) -> Record:
        """
        Принять «сырую» запись транспорта и подать её в конвейер.

        Обязательные ключи: ``address``, ``external_id``. Опциональные:
        ``kind`` (default ``new``), ``account`` (default —
        первый аккаунт listener'а), ``thread_id``, ``text``,
        ``sender_id``, ``sender_name``, ``reply_to_external_id``,
        ``event_at`` (default — сейчас, UTC). Исходный mapping целиком
        сохраняется в ``Record.raw``.
        """
        kind = RecordKind(str(raw.get('kind', RecordKind.NEW)))
        account_id = str(raw.get('account', self._account_ids[0]))
        if account_id not in self._account_ids:
            msg = (
                f'неизвестный аккаунт {account_id!r}; '
                f'аккаунты listener: {", ".join(self._account_ids)}'
            )
            raise ValueError(msg)
        address = str(raw['address'])
        thread_id = raw.get('thread_id')
        external_id = str(raw['external_id'])
        text = raw.get('text')
        content_hash = normalize_and_hash(text) if text is not None else None
        source_key = make_source_key(TRANSPORT, account_id, address, thread_id)
        now = datetime.now(UTC)
        record = Record(
            uid=uuid4(),
            kind=kind,
            dedup_key=make_dedup_key(kind, source_key, external_id, content_hash),
            origin='live',
            source=Endpoint(transport=TRANSPORT, address=address, thread_id=thread_id),
            received_by=AccountRef(transport=TRANSPORT, account_id=account_id),
            external_id=external_id,
            sender_id=raw.get('sender_id'),
            sender_name=raw.get('sender_name'),
            text=text,
            reply_to_external_id=raw.get('reply_to_external_id'),
            content_hash=content_hash,
            event_at=raw.get('event_at', now),
            received_at=now,
            raw=dict(raw),
        )
        await self._ingest.ingest(record)
        return record
