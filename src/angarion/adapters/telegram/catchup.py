"""
Алгоритм catch-up §9.3 (FR «Catch-up», M3, фаза 3): дозабор пропущенного
за простой — новые/правки/удаления — на старте listener'а, per source.

Чистая логика над портом ``TelegramClientPort`` (``fetch_history``) и
доменными портами реестра/курсоров: тестируется на fake-клиенте без сети.
Эмитит ``Record`` (``origin='catchup'``) в тот же ``IngestService``,
что и live — дедуп гасит пересечения (§9.3.5).

Ключевые решения (§9.3.4, W-truncation):
- Фетч идёт **от новых к старым** с лимитом ``max_messages`` и отсечкой по
  возрасту ``max_age_days``. Приоритет — свежие сообщения: при длинном
  простое лучше догнать новые, чем упереться в лимит на старых.
- **Покрытый диапазон**: если фетч усечён лимитом/возрастом, его нижняя
  граница — id самого старого зафетченного сообщения; удаления **ниже**
  не эмитируются (лучше пропуск, чем ложное удаление). Если не усечён —
  граница опускается до запрошенного floor (весь реестр покрыт).
- NEW/EDITED решаются **по реестру**: сообщение известно реестру и хэш
  расходится → EDITED; неизвестно и id > курсора → NEW.
- Удаления детектируются только для **chat-уровневых** источников
  (``thread_id=None``): ``map_deletion`` работает на chat-уровне (§9.4),
  топик-удаления live тоже не детектируются.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from angarion.adapters.registry_rules import id_at_least
from angarion.adapters.telegram.client import (
    TRANSPORT,
    RawTelegramDeletion,
)
from angarion.adapters.telegram.mapping import map_deletion, map_message, raw_media_hash
from angarion.adapters.telegram.media import enrich_with_downloads
from angarion.domain.keys import make_source_key, normalize_and_hash
from angarion.domain.models import AnalyticsEvent, RecordKind, SourceCursor

if TYPE_CHECKING:
    from datetime import timedelta

    from pydantic import AwareDatetime
    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.telegram.client import (
        RawTelegramMessage,
        TelegramClientPort,
    )
    from angarion.application.ingest import IngestService
    from angarion.config import MediaConfig
    from angarion.domain.ports import (
        AnalyticsPort,
        CursorStorePort,
        MessageRegistryPort,
    )

LAST_SEEN_KEY = 'last_seen_external_id'
"""Ключ payload курсора Telegram: численно сравниваемый внешний id (§9.2)."""

LAST_SCAN_KEY = 'last_scan_at'
"""Ключ payload курсора Telegram: ISO-время последнего прохода catch-up."""


def _last_seen(cursor: SourceCursor | None) -> int:
    """Численный ``last_seen_external_id`` курсора или 0 (первый запуск)."""
    if cursor is None:
        return 0
    raw = cursor.payload.get(LAST_SEEN_KEY)
    return int(raw) if raw is not None else 0


async def run_catchup(
    *,
    client: TelegramClientPort,
    account_id: str,
    chat_id: int,
    thread_id: str | None,
    registry: MessageRegistryPort,
    cursors: CursorStorePort,
    ingest: IngestService,
    analytics: AnalyticsPort,
    log: FilteringBoundLogger,
    media_policy: MediaConfig,
    max_messages: int,
    max_age: timedelta,
    now: AwareDatetime,
    record_truncation: bool = True,
) -> None:
    """
    Прогнать §9.3 по одному источнику: фетч истории → эмиссия → курсор.

    ``max_messages`` + ``max_age`` задают окно фетча (новые→старые). Лёгкий
    поллинг недавнего окна (T032) зовёт тот же алгоритм с малым окном и
    ``record_truncation=False``: узкое окно «усечено» by design, поэтому
    ``catchup_truncated`` для него — шум (truncation-guard при этом всё
    равно не даёт ложных удалений ниже окна — §9.3.4).
    """
    source_key = make_source_key(TRANSPORT, account_id, str(chat_id), thread_id)
    cursor = await cursors.load(source_key)
    last_seen = _last_seen(cursor)
    known = await registry.known_ids(source_key, '')
    floor = _requested_floor(known, last_seen)

    fetched, truncated = await _fetch(
        client=client,
        chat_id=chat_id,
        thread_id=thread_id,
        min_id=max(floor - 1, 0),
        max_messages=max_messages,
        cutoff=now - max_age,
    )

    await _emit_messages(
        fetched=fetched,
        account_id=account_id,
        registry=registry,
        ingest=ingest,
        client=client,
        media_policy=media_policy,
        log=log,
        last_seen=last_seen,
    )
    if thread_id is None:
        floor_covered = (
            min(raw.message_id for raw in fetched) if truncated and fetched else floor
        )
        await _emit_deletions(
            fetched=fetched,
            known=known,
            account_id=account_id,
            chat_id=chat_id,
            ingest=ingest,
            floor_covered=floor_covered,
            now=now,
        )

    await _save_cursor(cursors, source_key, fetched, last_seen, now)
    if truncated and record_truncation:
        log.warning('catchup_truncated', source_key=source_key, fetched=len(fetched))
        await analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind='catchup_truncated',
                payload={'source_key': source_key, 'fetched': len(fetched)},
                at=now,
            )
        )


def _requested_floor(known: set[str], last_seen: int) -> int:
    """Нижняя граница фетча: покрыть окно реестра и новые сообщения."""
    floors = [last_seen + 1]
    decimal_known = [int(k) for k in known if k.isdecimal()]
    if decimal_known:
        floors.append(min(decimal_known))
    return min(floors)


async def _fetch(
    *,
    client: TelegramClientPort,
    chat_id: int,
    thread_id: str | None,
    min_id: int,
    max_messages: int,
    cutoff: AwareDatetime,
) -> tuple[list[RawTelegramMessage], bool]:
    """Зафетчить историю (новые→старые), отсекая по возрасту и лимиту."""
    fetched: list[RawTelegramMessage] = []
    truncated = False
    async for raw in client.fetch_history(
        chat_id,
        limit=max_messages,
        thread_id=int(thread_id) if thread_id is not None else None,
        min_id=min_id,
    ):
        if raw.event_at < cutoff:
            truncated = True
            break
        fetched.append(raw)
    if len(fetched) >= max_messages:
        truncated = True
    return fetched, truncated


async def _emit_messages(
    *,
    fetched: list[RawTelegramMessage],
    account_id: str,
    registry: MessageRegistryPort,
    ingest: IngestService,
    client: TelegramClientPort,
    media_policy: MediaConfig,
    log: FilteringBoundLogger,
    last_seen: int,
) -> None:
    """NEW (id > курсора, неизвестно) / EDITED (известно, хэш расходится)."""
    for raw in reversed(fetched):  # старые первыми — естественный порядок
        msg_source_key = make_source_key(
            TRANSPORT,
            account_id,
            str(raw.chat_id),
            str(raw.thread_id) if raw.thread_id is not None else None,
        )
        record = await registry.get(msg_source_key, str(raw.message_id))
        if record is not None and record.deleted_at is None:
            new_hash = normalize_and_hash(raw.text) if raw.text is not None else None
            new_media_hash = raw_media_hash(raw)
            if record.content_hash != new_hash or record.media_hash != new_media_hash:
                edited = raw.model_copy(update={'kind': RecordKind.EDITED})
                event = map_message(edited, account_id, origin='catchup')
                if event is not None:
                    event = await enrich_with_downloads(
                        event, client=client, policy=media_policy, log=log
                    )
                    await ingest.ingest(event)
        elif record is None and raw.message_id > last_seen:
            event = map_message(raw, account_id, origin='catchup')
            if event is not None:
                event = await enrich_with_downloads(
                    event, client=client, policy=media_policy, log=log
                )
                await ingest.ingest(event)


async def _emit_deletions(
    *,
    fetched: list[RawTelegramMessage],
    known: set[str],
    account_id: str,
    chat_id: int,
    ingest: IngestService,
    floor_covered: int,
    now: AwareDatetime,
) -> None:
    """
    Известные реестру id в покрытом диапазоне, отсутствующие в фетче.

    ``known`` — id chat-уровневого source_key (вызывается только при
    ``thread_id=None``); ``map_deletion`` собирает тот же chat-уровневый
    ключ (§9.4), поэтому source_key передавать не нужно.
    """
    present = {str(raw.message_id) for raw in fetched if raw.thread_id is None}
    covered = {k for k in known if id_at_least(k, str(floor_covered))}
    missing = covered - present
    if not missing:
        return
    deletion = RawTelegramDeletion(
        chat_id=chat_id,
        message_ids=tuple(sorted(int(i) for i in missing)),
        deleted_at=now,
    )
    for event in map_deletion(deletion, account_id, origin='catchup'):
        await ingest.ingest(event)


async def _save_cursor(
    cursors: CursorStorePort,
    source_key: str,
    fetched: list[RawTelegramMessage],
    last_seen: int,
    now: AwareDatetime,
) -> None:
    """Сдвинуть курсор к самому новому зафетченному id (§9.3.5)."""
    new_last_seen = max([last_seen, *(raw.message_id for raw in fetched)])
    await cursors.save(
        SourceCursor(
            source_key=source_key,
            payload={
                LAST_SEEN_KEY: str(new_last_seen),
                LAST_SCAN_KEY: now.isoformat(),
            },
            updated_at=now,
        )
    )
