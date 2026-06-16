"""
Скачивание вложений при ingest принимающим аккаунтом (M7 A3).

``enrich_with_downloads`` — единый хук для live и catch-up путей адаптера:
по медиа-политике (``MediaConfig``) скачивает подходящие вложения события
в git-ignored каталог и проставляет ``MediaRef.local_path``. Скачивание
**best-effort**: сбой границы Telethon (FloodWait/transient) логируется и
деградирует до «только метаданные» — событие не теряется, sender при
отсутствии ``local_path`` уйдёт в refetch-fast-path (A2).

Логика «качать ли» — в ``MediaConfig.should_download`` (messenger-agnostic,
§3.A); здесь — Telegram-специфичная оркестрация над ``TelegramClientPort``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from angarion.adapters.telegram.client import FloodWaitError, TransientSendError

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.telegram.client import TelegramClientPort
    from angarion.config import MediaConfig
    from angarion.domain.models import InboundEvent, MediaRef


async def enrich_with_downloads(
    event: InboundEvent,
    *,
    client: TelegramClientPort,
    policy: MediaConfig,
    log: FilteringBoundLogger,
) -> InboundEvent:
    """
    Скачать подходящие вложения события и вернуть копию с ``local_path``.

    Без медиа или при выключенной политике возвращает событие как есть
    (без копии). Каждое вложение скачивается независимо; сбой одного не
    влияет на остальные и не теряет событие.
    """
    if not policy.download or not event.media:
        return event
    enriched: list[MediaRef] = []
    changed = False
    for media in event.media:
        downloaded = await _download_one(media, client=client, policy=policy, log=log)
        enriched.append(downloaded)
        changed = changed or downloaded is not media
    return event.model_copy(update={'media': enriched}) if changed else event


async def _download_one(
    media: MediaRef,
    *,
    client: TelegramClientPort,
    policy: MediaConfig,
    log: FilteringBoundLogger,
) -> MediaRef:
    """Скачать одно вложение (best-effort); вернуть тот же ``MediaRef`` при пропуске."""
    if not policy.should_download(media) or media.ref is None:
        return media
    try:
        path = await client.download_media(
            source_ref=media.ref, dest_dir=policy.storage_dir
        )
    except (FloodWaitError, TransientSendError) as exc:
        log.warning('media_download_failed', ref=media.ref, error=str(exc))
        return media
    if path is None:
        log.warning('media_download_empty', ref=media.ref)
        return media
    return media.model_copy(update={'local_path': path})
