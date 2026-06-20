"""
Скачивание медиа при ingest по политике (M7 A3): ``enrich_with_downloads``.

Best-effort: подходящие вложения качаются принимающим аккаунтом и получают
``local_path``; выключенная политика, фильтры, сбой границы или «нет медиа»
оставляют метаданные без файла (событие не теряется).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from telegram_fakes import FakeTelegramClient

from angarion.adapters.telegram.client import TransientSendError
from angarion.adapters.telegram.media import enrich_with_downloads
from angarion.config import MediaConfig
from angarion.domain.models import (
    AccountRef,
    Endpoint,
    MediaRef,
    Record,
    RecordKind,
)
from angarion.log import get_logger

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)
LOG = get_logger('test')


def _event(*media: MediaRef) -> Record:
    return Record(
        uid=uuid4(),
        kind=RecordKind.NEW,
        dedup_key='dk',
        origin='live',
        source=Endpoint(transport='telegram', address='-100123'),
        received_by=AccountRef(transport='telegram', account_id='main'),
        external_id='42',
        media=list(media),
        event_at=NOW,
        received_at=NOW,
    )


def _media(**over: object) -> MediaRef:
    fields: dict[str, object] = {'kind': 'photo', 'ref': '-100123:42', 'size': 1000}
    fields.update(over)
    return MediaRef.model_validate(fields)


async def test_disabled_policy_returns_event_unchanged() -> None:
    event = _event(_media())
    client = FakeTelegramClient()
    result = await enrich_with_downloads(
        event, client=client, policy=MediaConfig(download=False), log=LOG
    )
    assert result is event
    assert client.downloads == []


async def test_no_media_returns_event_unchanged() -> None:
    event = _event()
    client = FakeTelegramClient()
    result = await enrich_with_downloads(
        event, client=client, policy=MediaConfig(download=True), log=LOG
    )
    assert result is event
    assert client.downloads == []


async def test_downloads_matching_media_sets_local_path() -> None:
    event = _event(_media())
    client = FakeTelegramClient(
        download_effects=['/var/media/-100123_42.jpg']
    )
    result = await enrich_with_downloads(
        event,
        client=client,
        policy=MediaConfig(download=True, storage_dir='/var/media'),
        log=LOG,
    )
    assert result.media[0].local_path == '/var/media/-100123_42.jpg'
    assert client.downloads == [
        {'source_ref': '-100123:42', 'dest_dir': '/var/media'}
    ]


async def test_skips_kind_outside_whitelist() -> None:
    event = _event(_media(kind='photo'))
    client = FakeTelegramClient()
    policy = MediaConfig(download=True, allowed_kinds=frozenset({'video'}))
    result = await enrich_with_downloads(event, client=client, policy=policy, log=LOG)
    assert result.media[0].local_path is None
    assert client.downloads == []


async def test_download_returning_none_keeps_metadata() -> None:
    event = _event(_media())
    client = FakeTelegramClient(download_effects=[None])
    result = await enrich_with_downloads(
        event, client=client, policy=MediaConfig(download=True), log=LOG
    )
    assert result.media[0].local_path is None
    assert len(client.downloads) == 1


async def test_transient_failure_degrades_to_metadata() -> None:
    event = _event(_media())
    client = FakeTelegramClient(download_effects=[TransientSendError('net')])
    result = await enrich_with_downloads(
        event, client=client, policy=MediaConfig(download=True), log=LOG
    )
    assert result.media[0].local_path is None


async def test_already_downloaded_not_refetched() -> None:
    event = _event(_media(local_path='/already/x.jpg'))
    client = FakeTelegramClient()
    result = await enrich_with_downloads(
        event, client=client, policy=MediaConfig(download=True), log=LOG
    )
    assert result is event
    assert client.downloads == []
