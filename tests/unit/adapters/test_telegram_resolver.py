"""Резолв сущностей с прогревом get_dialogs (Q4, Q5)."""

from __future__ import annotations

import pytest
from telegram_fakes import FakeTelegramClient

from angarion.adapters.memory.storage import MemoryAnalytics
from angarion.adapters.telegram.resolver import resolve_sources
from angarion.config import EndpointConfig
from angarion.log import get_logger

pytestmark = pytest.mark.asyncio


def _ep(account: str, chat_id: str) -> EndpointConfig:
    return EndpointConfig(account=account, chat_id=chat_id)


async def _resolve(clients, sources):  # type: ignore[no-untyped-def]
    analytics = MemoryAnalytics()
    resolved = await resolve_sources(
        clients=clients,
        sources=sources,
        analytics=analytics,
        log=get_logger('test'),
    )
    return resolved, analytics


async def test_warms_each_client_once() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    await _resolve({'main': client}, [_ep('main', '@grp')])
    assert client.warmed == 1


async def test_resolves_to_signed_chat_id() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    resolved, _ = await _resolve({'main': client}, [_ep('main', '@grp')])
    assert len(resolved) == 1
    assert resolved[0].account_id == 'main'
    assert resolved[0].chat_id == -100123
    assert resolved[0].thread_id is None
    assert resolved[0].source_key == 'telegram:main:-100123'


async def test_account_without_sources_is_empty() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    resolved, _ = await _resolve({'main': client}, [])
    assert resolved == []


async def test_thread_id_carried_into_source_key() -> None:
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    ep = EndpointConfig(account='main', chat_id='@grp', thread_id='55')
    resolved, _ = await _resolve({'main': client}, [ep])
    assert resolved[0].thread_id == '55'
    assert resolved[0].source_key == 'telegram:main:-100123:55'


async def test_failed_source_degrades_not_raises() -> None:
    client = FakeTelegramClient(peer_ids={'@ok': -100123}, fail_peers=('@bad',))
    resolved, analytics = await _resolve(
        {'main': client}, [_ep('main', '@ok'), _ep('main', '@bad')]
    )
    assert [rs.chat_id for rs in resolved] == [-100123]
    unavailable = await analytics.recent(kind='source_unavailable')
    assert len(unavailable) == 1
    assert unavailable[0].payload['chat_id'] == '@bad'
    assert unavailable[0].payload['account'] == 'main'


async def test_source_for_unknown_account_skipped() -> None:
    """Защитная ветка: источник ссылается на аккаунт без клиента."""
    client = FakeTelegramClient(peer_ids={'@grp': -100123})
    resolved, _ = await _resolve({'main': client}, [_ep('ghost', '@grp')])
    assert resolved == []


async def test_recent_poll_flag_carried_for_enabled_endpoints() -> None:
    """T032: recent_poll переносится на ResolvedSource только для включённых."""
    client = FakeTelegramClient(peer_ids={'@a': -100111, '@b': -100222})
    ep_a, ep_b = _ep('main', '@a'), _ep('main', '@b')
    resolved = await resolve_sources(
        clients={'main': client},
        sources=[ep_a, ep_b],
        analytics=MemoryAnalytics(),
        log=get_logger('test'),
        recent_poll_endpoints=frozenset({ep_a}),
    )
    flags = {rs.chat_id: rs.recent_poll for rs in resolved}
    assert flags == {-100111: True, -100222: False}


async def test_multi_account_resolution() -> None:
    a = FakeTelegramClient(peer_ids={'@a': -100111})
    b = FakeTelegramClient(peer_ids={'@b': -100222})
    resolved, _ = await _resolve(
        {'acc_a': a, 'acc_b': b}, [_ep('acc_a', '@a'), _ep('acc_b', '@b')]
    )
    assert {(rs.account_id, rs.chat_id) for rs in resolved} == {
        ('acc_a', -100111),
        ('acc_b', -100222),
    }
