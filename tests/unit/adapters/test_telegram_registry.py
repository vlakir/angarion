"""ClientRegistry (M3, фаза 5): пул Telethon-клиентов per account (§12.1)."""

from __future__ import annotations

import pytest

from angarion.adapters.memory.storage import MemorySessionStore
from angarion.adapters.telegram.registry import ClientRegistry
from angarion.domain.errors import ConfigError
from telegram_fakes import FakeTelegramClient


class FakeConnectedClient(FakeTelegramClient):
    """FakeTelegramClient + протокол ConnectedClient (учёт disconnect)."""

    def __init__(self) -> None:
        super().__init__()
        self.disconnected = 0

    async def disconnect(self) -> None:
        self.disconnected += 1


def make_registry(
    *, env_sessions: dict[str, str] | None = None
) -> tuple[ClientRegistry, MemorySessionStore, list[tuple[int, str, str]]]:
    store = MemorySessionStore()
    calls: list[tuple[int, str, str]] = []

    async def connect(
        api_id: int, api_hash: str, session_string: str
    ) -> FakeConnectedClient:
        calls.append((api_id, api_hash, session_string))
        return FakeConnectedClient()

    registry = ClientRegistry(
        credentials={'main': (12345, 'hash')},
        session_store=store,
        env_sessions=env_sessions or {},
        connect=connect,
    )
    return registry, store, calls


class TestConnectAll:
    async def test_connects_each_account_from_loaded_session(self) -> None:
        registry, store, calls = make_registry()
        await store.save('main', 'SESSION-STRING')
        await registry.connect_all()
        assert calls == [(12345, 'hash', 'SESSION-STRING')]
        assert set(registry.clients) == {'main'}

    async def test_missing_session_fails_with_login_hint(self) -> None:
        registry, _store, _calls = make_registry()
        with pytest.raises(ConfigError, match='login'):
            await registry.connect_all()

    async def test_connect_all_is_idempotent(self) -> None:
        registry, store, calls = make_registry()
        await store.save('main', 'S')
        await registry.connect_all()
        await registry.connect_all()
        assert len(calls) == 1

    async def test_env_session_used_directly_without_store(self) -> None:
        """T042: StringSession из env подключается напрямую, минуя app.db."""
        registry, _store, calls = make_registry(env_sessions={'main': 'ENV-STRING'})
        await registry.connect_all()
        # store пуст, но ConfigError нет — сессия пришла из env
        assert calls == [(12345, 'hash', 'ENV-STRING')]

    async def test_env_session_takes_priority_over_stored(self) -> None:
        """T042: env-сессия имеет приоритет над сохранённой в app.db."""
        registry, store, calls = make_registry(env_sessions={'main': 'ENV-STRING'})
        await store.save('main', 'STORED-STRING')
        await registry.connect_all()
        assert calls == [(12345, 'hash', 'ENV-STRING')]

    async def test_falls_back_to_store_without_env_session(self) -> None:
        """T042: без env-сессии — прежний путь из app.db (дефолт не сломан)."""
        registry, store, calls = make_registry(env_sessions={})
        await store.save('main', 'STORED-STRING')
        await registry.connect_all()
        assert calls == [(12345, 'hash', 'STORED-STRING')]


class TestDisconnectAll:
    async def test_disconnects_every_client_and_clears(self) -> None:
        registry, store, _calls = make_registry()
        await store.save('main', 'S')
        await registry.connect_all()
        client = registry.clients['main']
        assert isinstance(client, FakeConnectedClient)
        await registry.disconnect_all()
        assert client.disconnected == 1
        assert dict(registry.clients) == {}

    async def test_disconnect_without_connect_is_noop(self) -> None:
        registry, _store, _calls = make_registry()
        await registry.disconnect_all()
        assert dict(registry.clients) == {}
