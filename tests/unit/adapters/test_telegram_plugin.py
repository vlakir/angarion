"""
Контракт и сборка telegram-плагина (T005): матрица возможностей, схема
секции ``[accounts.*]`` (фаза 1) и фабрики listener/sender + общий пул
(фаза 5).
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from angarion.adapters.memory.plugin import STORAGE_BACKEND
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import MemorySessionStore
from angarion.adapters.telegram import plugin as telegram_plugin
from angarion.adapters.telegram.listener import TelegramListener
from angarion.adapters.telegram.plugin import (
    PLUGIN,
    TELEGRAM_CAPABILITIES,
    TelegramAccountConfig,
)
from angarion.adapters.telegram.registry import ClientRegistry
from angarion.adapters.telegram.sender import TelegramSender
from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.application.ingest import IngestService
from angarion.application.router import Router
from angarion.bootstrap import AdapterDeps
from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    StorageConfig,
)
from angarion.domain.errors import ConfigError
from angarion.domain.plugin import LoginContext


def _deps(settings: AngarionSettings | None = None) -> AdapterDeps:
    storage = STORAGE_BACKEND.make(StorageConfig())
    ingest = IngestService(
        dedup=storage.dedup,
        registry=storage.registry,
        router=Router([]),
        queue=MemoryQueue(),
        analytics=storage.analytics,
    )
    return AdapterDeps(
        ingest=ingest, storage=storage, settings=settings or AngarionSettings()
    )


def _account() -> TelegramAccountConfig:
    return TelegramAccountConfig.model_validate(
        {'transport': 'telegram', 'api_id': 2040, 'api_hash': 'hash'}
    )


def test_capabilities_match_spec() -> None:
    caps = TELEGRAM_CAPABILITIES
    assert caps.user_account is True
    assert caps.edit_events is True
    assert caps.delete_events is True
    assert caps.history_fetch is True
    assert caps.threads is True
    assert caps.push_transport == 'client'


def test_account_config_valid() -> None:
    cfg = TelegramAccountConfig.model_validate(
        {'transport': 'telegram', 'api_id': 2040, 'api_hash': 'abc123'}
    )
    assert cfg.api_id == 2040
    assert cfg.api_hash == 'abc123'


def test_account_config_coerces_api_id_from_env_string() -> None:
    """Env-override даёт строки — int должен скоарситься (§11)."""
    cfg = TelegramAccountConfig.model_validate(
        {'transport': 'telegram', 'api_id': '2040', 'api_hash': 'abc123'}
    )
    assert cfg.api_id == 2040


def test_account_config_rejects_wrong_messenger() -> None:
    with pytest.raises(ValidationError):
        TelegramAccountConfig.model_validate(
            {'transport': 'memory', 'api_id': 2040, 'api_hash': 'abc123'}
        )


def test_account_config_requires_api_credentials() -> None:
    with pytest.raises(ValidationError):
        TelegramAccountConfig.model_validate({'transport': 'telegram'})


def test_account_config_rejects_nonpositive_api_id() -> None:
    with pytest.raises(ValidationError):
        TelegramAccountConfig.model_validate(
            {'transport': 'telegram', 'api_id': 0, 'api_hash': 'abc123'}
        )


def test_account_config_rejects_empty_api_hash() -> None:
    with pytest.raises(ValidationError):
        TelegramAccountConfig.model_validate(
            {'transport': 'telegram', 'api_id': 2040, 'api_hash': ''}
        )


def test_account_config_forbids_extra_keys() -> None:
    with pytest.raises(ValidationError):
        TelegramAccountConfig.model_validate(
            {
                'transport': 'telegram',
                'api_id': 2040,
                'api_hash': 'abc123',
                'unexpected': 'x',
            }
        )


class TestPluginObject:
    def test_plugin_shape(self) -> None:
        assert PLUGIN.name == 'telegram'
        assert PLUGIN.capabilities is TELEGRAM_CAPABILITIES
        assert PLUGIN.account_config_model is TelegramAccountConfig

    def test_factories_return_protocol_objects(self) -> None:
        deps = _deps()
        accounts = {'main': _account()}
        listener = PLUGIN.make_listener(
            deps, accounts, [EndpointConfig(account='main', address='@g')]
        )
        sender = PLUGIN.make_sender(deps, accounts)
        assert isinstance(listener, TelegramListener)
        assert isinstance(sender, TelegramSender)

    def test_listener_and_sender_share_one_registry(self) -> None:
        """§12.1: один пул клиентов на listener+sender (мемо в deps.shared)."""
        deps = _deps()
        accounts = {'main': _account()}
        listener = PLUGIN.make_listener(deps, accounts, [])
        sender = PLUGIN.make_sender(deps, accounts)
        assert listener._pool is sender._pool
        assert isinstance(listener._pool, ClientRegistry)

    def test_config_wired_into_adapters(self) -> None:
        settings = AngarionSettings.model_validate(
            {
                'telegram': {'sender': {'chat_per_second': 5.0}},
                'catchup': {'interval': 30},
            }
        )
        deps = _deps(settings)
        accounts = {'main': _account()}
        listener = PLUGIN.make_listener(deps, accounts, [])
        sender = PLUGIN.make_sender(deps, accounts)
        assert listener._catchup_interval == 30
        assert sender._chat_per_second == 5.0

    def test_recent_poll_endpoints_union_from_pipelines(self) -> None:
        """T032 (A-1): per-pipeline recent_poll → объединение источников listener'у."""
        settings = AngarionSettings.model_validate(
            {
                'accounts': {
                    'main': {'transport': 'telegram', 'api_id': 2040, 'api_hash': 'h'}
                },
                'pipelines': {
                    'hot': {
                        'processor': 'passthrough',
                        'events': ['new'],
                        'sources': [{'account': 'main', 'address': '-100'}],
                        'targets': [{'account': 'main', 'address': '-300'}],
                        'recent_poll': True,
                    },
                    'cold': {
                        'processor': 'passthrough',
                        'events': ['new'],
                        'sources': [{'account': 'main', 'address': '-200'}],
                        'targets': [{'account': 'main', 'address': '-300'}],
                    },
                },
            }
        )
        ep_on = EndpointConfig(account='main', address='-100')
        ep_off = EndpointConfig(account='main', address='-200')
        listener = PLUGIN.make_listener(_deps(settings), {'main': _account()}, [
            ep_on,
            ep_off,
        ])
        # только источник recent_poll-пайплайна попадает в множество
        assert listener._recent_poll_endpoints == frozenset({ep_on})

    async def test_empty_session_key_with_stored_session_fails_fast(self) -> None:
        """Пустой ANGARION_SESSION_KEY + есть сессия → fail-fast при подключении."""
        deps = _deps()  # settings.session_key == ''
        await deps.storage.session.save('main', 'CIPHERTEXT')
        accounts = {'main': _account()}
        listener = PLUGIN.make_listener(
            deps, accounts, [EndpointConfig(account='main', address='@g')]
        )
        with pytest.raises(ConfigError, match='ANGARION_SESSION_KEY'):
            await listener._pool.connect_all()


class TestLogin:
    """Шов логина перенесён в плагин (M7 B1): make_login → зашифр. сессия."""

    KEY = Fernet.generate_key().decode()  # рантайм-ключ, не хардкод

    async def test_make_login_saves_encrypted_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def fake_login(api_id: int, api_hash: str) -> str:
            assert (api_id, api_hash) == (2040, 'hash')
            return 'STRING-SESSION'

        monkeypatch.setattr(
            telegram_plugin, 'login_and_export_session', fake_login
        )
        inner = MemorySessionStore()
        assert PLUGIN.make_login is not None
        await PLUGIN.make_login(
            LoginContext(
                account_id='main',
                config=_account(),
                session=inner,
                session_key=self.KEY,
            )
        )
        loaded = await EncryptedSessionStore(inner, self.KEY).load('main')
        assert loaded == 'STRING-SESSION'
        raw = await inner.load('main')
        assert raw is not None
        assert raw != 'STRING-SESSION'  # ciphertext
