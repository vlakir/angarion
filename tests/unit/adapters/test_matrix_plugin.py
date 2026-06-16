"""
Контракт и каркас matrix-плагина (M7 B1, T010): матрица возможностей,
схема ``[accounts.*]``, парольный login-шов и заглушки listener/sender.
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from angarion.adapters.matrix import plugin as matrix_plugin
from angarion.adapters.matrix.plugin import (
    MATRIX_CAPABILITIES,
    PLUGIN,
    MatrixAccountConfig,
)
from angarion.adapters.matrix.session import (
    MatrixEncryptedSessionStore,
    MatrixSession,
)
from angarion.adapters.memory.storage import MemorySessionStore
from angarion.domain.errors import NotSupportedError
from angarion.domain.plugin import LoginContext

KEY = Fernet.generate_key().decode()  # рантайм-ключ, не хардкод (см. session-тест)


def _account(**overrides: object) -> MatrixAccountConfig:
    data: dict[str, object] = {
        'messenger': 'matrix',
        'homeserver': 'https://matrix.example',
        'user_id': '@bot:matrix.example',
    }
    data.update(overrides)
    return MatrixAccountConfig.model_validate(data)


def test_capabilities_full_profile() -> None:
    caps = MATRIX_CAPABILITIES
    assert caps.user_account is True
    assert caps.edit_events is True
    assert caps.delete_events is True
    assert caps.history_fetch is True
    assert caps.threads is True
    assert caps.push_transport == 'client'


def test_account_config_valid_defaults_device_name() -> None:
    cfg = _account()
    assert cfg.homeserver == 'https://matrix.example'
    assert cfg.user_id == '@bot:matrix.example'
    assert cfg.device_name == 'angarion'


def test_account_config_rejects_wrong_messenger() -> None:
    with pytest.raises(ValidationError):
        MatrixAccountConfig.model_validate(
            {'messenger': 'telegram', 'homeserver': 'h', 'user_id': 'u'}
        )


def test_account_config_requires_homeserver_and_user() -> None:
    with pytest.raises(ValidationError):
        MatrixAccountConfig.model_validate({'messenger': 'matrix'})


def test_account_config_rejects_empty_user_id() -> None:
    with pytest.raises(ValidationError):
        MatrixAccountConfig.model_validate(
            {'messenger': 'matrix', 'homeserver': 'h', 'user_id': ''}
        )


def test_account_config_forbids_extra_keys() -> None:
    """Пароль (секрет) в секцию не кладётся — extra-ключи запрещены."""
    with pytest.raises(ValidationError):
        MatrixAccountConfig.model_validate(
            {
                'messenger': 'matrix',
                'homeserver': 'h',
                'user_id': 'u',
                'password': 'secret',
            }
        )


class TestPluginObject:
    def test_plugin_shape(self) -> None:
        assert PLUGIN.name == 'matrix'
        assert PLUGIN.capabilities is MATRIX_CAPABILITIES
        assert PLUGIN.account_config_model is MatrixAccountConfig
        assert PLUGIN.make_login is not None

    def test_make_listener_is_stub_until_b2(self) -> None:
        with pytest.raises(NotSupportedError, match='B2'):
            PLUGIN.make_listener(object(), {}, [])

    def test_make_sender_is_stub_until_b3(self) -> None:
        with pytest.raises(NotSupportedError, match='B3'):
            PLUGIN.make_sender(object(), {})


class TestLogin:
    async def test_saves_encrypted_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        inner = MemorySessionStore()
        session = MatrixSession(
            homeserver='https://matrix.example',
            user_id='@bot:matrix.example',
            device_id='DEV1',
            access_token='tok-fake-xyz',
        )

        async def fake_login(
            homeserver: str, user_id: str, password: str, device_name: str
        ) -> str:
            assert (homeserver, user_id, password, device_name) == (
                'https://matrix.example',
                '@bot:matrix.example',
                'pw-from-env',
                'angarion',
            )
            return session.to_session_string()

        monkeypatch.setenv('ANGARION_MATRIX_PASSWORD', 'pw-from-env')
        monkeypatch.setattr(matrix_plugin, 'password_login', fake_login)
        assert PLUGIN.make_login is not None
        await PLUGIN.make_login(
            LoginContext(
                account_id='main',
                config=_account(),
                session=inner,
                session_key=KEY,
            )
        )
        # at-rest зашифровано, читается тем же ключом
        store = MatrixEncryptedSessionStore(inner, KEY)
        loaded = await store.load('main')
        assert loaded is not None
        assert MatrixSession.from_session_string(loaded) == session
        raw = await inner.load('main')
        assert raw is not None
        assert 'tok-fake-xyz' not in raw  # ciphertext, не плейнтекст

    async def test_password_resolves_from_getpass_without_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv('ANGARION_MATRIX_PASSWORD', raising=False)
        monkeypatch.setattr(matrix_plugin, 'getpass', lambda _prompt: 'typed-pw')
        captured: list[str] = []

        async def fake_login(
            _h: str, _u: str, password: str, _d: str
        ) -> str:
            captured.append(password)
            return MatrixSession(
                homeserver='h', user_id='u', device_id='d', access_token='t'
            ).to_session_string()

        monkeypatch.setattr(matrix_plugin, 'password_login', fake_login)
        assert PLUGIN.make_login is not None
        await PLUGIN.make_login(
            LoginContext(
                account_id='main',
                config=_account(),
                session=MemorySessionStore(),
                session_key=KEY,
            )
        )
        assert captured == ['typed-pw']
