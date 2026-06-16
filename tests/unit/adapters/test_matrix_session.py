"""
MatrixSession (сериализация непрозрачной сессии) и
MatrixEncryptedSessionStore (Fernet-шифрование at-rest, M7 B1).

Декоратор сам является ``SessionStorePort`` — прогоняем его через
публичный контрактный набор, плюс крипто-специфику (ciphertext в
хранилище, fail-fast без ключа, ошибка расшифровки чужим ключом).
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet
from angarion.testing import SessionStoreContract

from angarion.adapters.matrix.session import (
    MatrixEncryptedSessionStore,
    MatrixSession,
)
from angarion.adapters.memory.storage import MemorySessionStore
from angarion.domain import ports
from angarion.domain.errors import ConfigError

# Тестовые ключи генерируются в рантайме (не хардкод — детектор секретов
# не должен принимать фикстуры за реальные ключи); важна лишь их валидность
# и различие KEY_A != KEY_B.
KEY_A = Fernet.generate_key().decode()
KEY_B = Fernet.generate_key().decode()


def _session() -> MatrixSession:
    return MatrixSession(
        homeserver='https://matrix.example',
        user_id='@bot:matrix.example',
        device_id='DEV123',
        access_token='tok-fake-token',
    )


def test_session_string_roundtrip() -> None:
    session = _session()
    restored = MatrixSession.from_session_string(session.to_session_string())
    assert restored == session


def test_session_string_is_opaque_text() -> None:
    raw = _session().to_session_string()
    assert isinstance(raw, str)
    assert 'tok-fake-token' in raw  # сериализация без потери токена
    assert 'DEV123' in raw


def test_from_session_string_rejects_garbage() -> None:
    with pytest.raises(ValueError, match='validation error|JSON'):
        MatrixSession.from_session_string('not-json')


class TestEncryptedSessionStoreContract(SessionStoreContract):
    """С валидным ключом декоратор — полноценный ``SessionStorePort``."""

    @pytest.fixture
    def session_store(self) -> MatrixEncryptedSessionStore:
        return MatrixEncryptedSessionStore(MemorySessionStore(), KEY_A)


def test_satisfies_session_store_port() -> None:
    store = MatrixEncryptedSessionStore(MemorySessionStore(), KEY_A)
    assert isinstance(store, ports.SessionStorePort)


async def test_at_rest_is_ciphertext_not_plaintext() -> None:
    inner = MemorySessionStore()
    store = MatrixEncryptedSessionStore(inner, KEY_A)
    await store.save('acc1', 'secret-session')
    stored = await inner.load('acc1')
    assert stored is not None
    assert stored != 'secret-session'
    assert 'secret-session' not in stored


async def test_roundtrip_decrypts() -> None:
    inner = MemorySessionStore()
    await MatrixEncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    reader = MatrixEncryptedSessionStore(inner, KEY_A)
    assert await reader.load('acc1') == 'secret-session'


async def test_wrong_key_fails_to_decrypt() -> None:
    inner = MemorySessionStore()
    await MatrixEncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    rotated = MatrixEncryptedSessionStore(inner, KEY_B)
    with pytest.raises(ConfigError, match='расшифровать'):
        await rotated.load('acc1')


def test_invalid_nonempty_key_is_config_error() -> None:
    with pytest.raises(ConfigError, match='ANGARION_SESSION_KEY невалиден'):
        MatrixEncryptedSessionStore(MemorySessionStore(), 'not-a-valid-key')


async def test_missing_key_load_absent_returns_none() -> None:
    store = MatrixEncryptedSessionStore(MemorySessionStore(), '')
    assert await store.load('acc1') is None


async def test_missing_key_save_fails_fast() -> None:
    store = MatrixEncryptedSessionStore(MemorySessionStore(), '')
    with pytest.raises(ConfigError, match='не задан'):
        await store.save('acc1', 'secret-session')


async def test_missing_key_load_existing_fails_fast() -> None:
    inner = MemorySessionStore()
    await MatrixEncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    store = MatrixEncryptedSessionStore(inner, '')
    with pytest.raises(ConfigError, match='не задан'):
        await store.load('acc1')


async def test_ensure_ready_missing_key_with_sessions_fails() -> None:
    inner = MemorySessionStore()
    await MatrixEncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    store = MatrixEncryptedSessionStore(inner, '')
    with pytest.raises(ConfigError, match='есть'):
        await store.ensure_ready()


async def test_ensure_ready_missing_key_no_sessions_ok() -> None:
    await MatrixEncryptedSessionStore(MemorySessionStore(), '').ensure_ready()


async def test_ensure_ready_with_key_ok() -> None:
    inner = MemorySessionStore()
    store = MatrixEncryptedSessionStore(inner, KEY_A)
    await store.save('acc1', 'secret-session')
    await store.ensure_ready()
