"""
EncryptedSessionStore (Q2 спеки T005): Fernet-шифрование сессий at-rest.

Декоратор сам является ``SessionStorePort`` — прогоняем его через
публичный контрактный набор (обёрнутый InMemory + валидный ключ), плюс
крипто-специфику: ciphertext в хранилище, fail-fast без ключа, ошибка
расшифровки чужим ключом.
"""

from __future__ import annotations

import pytest
from angarion.testing import SessionStoreContract

from angarion.adapters.memory.storage import MemorySessionStore
from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.domain import ports
from angarion.domain.errors import ConfigError

KEY_A = 'BjiQVGDpc9wzvfC93-ZP51EPh7Tt0fQXINZu9Xetu9I='
KEY_B = 'Rxm8Acq1ncueccwbMtbkgFqbSDAyEqHzWtvM6WrbMXw='


class TestEncryptedSessionStoreContract(SessionStoreContract):
    """С валидным ключом декоратор — полноценный ``SessionStorePort``."""

    @pytest.fixture
    def session_store(self) -> EncryptedSessionStore:
        return EncryptedSessionStore(MemorySessionStore(), KEY_A)


def test_satisfies_session_store_port() -> None:
    store = EncryptedSessionStore(MemorySessionStore(), KEY_A)
    assert isinstance(store, ports.SessionStorePort)


async def test_at_rest_is_ciphertext_not_plaintext() -> None:
    inner = MemorySessionStore()
    store = EncryptedSessionStore(inner, KEY_A)
    await store.save('acc1', 'secret-session')
    stored = await inner.load('acc1')
    assert stored is not None
    assert stored != 'secret-session'
    assert 'secret-session' not in stored


async def test_other_key_instance_decrypts_same_inner() -> None:
    inner = MemorySessionStore()
    await EncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    reader = EncryptedSessionStore(inner, KEY_A)
    assert await reader.load('acc1') == 'secret-session'


async def test_wrong_key_fails_to_decrypt() -> None:
    inner = MemorySessionStore()
    await EncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    rotated = EncryptedSessionStore(inner, KEY_B)
    with pytest.raises(ConfigError, match='расшифровать'):
        await rotated.load('acc1')


def test_invalid_nonempty_key_is_config_error() -> None:
    with pytest.raises(ConfigError, match='ANGARION_SESSION_KEY невалиден'):
        EncryptedSessionStore(MemorySessionStore(), 'not-a-valid-fernet-key')


async def test_missing_key_load_absent_returns_none() -> None:
    store = EncryptedSessionStore(MemorySessionStore(), '')
    assert await store.load('acc1') is None


async def test_missing_key_save_fails_fast() -> None:
    store = EncryptedSessionStore(MemorySessionStore(), '')
    with pytest.raises(ConfigError, match='не задан'):
        await store.save('acc1', 'secret-session')


async def test_missing_key_load_existing_fails_fast() -> None:
    inner = MemorySessionStore()
    await EncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    store = EncryptedSessionStore(inner, '')
    with pytest.raises(ConfigError, match='не задан'):
        await store.load('acc1')


async def test_ensure_ready_missing_key_with_sessions_fails() -> None:
    inner = MemorySessionStore()
    await EncryptedSessionStore(inner, KEY_A).save('acc1', 'secret-session')
    store = EncryptedSessionStore(inner, '')
    with pytest.raises(ConfigError, match='есть'):
        await store.ensure_ready()


async def test_ensure_ready_missing_key_no_sessions_ok() -> None:
    await EncryptedSessionStore(MemorySessionStore(), '').ensure_ready()


async def test_ensure_ready_with_key_ok() -> None:
    inner = MemorySessionStore()
    store = EncryptedSessionStore(inner, KEY_A)
    await store.save('acc1', 'secret-session')
    await store.ensure_ready()
