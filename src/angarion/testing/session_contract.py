"""Контрактный набор ``SessionStorePort`` (§5, §12.1 ТЗ; M3, T005)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from angarion.domain.ports import SessionStorePort


class SessionStoreContract:
    """
    Поведенческая спецификация хранилища сессий аккаунтов: строка
    сессии непрозрачна, ключ — ``account_id``, ``save`` перезаписывает,
    ``account_ids`` отсортированы.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def session_store(self) -> SessionStorePort:
        raise NotImplementedError

    async def test_load_missing_returns_none(
        self, session_store: SessionStorePort
    ) -> None:
        assert await session_store.load('acc1') is None

    async def test_save_load_roundtrip(self, session_store: SessionStorePort) -> None:
        await session_store.save('acc1', 'session-string-xyz')
        assert await session_store.load('acc1') == 'session-string-xyz'

    async def test_save_overwrites_previous(
        self, session_store: SessionStorePort
    ) -> None:
        await session_store.save('acc1', 'old')
        await session_store.save('acc1', 'new')
        assert await session_store.load('acc1') == 'new'

    async def test_sessions_scoped_by_account(
        self, session_store: SessionStorePort
    ) -> None:
        await session_store.save('acc1', 'session-a')
        await session_store.save('acc2', 'session-b')
        assert await session_store.load('acc1') == 'session-a'
        assert await session_store.load('acc2') == 'session-b'

    async def test_account_ids_empty_initially(
        self, session_store: SessionStorePort
    ) -> None:
        assert await session_store.account_ids() == []

    async def test_account_ids_sorted(self, session_store: SessionStorePort) -> None:
        await session_store.save('beta', 'sb')
        await session_store.save('alpha', 'sa')
        assert await session_store.account_ids() == ['alpha', 'beta']
