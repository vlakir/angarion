"""Контрактный набор ``StateStorePort`` (§5, §10.3 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from angarion.domain.ports import StateStorePort


class StateStoreContract:
    """
    Поведенческая спецификация KV-хранилища состояния процессоров:
    значения — JSON-строки, ключи неймспейсятся по пайплайну,
    ``keys()`` — отсортированы и фильтруются префиксом.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def state(self) -> StateStorePort:
        raise NotImplementedError

    async def test_get_missing_returns_none(self, state: StateStorePort) -> None:
        assert await state.get('digest', 'k') is None

    async def test_set_get_roundtrip(self, state: StateStorePort) -> None:
        await state.set('digest', 'k', '{"n": 1}')
        assert await state.get('digest', 'k') == '{"n": 1}'

    async def test_set_overwrites(self, state: StateStorePort) -> None:
        await state.set('digest', 'k', '{"n": 1}')
        await state.set('digest', 'k', '{"n": 2}')
        assert await state.get('digest', 'k') == '{"n": 2}'

    async def test_delete_removes_key(self, state: StateStorePort) -> None:
        await state.set('digest', 'k', '{"n": 1}')
        await state.delete('digest', 'k')
        assert await state.get('digest', 'k') is None

    async def test_delete_missing_is_noop(self, state: StateStorePort) -> None:
        await state.delete('digest', 'k')
        assert await state.get('digest', 'k') is None

    async def test_keys_sorted_and_filtered_by_prefix(
        self, state: StateStorePort
    ) -> None:
        await state.set('digest', 'seen:2', 'b')
        await state.set('digest', 'seen:1', 'a')
        await state.set('digest', 'other', 'c')
        assert await state.keys('digest', prefix='seen:') == ['seen:1', 'seen:2']
        assert await state.keys('digest') == ['other', 'seen:1', 'seen:2']

    async def test_namespaces_isolated(self, state: StateStorePort) -> None:
        await state.set('digest', 'k', 'a')
        await state.set('relay', 'k', 'b')
        await state.delete('digest', 'k')
        assert await state.get('digest', 'k') is None
        assert await state.get('relay', 'k') == 'b'
        assert await state.keys('relay') == ['k']
