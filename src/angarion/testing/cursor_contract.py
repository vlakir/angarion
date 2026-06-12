"""Контрактный набор ``CursorStorePort`` (§5, §9.2 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from angarion.testing.factories import NOW, SOURCE_KEY, make_cursor

if TYPE_CHECKING:
    from angarion.domain.ports import CursorStorePort


class CursorStoreContract:
    """
    Поведенческая спецификация хранилища курсоров catch-up:
    payload непрозрачен для ядра, ключ — ``source_key``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def cursors(self) -> CursorStorePort:
        raise NotImplementedError

    async def test_load_missing_returns_none(self, cursors: CursorStorePort) -> None:
        assert await cursors.load(SOURCE_KEY) is None

    async def test_save_load_roundtrip(self, cursors: CursorStorePort) -> None:
        cursor = make_cursor()
        await cursors.save(cursor)
        assert await cursors.load(SOURCE_KEY) == cursor

    async def test_save_overwrites_previous(self, cursors: CursorStorePort) -> None:
        await cursors.save(make_cursor())
        updated = make_cursor(
            payload={'last_seen_external_id': '99', 'last_scan_at': '2026-06-11'},
            updated_at=NOW + timedelta(minutes=1),
        )
        await cursors.save(updated)
        assert await cursors.load(SOURCE_KEY) == updated

    async def test_cursors_scoped_by_source_key(self, cursors: CursorStorePort) -> None:
        first = make_cursor()
        second = make_cursor(
            source_key='memory:acc1:-100999', payload={'sync_token': 's72594'}
        )
        await cursors.save(first)
        await cursors.save(second)
        assert await cursors.load(SOURCE_KEY) == first
        assert await cursors.load('memory:acc1:-100999') == second
