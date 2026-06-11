"""Контрактный набор ``DedupStorePort`` (§5, §7.2 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

import pytest
from factories import FAR_FUTURE, LONG_AGO

from angarion.domain.ports import DedupStorePort


class DedupStoreContract:
    """
    Поведенческая спецификация входной дедупликации: «отметить, если
    не было» (True = новый ключ), TTL-очистка ``prune()`` (A-7, §17.3).

    Выходная идемпотентность после C-9 живёт в outbox
    (``OutboxContract``), не здесь.
    """

    @pytest.fixture
    def dedup(self) -> DedupStorePort:
        raise NotImplementedError

    async def test_mark_inbound_first_time_true(self, dedup: DedupStorePort) -> None:
        assert await dedup.mark_inbound('k1') is True

    async def test_mark_inbound_duplicate_false(self, dedup: DedupStorePort) -> None:
        await dedup.mark_inbound('k1')
        assert await dedup.mark_inbound('k1') is False

    async def test_mark_inbound_keys_independent(
        self, dedup: DedupStorePort
    ) -> None:
        await dedup.mark_inbound('k1')
        assert await dedup.mark_inbound('k2') is True

    async def test_prune_removes_old_marks(self, dedup: DedupStorePort) -> None:
        await dedup.mark_inbound('k1')
        assert await dedup.prune(older_than=FAR_FUTURE) == 1
        assert await dedup.mark_inbound('k1') is True

    async def test_prune_keeps_fresh_marks(self, dedup: DedupStorePort) -> None:
        await dedup.mark_inbound('k1')
        assert await dedup.prune(older_than=LONG_AGO) == 0
        assert await dedup.mark_inbound('k1') is False
