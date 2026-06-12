"""Контрактный набор ``AnalyticsPort`` (§5 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from angarion.testing.factories import NOW, make_analytics_event

if TYPE_CHECKING:
    from angarion.domain.ports import AnalyticsPort


class AnalyticsContract:
    """
    Поведенческая спецификация аналитики: append-only ``record``,
    read-сторона ``recent`` (новые первыми) / ``counts_by_kind``,
    ретеншн-очистка ``prune()`` (A-7, §17.3).
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def analytics(self) -> AnalyticsPort:
        raise NotImplementedError

    async def test_recent_returns_newest_first(self, analytics: AnalyticsPort) -> None:
        first = make_analytics_event()
        second = make_analytics_event(kind='processed', at=NOW + timedelta(seconds=1))
        await analytics.record(first)
        await analytics.record(second)
        assert await analytics.recent() == [second, first]

    async def test_recent_respects_limit(self, analytics: AnalyticsPort) -> None:
        for n in range(3):
            await analytics.record(make_analytics_event(at=NOW + timedelta(seconds=n)))
        limit = 2
        assert len(await analytics.recent(limit=limit)) == limit

    async def test_recent_filters_by_kind(self, analytics: AnalyticsPort) -> None:
        ingested = make_analytics_event()
        await analytics.record(ingested)
        await analytics.record(make_analytics_event(kind='processed'))
        assert await analytics.recent(kind='ingested') == [ingested]

    async def test_recent_filters_by_pipeline(self, analytics: AnalyticsPort) -> None:
        digest = make_analytics_event(pipeline='digest')
        await analytics.record(digest)
        await analytics.record(make_analytics_event(pipeline='relay'))
        assert await analytics.recent(pipeline='digest') == [digest]

    async def test_counts_by_kind_since(self, analytics: AnalyticsPort) -> None:
        await analytics.record(make_analytics_event(at=NOW - timedelta(days=2)))
        await analytics.record(make_analytics_event())
        await analytics.record(make_analytics_event())
        await analytics.record(make_analytics_event(kind='processed'))
        counts = await analytics.counts_by_kind(since=NOW - timedelta(days=1))
        assert counts == {'ingested': 2, 'processed': 1}

    async def test_counts_by_kind_filters_by_pipeline(
        self, analytics: AnalyticsPort
    ) -> None:
        await analytics.record(make_analytics_event(pipeline='digest'))
        await analytics.record(make_analytics_event(pipeline='relay'))
        counts = await analytics.counts_by_kind(
            since=NOW - timedelta(days=1), pipeline='digest'
        )
        assert counts == {'ingested': 1}

    async def test_prune_removes_old_events(self, analytics: AnalyticsPort) -> None:
        fresh = make_analytics_event()
        await analytics.record(make_analytics_event(at=NOW - timedelta(days=100)))
        await analytics.record(fresh)
        assert await analytics.prune(older_than=NOW - timedelta(days=90)) == 1
        assert await analytics.recent() == [fresh]
