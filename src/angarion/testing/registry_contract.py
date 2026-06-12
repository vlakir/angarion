"""Контрактный набор ``MessageRegistryPort`` (§5, §9.2 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from angarion.domain.models import RegistryDelta, RegistryOutcome, RegistryVersion
from angarion.testing.factories import NOW, SOURCE_KEY, make_record

if TYPE_CHECKING:
    from angarion.domain.ports import MessageRegistryPort


class MessageRegistryContract:
    """
    Поведенческая спецификация реестра сообщений: четыре исхода
    upsert (включая staleness-guard — A-3), архив вытесненных версий,
    мягкое удаление, ``known_ids`` и очистка окна ``prune()``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def registry(self) -> MessageRegistryPort:
        raise NotImplementedError

    async def test_upsert_new(self, registry: MessageRegistryPort) -> None:
        rec = make_record()
        delta = await registry.upsert(rec)
        assert delta == RegistryDelta(outcome=RegistryOutcome.IS_NEW)
        assert await registry.get(SOURCE_KEY, '42') == rec

    async def test_get_unknown_returns_none(
        self, registry: MessageRegistryPort
    ) -> None:
        assert await registry.get(SOURCE_KEY, '404') is None

    async def test_upsert_same_hash_unchanged(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record())
        delta = await registry.upsert(make_record(edit_ts=NOW + timedelta(seconds=5)))
        assert delta == RegistryDelta(outcome=RegistryOutcome.UNCHANGED)
        assert await registry.versions(SOURCE_KEY, '42') == []

    async def test_upsert_changed_text_archives_version(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record())
        edited = make_record(
            text='hello!',
            content_hash='hash-b',
            edit_ts=NOW + timedelta(minutes=1),
        )
        delta = await registry.upsert(edited)
        assert delta == RegistryDelta(
            outcome=RegistryOutcome.TEXT_CHANGED, previous_text='hello'
        )
        assert await registry.get(SOURCE_KEY, '42') == edited
        assert await registry.versions(SOURCE_KEY, '42') == [
            RegistryVersion(text='hello', content_hash='hash-a', recorded_at=NOW)
        ]

    async def test_upsert_stale_is_ignored(self, registry: MessageRegistryPort) -> None:
        current = make_record(text='hello!', content_hash='hash-b', edit_ts=NOW)
        await registry.upsert(current)
        late_original = make_record(event_at=NOW - timedelta(minutes=5))
        delta = await registry.upsert(late_original)
        assert delta == RegistryDelta(outcome=RegistryOutcome.STALE)
        assert await registry.get(SOURCE_KEY, '42') == current
        assert await registry.versions(SOURCE_KEY, '42') == []

    async def test_edit_back_to_previous_text_is_text_changed(
        self, registry: MessageRegistryPort
    ) -> None:
        """§9.4: A→B→A — на уровне реестра это полноценная правка."""
        await registry.upsert(make_record())
        await registry.upsert(
            make_record(
                text='v2', content_hash='hash-b', edit_ts=NOW + timedelta(minutes=1)
            )
        )
        delta = await registry.upsert(make_record(edit_ts=NOW + timedelta(minutes=2)))
        assert delta == RegistryDelta(
            outcome=RegistryOutcome.TEXT_CHANGED, previous_text='v2'
        )
        versions = await registry.versions(SOURCE_KEY, '42')
        assert [v.text for v in versions] == ['hello', 'v2']

    async def test_versions_oldest_first(self, registry: MessageRegistryPort) -> None:
        await registry.upsert(make_record())
        await registry.upsert(
            make_record(
                text='v2', content_hash='hash-b', edit_ts=NOW + timedelta(minutes=1)
            )
        )
        await registry.upsert(
            make_record(
                text='v3', content_hash='hash-c', edit_ts=NOW + timedelta(minutes=2)
            )
        )
        versions = await registry.versions(SOURCE_KEY, '42')
        assert [v.text for v in versions] == ['hello', 'v2']
        assert versions[0].recorded_at < versions[1].recorded_at

    async def test_mark_deleted_returns_last_state(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record())
        deleted = await registry.mark_deleted(SOURCE_KEY, '42')
        assert deleted is not None
        assert deleted.text == 'hello'
        assert deleted.deleted_at is not None
        stored = await registry.get(SOURCE_KEY, '42')
        assert stored is not None
        assert stored.deleted_at is not None

    async def test_mark_deleted_unknown_returns_none(
        self, registry: MessageRegistryPort
    ) -> None:
        assert await registry.mark_deleted(SOURCE_KEY, '404') is None

    async def test_mark_deleted_is_idempotent(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record())
        first = await registry.mark_deleted(SOURCE_KEY, '42')
        second = await registry.mark_deleted(SOURCE_KEY, '42')
        assert second == first

    async def test_known_ids_filters_min_id_numerically(
        self, registry: MessageRegistryPort
    ) -> None:
        """Числовые id сравниваются как числа: '9' < '10' (не лексикографика)."""
        for external_id in ('9', '10', '11'):
            await registry.upsert(make_record(external_id=external_id))
        assert await registry.known_ids(SOURCE_KEY, min_id='10') == {'10', '11'}

    async def test_known_ids_non_numeric_compared_lexicographically(
        self, registry: MessageRegistryPort
    ) -> None:
        for external_id in ('a', 'b', 'c'):
            await registry.upsert(make_record(external_id=external_id))
        assert await registry.known_ids(SOURCE_KEY, min_id='b') == {'b', 'c'}

    async def test_known_ids_excludes_deleted(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record(external_id='1'))
        await registry.upsert(make_record(external_id='2'))
        await registry.mark_deleted(SOURCE_KEY, '1')
        assert await registry.known_ids(SOURCE_KEY, min_id='0') == {'2'}

    async def test_known_ids_scoped_to_source(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record())
        await registry.upsert(make_record(source_key='memory:acc1:-100999'))
        assert await registry.known_ids(SOURCE_KEY, min_id='0') == {'42'}

    async def test_prune_removes_records_older_than(
        self, registry: MessageRegistryPort
    ) -> None:
        old = make_record(external_id='1', event_at=NOW - timedelta(days=30))
        fresh = make_record(external_id='2')
        await registry.upsert(old)
        await registry.upsert(fresh)
        assert await registry.prune(older_than=NOW - timedelta(days=7)) == 1
        assert await registry.get(SOURCE_KEY, '1') is None
        assert await registry.get(SOURCE_KEY, '2') == fresh

    async def test_prune_drops_versions_of_pruned_records(
        self, registry: MessageRegistryPort
    ) -> None:
        await registry.upsert(make_record(event_at=NOW - timedelta(days=30)))
        await registry.upsert(
            make_record(
                text='v2',
                content_hash='hash-b',
                event_at=NOW - timedelta(days=30),
                edit_ts=NOW - timedelta(days=29),
            )
        )
        assert await registry.prune(older_than=NOW - timedelta(days=7)) == 1
        assert await registry.versions(SOURCE_KEY, '42') == []

    async def test_prune_drops_all_old_versions_of_kept_record(
        self, registry: MessageRegistryPort
    ) -> None:
        """Запись в окне, но весь её архив — за окном: архив пустеет."""
        await registry.upsert(make_record(event_at=NOW - timedelta(days=30)))
        await registry.upsert(
            make_record(
                text='v2',
                content_hash='hash-b',
                event_at=NOW - timedelta(days=30),
                edit_ts=NOW,
            )
        )
        assert await registry.prune(older_than=NOW - timedelta(days=7)) == 0
        assert await registry.get(SOURCE_KEY, '42') is not None
        assert await registry.versions(SOURCE_KEY, '42') == []

    async def test_prune_trims_old_versions_of_kept_records(
        self, registry: MessageRegistryPort
    ) -> None:
        """Запись в окне, но её ранние версии — за окном: версии чистятся."""
        await registry.upsert(make_record(event_at=NOW - timedelta(days=30)))
        await registry.upsert(
            make_record(
                text='v2',
                content_hash='hash-b',
                event_at=NOW - timedelta(days=30),
                edit_ts=NOW,
            )
        )
        await registry.upsert(
            make_record(
                text='v3',
                content_hash='hash-c',
                event_at=NOW - timedelta(days=30),
                edit_ts=NOW + timedelta(minutes=1),
            )
        )
        assert await registry.prune(older_than=NOW - timedelta(days=7)) == 0
        versions = await registry.versions(SOURCE_KEY, '42')
        assert [v.text for v in versions] == ['v2']
