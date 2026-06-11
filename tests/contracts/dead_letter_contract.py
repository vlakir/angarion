"""Контрактный набор ``DeadLetterPort`` (§8 ТЗ, C-2; FR-6, SC-5)."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from factories import NOW, make_dead_letter, make_envelope

from angarion.domain.ports import DeadLetterPort


class DeadLetterContract:
    """
    Поведенческая спецификация DLQ: узкий put/list/take (C-2),
    порядок поступления, фильтр по пайплайну; автоматической очистки
    нет (§17.3) — разбор ручной.
    """

    @pytest.fixture
    def dead_letters(self) -> DeadLetterPort:
        raise NotImplementedError

    async def test_put_list_roundtrip(self, dead_letters: DeadLetterPort) -> None:
        letter = make_dead_letter()
        await dead_letters.put(letter)
        assert await dead_letters.list() == [letter]

    async def test_list_in_arrival_order(self, dead_letters: DeadLetterPort) -> None:
        first = make_dead_letter()
        second = make_dead_letter(failed_at=NOW + timedelta(seconds=1))
        await dead_letters.put(first)
        await dead_letters.put(second)
        assert await dead_letters.list() == [first, second]

    async def test_list_filters_by_pipeline(
        self, dead_letters: DeadLetterPort
    ) -> None:
        await dead_letters.put(
            make_dead_letter(envelope=make_envelope(pipeline='digest'))
        )
        relay = make_dead_letter(envelope=make_envelope(pipeline='relay'))
        await dead_letters.put(relay)
        assert await dead_letters.list(pipeline='relay') == [relay]

    async def test_list_respects_limit(self, dead_letters: DeadLetterPort) -> None:
        for _ in range(3):
            await dead_letters.put(make_dead_letter())
        assert len(await dead_letters.list(limit=2)) == 2

    async def test_take_removes_and_returns(
        self, dead_letters: DeadLetterPort
    ) -> None:
        letter = make_dead_letter()
        await dead_letters.put(letter)
        assert await dead_letters.take(letter.uid) == letter
        assert await dead_letters.list() == []

    async def test_take_unknown_returns_none(
        self, dead_letters: DeadLetterPort
    ) -> None:
        assert await dead_letters.take(uuid4()) is None
