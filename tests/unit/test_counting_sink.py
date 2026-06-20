"""Контракт ``SinkPort`` и поведение ``CountingSink`` (T044, фаза A)."""

from __future__ import annotations

import pytest

from angarion.domain import ports
from angarion.testing import CountingSink, SinkContract, make_outbound


class TestCountingSink(SinkContract):
    @pytest.fixture
    def sink(self) -> CountingSink:
        return CountingSink()


async def test_counts_without_accumulating_records() -> None:
    sink = CountingSink()
    assert sink.count == 0
    assert sink.last_delivered_at is None
    await sink.send(make_outbound())
    await sink.send(make_outbound(text='second'))
    assert sink.count == 2
    # сток не хранит сами записи — только счётчик и perf-метку
    assert not hasattr(sink, 'sent')
    assert sink.last_delivered_at is not None


async def test_receipts_have_unique_external_ids() -> None:
    sink = CountingSink()
    first = await sink.send(make_outbound())
    second = await sink.send(make_outbound())
    assert first.external_id != second.external_id


async def test_on_delivery_hook_receives_each_record() -> None:
    seen: list[str | None] = []
    sink = CountingSink(on_delivery=lambda record: seen.append(record.text))
    await sink.send(make_outbound(text='a'))
    await sink.send(make_outbound(text='b'))
    assert seen == ['a', 'b']


def test_is_sink_port() -> None:
    assert isinstance(CountingSink(), ports.SinkPort)
