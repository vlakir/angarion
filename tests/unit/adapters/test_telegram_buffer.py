"""LiveBuffer (Q8): неблокирующий буфер с warning по мягкому лимиту."""

from __future__ import annotations

import pytest
from angarion.testing import make_record

from angarion.adapters.memory.storage import MemoryAnalytics
from angarion.adapters.telegram.buffer import LiveBuffer
from angarion.log import get_logger

pytestmark = pytest.mark.asyncio


def _buffer(soft_limit: int) -> tuple[LiveBuffer, MemoryAnalytics]:
    analytics = MemoryAnalytics()
    buffer = LiveBuffer(
        soft_limit=soft_limit, log=get_logger('test'), analytics=analytics
    )
    return buffer, analytics


async def test_put_below_limit_no_warning() -> None:
    buffer, analytics = _buffer(3)
    await buffer.put(make_record())
    await buffer.put(make_record())
    assert buffer.qsize() == 2
    assert await analytics.recent(kind='live_buffer_high') == []


async def test_warns_once_on_crossing_soft_limit() -> None:
    buffer, analytics = _buffer(2)
    await buffer.put(make_record())
    await buffer.put(make_record())
    await buffer.put(make_record())
    warnings = await analytics.recent(kind='live_buffer_high')
    assert len(warnings) == 1
    assert warnings[0].payload == {'depth': 2, 'soft_limit': 2}


async def test_fifo_order() -> None:
    buffer, _ = _buffer(10)
    first = make_record(external_id='1')
    second = make_record(external_id='2')
    await buffer.put(first)
    await buffer.put(second)
    assert (await buffer.get()).external_id == '1'
    assert (await buffer.get()).external_id == '2'


async def test_warning_rearms_after_draining_below_limit() -> None:
    buffer, analytics = _buffer(2)
    await buffer.put(make_record())
    await buffer.put(make_record())
    await buffer.get()  # опускаемся ниже лимита → флаг сбрасывается
    await buffer.put(make_record())
    await buffer.put(make_record())
    assert len(await analytics.recent(kind='live_buffer_high')) == 2


async def test_empty_reflects_state() -> None:
    buffer, _ = _buffer(5)
    assert buffer.empty() is True
    await buffer.put(make_record())
    assert buffer.empty() is False
    await buffer.get()
    assert buffer.empty() is True
