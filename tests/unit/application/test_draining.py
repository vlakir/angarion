"""Ограниченный graceful-drain циклов воркеров (T031): shielded_drain."""

from __future__ import annotations

import asyncio

import pytest

from angarion.application.draining import shielded_drain

pytestmark = pytest.mark.asyncio


async def test_returns_handling_result_without_cancel() -> None:
    """Без отмены — просто прозрачно возвращает результат операции."""

    async def work() -> int:
        return 42

    handling = asyncio.ensure_future(work())
    assert await shielded_drain(handling, 5.0) == 42


async def test_cancel_drains_quick_handling_to_completion() -> None:
    """
    Отмена при быстрой операции: shield не обрывает её, операция
    доделывается в пределах таймаута, отмена пробрасывается дальше.
    """
    finished = asyncio.Event()

    async def work() -> None:
        await asyncio.sleep(0.02)
        finished.set()

    handling = asyncio.ensure_future(work())
    task = asyncio.create_task(shielded_drain(handling, 5.0))
    await asyncio.sleep(0)  # дать shielded_drain зайти в await
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()  # дренировали, не оборвали
    assert not handling.cancelled()


async def test_cancel_aborts_hanging_handling_within_timeout() -> None:
    """
    Отмена заблокированной операции (долгий sleep): drain ограничен
    таймаутом — стоп завершается ~за таймаут, а не за длину сна; операция
    обрывается.
    """
    aborted = asyncio.Event()

    async def stuck() -> None:
        try:
            await asyncio.sleep(100)  # «залип» в throttle/FloodWait
        except asyncio.CancelledError:
            aborted.set()
            raise

    handling = asyncio.ensure_future(stuck())
    task = asyncio.create_task(shielded_drain(handling, 0.05))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1.0)  # не 100 c
    assert handling.cancelled()
    assert aborted.is_set()
