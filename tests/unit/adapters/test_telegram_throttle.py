"""Token-bucket троттлинг отправки (FR «Sender», M3, фаза 4)."""

from __future__ import annotations

import pytest

from angarion.adapters.telegram.throttle import TokenBucket


class FakeClock:
    """Управляемые часы: ``sleep`` двигает время вперёд детерминированно."""

    def __init__(self) -> None:
        self.t = 0.0
        self.slept: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += seconds


def _bucket(clock: FakeClock, *, rate: float, capacity: float) -> TokenBucket:
    return TokenBucket(
        rate=rate, capacity=capacity, clock=clock.now, sleep=clock.sleep
    )


def test_rejects_non_positive_params() -> None:
    clock = FakeClock()
    with pytest.raises(ValueError, match='положительны'):
        _bucket(clock, rate=0, capacity=1)
    with pytest.raises(ValueError, match='положительны'):
        _bucket(clock, rate=1, capacity=0)


async def test_initial_burst_up_to_capacity_is_immediate() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=1.0, capacity=3.0)
    for _ in range(3):
        await bucket.acquire()
    assert clock.slept == []  # стартовый бакет полон — без ожидания


async def test_blocks_until_refill_when_empty() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=1.0, capacity=1.0)
    await bucket.acquire()  # съели единственный токен
    await bucket.acquire()  # пришлось ждать ~1с до следующего
    assert clock.slept == [1.0]
    assert clock.t == 1.0


async def test_refill_capped_at_capacity() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=1.0, capacity=2.0)
    await bucket.acquire()  # 2 -> 1
    clock.t += 100.0  # много времени, но накопить можно лишь до capacity
    await bucket.acquire()  # 2 -> 1 (без ожидания)
    await bucket.acquire()  # 1 -> 0 (без ожидания)
    await bucket.acquire()  # пусто -> ждём 1с
    assert clock.slept == [1.0]


async def test_rate_governs_wait_duration() -> None:
    clock = FakeClock()
    bucket = _bucket(clock, rate=2.0, capacity=1.0)  # 2 токена/с
    await bucket.acquire()
    await bucket.acquire()  # ждём 1/2 с
    assert clock.slept == [0.5]
