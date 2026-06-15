"""
Token-bucket троттлинг отправки (FR «Sender», M3, фаза 4).

Неблокирующий (``asyncio.sleep``, не блокирует event loop): перед каждой
отправкой sender берёт по токену из бакета чата (≤ 1 msg/s на чат) и
бакета аккаунта (≤ 20/min на аккаунт) — пороги конфигурируемы. Бакет
допускает короткий всплеск до ``capacity``, затем выравнивает темп под
``rate`` (классический token bucket: лимиты Telegram «в среднем», но с
запасом на пачку).

Часы и сон инъектируются (``clock``/``sleep``) — алгоритм тестируется
детерминированно, без реальных задержек.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


class TokenBucket:
    """Асинхронный token bucket: ``acquire`` ждёт, пока не наберётся токен."""

    def __init__(
        self,
        *,
        rate: float,
        capacity: float,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        if rate <= 0 or capacity <= 0:
            msg = 'rate и capacity token bucket должны быть положительны'
            raise ValueError(msg)
        self._rate = rate
        self._capacity = capacity
        self._clock = clock
        self._sleep = sleep
        self._tokens = capacity
        self._updated = clock()

    async def acquire(self) -> None:
        """Дождаться и списать один токен (не блокируя event loop)."""
        while True:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return
            await self._sleep((1 - self._tokens) / self._rate)

    def reconfigure(self, *, rate: float, capacity: float) -> None:
        """
        Сменить ``rate``/``capacity`` на лету (динамические лимиты §12.8).

        Сначала добираем токены по старому темпу до текущего момента,
        затем применяем новые параметры и подрезаем накопленное под новый
        ``capacity`` — смена лимита не «дарит» всплеск сверх новой ёмкости.
        """
        if rate <= 0 or capacity <= 0:
            msg = 'rate и capacity token bucket должны быть положительны'
            raise ValueError(msg)
        self._refill()
        self._rate = rate
        self._capacity = capacity
        self._tokens = min(self._tokens, capacity)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated
        self._updated = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
