"""
Ограниченный по времени graceful-drain циклов воркеров (T031).

`PipelineWorker`/`DeliveryWorker` на отмене не обрывают in-flight операцию
(``asyncio.shield`` — защита от порчи частичной записи, C-9), а дожидаются
её завершения. Но если операция залипла в долгом ожидании — token-bucket
троттлинг или ``FloodWait``-sleep (§12.8) — это подвешивало graceful-
остановку (`app.stop()`) на всю длину сна (T031, всплыло в T009/M6).

``shielded_drain`` ограничивает дренаж: даёт операции ``drain_seconds`` на
завершение, иначе обрывает. Возможный дубль от обрыва между ``send`` и
``mark_sent`` покрывает at-least-once (§7.1) — это не потеря.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress


async def shielded_drain[T](handling: asyncio.Future[T], drain_seconds: float) -> T:
    """
    Дождаться ``handling``, защитив его от отмены вызывающего (``shield``).

    Штатно — вернуть результат операции. При отмене вызывающего дать
    ``handling`` не больше ``drain_seconds`` доработать, после чего оборвать;
    в обоих случаях отмена пробрасывается дальше (цикл воркера завершается).
    """
    try:
        return await asyncio.shield(handling)
    except asyncio.CancelledError:
        with suppress(TimeoutError):
            await asyncio.wait_for(handling, drain_seconds)
        raise
