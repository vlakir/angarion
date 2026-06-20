"""
Программный ручной триггер (T038): встраиваем angarion в свой код.

Запуск — через лаунчер (он подхватывает dev-сессию и креды):

    examples/trigger/run.sh inject

Сценарий: собираем конвейер из ``app.toml``, поднимаем его и впрыскиваем
события РУЧНУЮ — без живого сообщения в группе A и без ручной сборки
``Record`` (фабрика прячется за ``ManualEvent``). Два пути:

1. ``app.submit_event(...)`` — event-семантика: запись маршрутизируется по
   ``source`` (как живое событие) → попадает в пайплайн ``forward`` →
   доставляется в группу B. ``source`` события должен совпасть с источником
   пайплайна, иначе маршрут не найдётся.
2. ``app.run_pipeline("forward", ...)`` — pipeline-семантика: запись
   ставится в именованный пайплайн НАПРЯМУЮ, минуя маршрутизацию и dedup
   (изолированный прогон конкретного пайплайна).

Оба ручных события несут ``origin='manual'`` (видно в ``/ui/events``).
"""

from __future__ import annotations

import asyncio
import logging

from angarion import ManualEvent
from angarion.bootstrap import build_app
from angarion.config import AngarionSettings, load_settings
from angarion.domain.models import Endpoint

CONFIG = 'app.toml'
PIPELINE = 'forward'
DELIVER_GRACE_SECONDS = 15.0
"""Пауза, чтобы worker + delivery успели обработать и доставить впрыснутое.

С запасом: реальная отправка в Telegram идёт последовательно (~1 c на
сообщение), а ``app.stop()`` отключает клиента — не дав паузы, хвост
исходящих останется ``pending``. Catch-up в этом примере выключен
(``[catchup] enabled = false``), так что в очереди только наши два события.
"""

log = logging.getLogger('angarion.examples.trigger')


def _pipeline_source(settings: AngarionSettings, name: str) -> Endpoint:
    """Source-``Endpoint`` пайплайна — чтобы событие смаршрутизировалось в него."""
    src = settings.pipelines[name].sources[0]
    transport = settings.accounts[src.account].transport
    return Endpoint(transport=transport, address=src.address, thread_id=src.thread_id)


async def main() -> None:
    settings = load_settings(CONFIG)
    source = _pipeline_source(settings, PIPELINE)
    app = build_app(settings)
    await app.start()
    try:
        # 1) event-путь: маршрутизация по source → пайплайн forward → группа B
        await app.submit_event(
            ManualEvent(source=source, text='Ручной впрыск через submit_event (event)')
        )
        log.info('→ submit_event: событие подано, маршрутизируется по source.')

        # 2) pipeline-путь: прямо в пайплайн forward, минуя router/dedup
        await app.run_pipeline(
            PIPELINE,
            ManualEvent(
                source=source, text='Ручной запуск через run_pipeline (direct)'
            ),
        )
        log.info('→ run_pipeline: запись поставлена прямо в пайплайн %r.', PIPELINE)

        log.info('→ Жду %g c доставки в группу B…', DELIVER_GRACE_SECONDS)
        await asyncio.sleep(DELIVER_GRACE_SECONDS)
    finally:
        await app.stop()
    log.info('→ Готово. Оба сообщения должны прилететь в группу B.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    asyncio.run(main())
