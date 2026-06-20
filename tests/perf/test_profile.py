"""
Сэмплирующий профиль sqlite-прогона (T044, фаза C, метод (c)).

``pyinstrument`` (dev-dep, async-режим) профилирует сквозной burst на
persistqueue+SQLite — реалистичной конфигурации с известными
сериализующими точками. Цель — НАЗВАТЬ сериализующую точку (где event-loop
проводит время в ожидании БД/очереди), а не только общий потолок: профиль
дополняет компонентные микро-бенчи (метод (a)) и атрибуцию по этапам
(метод (b)) call-tree'ом.

Маркер ``perf`` — вне gated-прогона; запуск:
``pytest -m perf --no-cov -s tests/perf/test_profile.py``.

Knobs через окружение:

- ``ANGARION_PERF_PROFILE_N`` — размер burst под профилем (по умолчанию 800;
  меньше обычного burst — сэмплирование добавляет накладные расходы).
- ``ANGARION_PERF_JSON_DIR`` — если задан, профиль сохраняется рядом
  HTML-артефактом ``profile-persistqueue-sqlite.html``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyinstrument import Profiler

from _harness import run_burst

pytestmark = pytest.mark.perf

_DEFAULT_PROFILE_N = 800


def _profile_n() -> int:
    return int(os.environ.get('ANGARION_PERF_PROFILE_N', str(_DEFAULT_PROFILE_N)))


async def test_profile_sqlite_burst(tmp_path: Path) -> None:
    n = _profile_n()
    profiler = Profiler(async_mode='enabled')
    profiler.start()
    report = await run_burst(
        n=n,
        queue_backend='persistqueue',
        storage_backend='sqlite',
        tmp_path=tmp_path,
        timeout=300.0,
    )
    profiler.stop()

    assert report.delivered == n

    text = profiler.output_text(unicode=False, color=False, show_all=False)
    assert text.strip()
    print('\n' + report.render())
    print('\n=== pyinstrument (persistqueue+sqlite burst) ===\n' + text)

    json_dir = os.environ.get('ANGARION_PERF_JSON_DIR')
    if json_dir:
        out = Path(json_dir)
        out.mkdir(parents=True, exist_ok=True)
        (out / 'profile-persistqueue-sqlite.html').write_text(
            profiler.output_html(), encoding='utf-8'
        )
