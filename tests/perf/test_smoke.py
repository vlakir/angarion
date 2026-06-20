"""
Smoke-тест нагрузочного гарнеса (T044, фаза A).

Доказывает, что гарнес собирает настоящий конвейер, прогоняет burst и
выдаёт корректный :class:`PerfReport`. Маркер ``perf`` — вне gated-прогона
(``addopts = -m 'not perf'``); запуск: ``pytest -m perf --no-cov tests/perf``.
"""

from __future__ import annotations

import pytest

from _harness import run_burst

pytestmark = pytest.mark.perf


async def test_memory_burst_produces_report() -> None:
    n = 200
    report = await run_burst(n=n, queue_backend='memory', storage_backend='memory')

    assert report.delivered == n
    assert report.throughput_rps > 0
    assert report.elapsed_s > 0
    assert report.lat_p50_ms >= 0
    assert report.lat_p50_ms <= report.lat_p95_ms <= report.lat_p99_ms
    assert report.final_depth == 0
    assert report.peak_depth >= 0
    # отчёт печатается человекочитаемо (виден при -s / -rA)
    print('\n' + report.render())
