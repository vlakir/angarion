"""
Burst-прогоны нагрузочного гарнеса по матрице бэкендов (T044, фаза B).

Closed-loop: подать N записей, дождаться доставки всех, замерить throughput
и перцентили латентности. Прогоняется по двум конфигурациям из
:data:`_harness.BACKENDS` — memory-потолок и persistqueue+SQLite — чтобы
увидеть «цену персистентности». Маркер ``perf`` — вне gated-прогона
(``addopts = -m 'not perf'``); запуск: ``pytest -m perf --no-cov tests/perf``.

Knobs через окружение:

- ``ANGARION_PERF_BURST_N`` — размер пакета (по умолчанию 2000).
- ``ANGARION_PERF_MIN_RPS`` — soft-порог throughput (по умолчанию выкл).
- ``ANGARION_PERF_JSON_DIR`` — каталог JSON-артефактов (по умолчанию выкл).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from _harness import (
    BACKENDS,
    QueueBackendName,
    StorageBackendName,
    maybe_assert_min_rps,
    maybe_write_json,
    run_burst,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.perf

_DEFAULT_BURST_N = 2000


def _burst_n() -> int:
    return int(os.environ.get('ANGARION_PERF_BURST_N', str(_DEFAULT_BURST_N)))


@pytest.mark.parametrize(('queue_backend', 'storage_backend'), BACKENDS)
async def test_burst_matrix(
    tmp_path: Path,
    queue_backend: QueueBackendName,
    storage_backend: StorageBackendName,
) -> None:
    n = _burst_n()
    report = await run_burst(
        n=n,
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        tmp_path=tmp_path,
        timeout=300.0,
    )

    assert report.delivered == n
    assert report.throughput_rps > 0
    assert report.elapsed_s > 0
    assert report.lat_p50_ms <= report.lat_p95_ms <= report.lat_p99_ms
    assert report.final_depth == 0

    print('\n' + report.render())
    maybe_write_json(report, label=f'burst-{queue_backend}-{storage_backend}')
    maybe_assert_min_rps(report.throughput_rps)
