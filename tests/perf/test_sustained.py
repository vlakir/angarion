"""
Sustained/ramp-прогоны нагрузочного гарнеса по матрице бэкендов (T044, фаза B).

Open-loop: подавать записи с целевым темпом ``target_rps`` фиксированное
время и смотреть профиль глубины очереди. Если конвейер не успевает —
очередь растёт (``saturated``), наклон глубины показывает отставание. На
memory-потолке при умеренном темпе конвейер обычно успевает; на
persistqueue+SQLite тот же темп вскрывает точку насыщения.

Knobs через окружение:

- ``ANGARION_PERF_SUSTAINED_RPS`` — целевой темп подачи (по умолчанию 500).
- ``ANGARION_PERF_SUSTAINED_SECONDS`` — длительность окна (по умолчанию 5).
- ``ANGARION_PERF_MIN_RPS`` — soft-порог темпа доставки (по умолчанию выкл).
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
    run_sustained,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.perf

_DEFAULT_TARGET_RPS = 500.0
_DEFAULT_DURATION_S = 5.0


def _target_rps() -> float:
    return float(
        os.environ.get('ANGARION_PERF_SUSTAINED_RPS', str(_DEFAULT_TARGET_RPS))
    )


def _duration_s() -> float:
    return float(
        os.environ.get('ANGARION_PERF_SUSTAINED_SECONDS', str(_DEFAULT_DURATION_S))
    )


@pytest.mark.parametrize(('queue_backend', 'storage_backend'), BACKENDS)
async def test_sustained_matrix(
    tmp_path: Path,
    queue_backend: QueueBackendName,
    storage_backend: StorageBackendName,
) -> None:
    report = await run_sustained(
        target_rps=_target_rps(),
        duration_s=_duration_s(),
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        tmp_path=tmp_path,
    )

    assert report.submitted > 0
    assert report.submit_rps > 0
    assert report.delivery_rps >= 0
    assert report.delivered <= report.submitted
    assert report.peak_depth >= 0
    assert len(report.samples) > 0

    print('\n' + report.render())
    maybe_write_json(report, label=f'sustained-{queue_backend}-{storage_backend}')
    maybe_assert_min_rps(report.delivery_rps)
