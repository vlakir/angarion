"""
Атрибуция времени по этапам конвейера по матрице бэкендов (T044, фаза C, (b)).

Однопоточно гоняет ingest → process → deliver с потактовыми метками и
печатает долю каждого этапа — где именно «живёт» время обработки одной
записи. Маркер ``perf`` — вне gated-прогона; запуск:
``pytest -m perf --no-cov tests/perf``.

Knobs через окружение:

- ``ANGARION_PERF_ATTRIBUTION_N`` — число записей (по умолчанию 500).
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
    maybe_write_json,
    run_stage_attribution,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.perf

_DEFAULT_N = 500


def _n() -> int:
    return int(os.environ.get('ANGARION_PERF_ATTRIBUTION_N', str(_DEFAULT_N)))


@pytest.mark.parametrize(('queue_backend', 'storage_backend'), BACKENDS)
async def test_attribution_matrix(
    tmp_path: Path,
    queue_backend: QueueBackendName,
    storage_backend: StorageBackendName,
) -> None:
    n = _n()
    report = await run_stage_attribution(
        n=n,
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        tmp_path=tmp_path,
    )

    assert report.n == n
    assert report.ingest_ms_median >= 0
    assert report.process_ms_median >= 0
    assert report.deliver_ms_median >= 0
    assert report.e2e_ms_median > 0
    shares = report.ingest_share + report.process_share + report.deliver_share
    assert shares == pytest.approx(1.0, abs=1e-9)
    assert report.dominant() in {'ingest', 'process', 'deliver'}

    print('\n' + report.render())
    maybe_write_json(report, label=f'attribution-{queue_backend}-{storage_backend}')
