"""
Компонентные микро-бенчи портов по матрице бэкендов (T044, фаза C, метод (a)).

Гоняет каждый driven-порт в изоляции и печатает rec/s — количественная
локализация узкого места (какой порт сериализует горячий путь). Маркер
``perf`` — вне gated-прогона; запуск: ``pytest -m perf --no-cov tests/perf``.

Knobs через окружение:

- ``ANGARION_PERF_MICROBENCH_ITERS`` — число итераций на порт (по умолчанию 2000).
- ``ANGARION_PERF_JSON_DIR`` — каталог JSON-артефактов (по умолчанию выкл).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from _harness import BACKENDS, QueueBackendName, StorageBackendName, maybe_write_json
from _microbench import COMPONENTS, run_microbench

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.perf

_DEFAULT_ITERS = 2000


def _iters() -> int:
    return int(os.environ.get('ANGARION_PERF_MICROBENCH_ITERS', str(_DEFAULT_ITERS)))


@pytest.mark.parametrize(('queue_backend', 'storage_backend'), BACKENDS)
async def test_microbench_matrix(
    tmp_path: Path,
    queue_backend: QueueBackendName,
    storage_backend: StorageBackendName,
) -> None:
    iterations = _iters()
    report = await run_microbench(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        iterations=iterations,
        tmp_path=tmp_path,
    )

    assert report.iterations == iterations
    assert set(report.components) == set(COMPONENTS)
    for name, rps in report.components.items():
        assert rps > 0, name
    assert report.slowest() in COMPONENTS

    print('\n' + report.render())
    maybe_write_json(report, label=f'microbench-{queue_backend}-{storage_backend}')
