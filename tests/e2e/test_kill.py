"""
Kill-тест M2 (§16 ТЗ; FR-12, SC-2, C-4 спеки T003): конвейер на
персистентных адаптерах в дочернем процессе, SIGKILL в детерминированных
точках + рандомизированные итерации по бюджету (C-7), рестарт —
доставка без потерь; дубли — только в остаточном окне C-9
«send выполнен, mark_sent не записан», максимум один на kill.

POSIX-only: SIGKILL (A-10).
"""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from kill_child import (
    DELIVERED_LOG,
    POINT_AFTER_SEND,
    POINT_BEFORE_ACK,
    POINT_BETWEEN_PUT_AND_MARK,
    expected_keys,
    target_key,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.skipif(
    os.name != 'posix', reason='kill-тест требует SIGKILL — POSIX-only (A-10)'
)

CHILD = Path(__file__).with_name('kill_child.py')
EVENT_COUNT = 40
TARGET = '25'
EXPECTED = expected_keys(EVENT_COUNT)
TARGET_KEY = target_key(TARGET)

COMPLETION_TIMEOUT = 25.0
KILL_RUN_TIMEOUT = 30.0
RANDOM_BUDGET_SECONDS = 10.0
RANDOM_SLEEP_RANGE = (0.3, 1.2)


def _spawn(data_dir: Path, *, point: str = '') -> subprocess.Popen[bytes]:
    env = os.environ.copy()
    env.update(
        KILL_DATA_DIR=str(data_dir),
        KILL_POINT=point,
        KILL_TARGET=TARGET,
        KILL_EVENT_COUNT=str(EVENT_COUNT),
    )
    with (data_dir / 'child.log').open('ab') as log:
        return subprocess.Popen(  # noqa: S603
            [sys.executable, str(CHILD)],
            env=env,
            stdout=log,
            stderr=log,
        )


def _delivered(data_dir: Path) -> list[str]:
    path = data_dir / DELIVERED_LOG
    if not path.exists():
        return []
    return path.read_text(encoding='utf-8').splitlines()


def _child_log(data_dir: Path) -> str:
    path = data_dir / 'child.log'
    return path.read_text(encoding='utf-8') if path.exists() else '<пусто>'


def _wait_for_completion(data_dir: Path, proc: subprocess.Popen[bytes]) -> None:
    """Дождаться доставки всех ожидаемых ключей финальным чистым прогоном."""
    deadline = time.monotonic() + COMPLETION_TIMEOUT
    while time.monotonic() < deadline:
        if EXPECTED <= set(_delivered(data_dir)):
            return
        if proc.poll() is not None:
            msg = (
                f'финальный прогон умер (rc={proc.returncode}) до полной '
                f'доставки; лог:\n{_child_log(data_dir)}'
            )
            raise AssertionError(msg)
        time.sleep(0.05)
    delivered = set(_delivered(data_dir))
    msg = (
        f'не дождались полной доставки за {COMPLETION_TIMEOUT} c: '
        f'{len(delivered & EXPECTED)}/{len(EXPECTED)}; '
        f'лог:\n{_child_log(data_dir)}'
    )
    raise AssertionError(msg)


@pytest.fixture
def final_run(tmp_path: Path) -> Iterator[list[subprocess.Popen[bytes]]]:
    """Реестр запущенных процессов: по выходу из теста все добиваются."""
    procs: list[subprocess.Popen[bytes]] = []
    yield procs
    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def _assert_no_loss(data_dir: Path) -> Counter[str]:
    counts = Counter(_delivered(data_dir))
    missing = EXPECTED - set(counts)
    assert not missing, f'потеряны доставки: {sorted(missing)[:5]}…'
    unexpected = set(counts) - EXPECTED
    assert not unexpected, f'неожиданные доставки: {sorted(unexpected)[:5]}'
    return counts


def _assert_dup_budget(counts: Counter[str], kills: int) -> None:
    """C-4: дубли только из окна send→mark_sent — не больше одного на kill."""
    over_budget = {key: n for key, n in counts.items() if n > kills + 1}
    assert not over_budget, f'дублей больше бюджета kill-ов: {over_budget}'
    extras = sum(n - 1 for n in counts.values())
    assert extras <= kills, f'суммарно {extras} дублей при {kills} kill-ах'


@pytest.mark.parametrize(
    'point',
    [POINT_BETWEEN_PUT_AND_MARK, POINT_BEFORE_ACK, POINT_AFTER_SEND],
)
def test_deterministic_kill_point_keeps_delivery(
    point: str, tmp_path: Path, final_run: list[subprocess.Popen[bytes]]
) -> None:
    """SIGKILL в заданной точке конвейера: потерь нет, дубли — по C-4."""
    victim = _spawn(tmp_path, point=point)
    final_run.append(victim)
    assert victim.wait(timeout=KILL_RUN_TIMEOUT) == -signal.SIGKILL, (
        f'kill-точка {point!r} не сработала; лог:\n{_child_log(tmp_path)}'
    )

    restarted = _spawn(tmp_path)
    final_run.append(restarted)
    _wait_for_completion(tmp_path, restarted)

    counts = _assert_no_loss(tmp_path)
    _assert_dup_budget(counts, kills=1)
    if point == POINT_AFTER_SEND:
        # kill внутри send (до mark_sent) детерминированно ловит окно C-9:
        # ровно один дубль мишени — гарантия конструкции kill-точки.
        assert counts[TARGET_KEY] == 2  # noqa: PLR2004
    else:
        # before_ack / between_put_and_mark: kill приходится не на доставку,
        # но мишень к этому моменту уже в outbox, а доставкой занят отдельный
        # конкурентный DeliveryWorker. Его send→mark_sent — то же окно C-9
        # (§7.1, at-least-once): kill может совпасть с ним и дать дубль
        # мишени. Это не потеря и не нарушение контракта, поэтому ждём
        # at-least-once, а не «ровно один»; верхнюю границу (≤ 1 дубль на
        # kill) уже проверил _assert_dup_budget. Иначе тест флачит на 3.12
        # под нагрузкой CI (T035): legitimate дубль ловится как 2 == 1.
        assert counts[TARGET_KEY] >= 1


def test_random_kills_keep_delivery(
    tmp_path: Path, final_run: list[subprocess.Popen[bytes]]
) -> None:
    """SIGKILL в произвольные моменты по бюджету (C-7): потерь нет."""
    rng = random.Random()  # noqa: S311 — рандомизация сценария, не криптография
    kills = 0
    deadline = time.monotonic() + RANDOM_BUDGET_SECONDS
    while time.monotonic() < deadline:
        proc = _spawn(tmp_path)
        final_run.append(proc)
        time.sleep(rng.uniform(*RANDOM_SLEEP_RANGE))
        proc.kill()
        proc.wait(timeout=10)
        kills += 1
    assert kills > 0

    restarted = _spawn(tmp_path)
    final_run.append(restarted)
    _wait_for_completion(tmp_path, restarted)

    counts = _assert_no_loss(tmp_path)
    _assert_dup_budget(counts, kills=kills)
