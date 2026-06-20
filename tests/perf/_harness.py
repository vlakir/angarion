"""
Ядро нагрузочного гарнеса angarion (T044, фаза A).

Прогоняет синтетический поток записей через **настоящий** конвейер
(``build_app``: ingest → queue → worker → outbox → delivery → sink) без
сети и реальных платформ. Listener — ``MemoryListener`` (программная
инжекция), процессор — тривиальный ``passthrough`` (меряем накладные
расходы фреймворка, не стоимость обработки), сток — ``CountingSink``
(считает доставленное, не накапливая записи).

Латентность считается без зависимости от ``OutboundRecord.record_uid``
(его passthrough не проставляет, см. spec Analyze): в текст входящей
записи кладётся ``perf_counter``-метка эмита, ``passthrough`` ретранслирует
текст как есть, а ``on_delivery``-хук стока парсит метку и фиксирует
``perf_counter() - emit`` на каждую доставку.

Это building blocks; сценарии (burst/sustained, матрица бэкендов) живут в
``test_*.py`` рядом. Модуль не собирается pytest'ом (префикс ``_``).
"""

from __future__ import annotations

import asyncio
import math
import os
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict

from angarion.adapters.memory.listener import TRANSPORT, MemoryListener
from angarion.adapters.memory.plugin import (
    MEMORY_CAPABILITIES,
    QUEUE_BACKEND as MEMORY_QUEUE_BACKEND,
    STORAGE_BACKEND as MEMORY_STORAGE_BACKEND,
    MemoryAccountConfig,
)
from angarion.adapters.queue.persistqueue_ import QUEUE_BACKEND as PERSISTQUEUE_BACKEND
from angarion.adapters.storage.plugin import STORAGE_BACKEND as SQLITE_STORAGE_BACKEND
from angarion.bootstrap import LoadedPlugins, build_app
from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
    WorkerConfig,
)
from angarion.domain.models import RecordKind
from angarion.domain.plugin import AdapterPlugin
from angarion.testing import CountingSink

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pydantic import BaseModel as PydanticModel

    from angarion.bootstrap import AdapterDeps, AngarionApp

QueueBackendName = Literal['memory', 'persistqueue']
StorageBackendName = Literal['memory', 'sqlite']

SRC_CHAT = '-100perfsrc'
DST_CHAT = '-100perfdst'
ACCOUNT = 'main'

#: Матрица бэкендов из spec §3: (1) memory-потолок, (2) реалистичная
#: персистентность persistqueue+SQLite. Не полное декартово произведение —
#: ровно две осмысленные конфигурации.
BACKENDS: tuple[tuple[QueueBackendName, StorageBackendName], ...] = (
    ('memory', 'memory'),
    ('persistqueue', 'sqlite'),
)


class PerfReport(BaseModel):
    """Сводка одного прогона: конфигурация + измеренные агрегаты."""

    model_config = ConfigDict(frozen=True)

    queue_backend: QueueBackendName
    storage_backend: StorageBackendName
    n: int
    poll_interval: float
    delivered: int
    elapsed_s: float
    throughput_rps: float
    lat_p50_ms: float
    lat_p95_ms: float
    lat_p99_ms: float
    peak_depth: int
    final_depth: int

    def render(self) -> str:
        """Человекочитаемый отчёт по прогону (числа + условия)."""
        return '\n'.join(
            (
                f'perf-прогон: queue={self.queue_backend} '
                f'storage={self.storage_backend} N={self.n} '
                f'poll_interval={self.poll_interval}s',
                f'  доставлено:   {self.delivered}/{self.n}',
                f'  время:        {self.elapsed_s:.3f} s',
                f'  throughput:   {self.throughput_rps:.1f} rec/s',
                f'  латентность:  p50={self.lat_p50_ms:.2f} '
                f'p95={self.lat_p95_ms:.2f} p99={self.lat_p99_ms:.2f} ms',
                f'  глубина оч.:  peak={self.peak_depth} final={self.final_depth}',
            )
        )


class DepthSample(BaseModel):
    """Точка профиля очереди во времени: (секунды от старта, глубина, доставлено)."""

    model_config = ConfigDict(frozen=True)

    t_s: float
    depth: int
    delivered: int


class SustainedReport(BaseModel):
    """
    Сводка sustained/ramp-прогона: подача с целевым темпом ``target_rps`` в
    течение ``duration_s`` и профиль ``queue.depth()`` во времени. Цель —
    насыщение/backpressure: успевает ли конвейер за темпом подачи (slope
    глубины очереди) и где точка, после которой обработка отстаёт.
    """

    model_config = ConfigDict(frozen=True)

    queue_backend: QueueBackendName
    storage_backend: StorageBackendName
    target_rps: float
    duration_s: float
    poll_interval: float
    submitted: int
    delivered: int
    submit_rps: float
    delivery_rps: float
    peak_depth: int
    final_depth: int
    depth_slope_rps: float
    saturated: bool
    samples: tuple[DepthSample, ...]

    def render(self) -> str:
        """Человекочитаемый отчёт по sustained-прогону (числа + условия)."""
        if self.submit_rps < self.target_rps * 0.9:
            verdict = 'НАСЫЩЕН (подача не достигает target — узкое место на входе)'
        elif self.depth_slope_rps > self.target_rps * 0.05:
            verdict = 'НАСЫЩЕН (очередь растёт — доставка отстаёт)'
        else:
            verdict = 'успевает'
        return '\n'.join(
            (
                f'sustained-прогон: queue={self.queue_backend} '
                f'storage={self.storage_backend} '
                f'target={self.target_rps:.0f} rec/s '
                f'duration={self.duration_s:.1f}s poll={self.poll_interval}s',
                f'  подано/доставлено: {self.submitted}/{self.delivered}',
                f'  темп подачи:       {self.submit_rps:.1f} rec/s',
                f'  темп доставки:     {self.delivery_rps:.1f} rec/s',
                f'  глубина оч.:       peak={self.peak_depth} '
                f'final={self.final_depth} slope={self.depth_slope_rps:+.1f} rec/s',
                f'  вердикт:           {verdict}',
            )
        )


def _percentile(values_sorted: Sequence[float], q: float) -> float:
    """Перцентиль методом ближайшего ранга (``q`` в [0, 1])."""
    if not values_sorted:
        return 0.0
    idx = min(len(values_sorted) - 1, max(0, math.ceil(q * len(values_sorted)) - 1))
    return values_sorted[idx]


def _slope(points: Sequence[DepthSample]) -> float:
    """МНК-наклон глубины очереди по времени (rec/s); 0.0 при <2 точках."""
    n = len(points)
    if n < 2:
        return 0.0
    sx = sum(p.t_s for p in points)
    sy = sum(p.depth for p in points)
    sxx = sum(p.t_s * p.t_s for p in points)
    sxy = sum(p.t_s * p.depth for p in points)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0
    return (n * sxy - sx * sy) / denom


def _perf_memory_plugin(sink: CountingSink) -> AdapterPlugin:
    """memory-плагин, чей sender — переданный ``CountingSink`` (test-owned)."""

    def make_listener(
        deps: AdapterDeps,
        accounts: Mapping[str, PydanticModel],
        _sources: Sequence[EndpointConfig],
    ) -> MemoryListener:
        return MemoryListener(ingest=deps.ingest, account_ids=tuple(accounts))

    def make_sender(
        _deps: AdapterDeps, _accounts: Mapping[str, PydanticModel]
    ) -> CountingSink:
        return sink

    return AdapterPlugin(
        name=TRANSPORT,
        capabilities=MEMORY_CAPABILITIES,
        account_config_model=MemoryAccountConfig,
        make_listener=make_listener,
        make_sender=make_sender,
    )


def build_perf_plugins(sink: CountingSink) -> LoadedPlugins:
    """Реестр плагинов гарнеса: memory-listener+CountingSink, обе очереди/БД."""
    return LoadedPlugins(
        adapters={TRANSPORT: _perf_memory_plugin(sink)},
        queues={
            'memory': MEMORY_QUEUE_BACKEND,
            'persistqueue': PERSISTQUEUE_BACKEND,
        },
        storages={
            'memory': MEMORY_STORAGE_BACKEND,
            'sqlite': SQLITE_STORAGE_BACKEND,
        },
    )


def build_perf_settings(
    *,
    queue_backend: QueueBackendName,
    storage_backend: StorageBackendName,
    poll_interval: float,
    tmp_path: Path | None,
) -> AngarionSettings:
    """
    Конфиг прогона: один memory-аккаунт, один пайплайн ``passthrough``
    SRC→DST, ужатые poll/backoff (иначе throughput упрётся в ``sleep``, см.
    spec Analyze). sqlite/persistqueue пишут в изолированный ``tmp_path``.
    """
    payload: dict[str, Any] = {
        'accounts': {ACCOUNT: {'transport': TRANSPORT}},
        'storage': {'backend': storage_backend},
        'queue': {'backend': queue_backend},
        'worker': WorkerConfig(
            max_retries=1,
            backoff_base=0.001,
            backoff_cap=0.002,
            poll_interval=poll_interval,
        ),
        'pipelines': {
            'perf': PipelineConfig(
                processor='passthrough',
                events=frozenset(RecordKind),
                sources=(EndpointConfig(account=ACCOUNT, address=SRC_CHAT),),
                targets=(EndpointConfig(account=ACCOUNT, address=DST_CHAT),),
            )
        },
    }
    if storage_backend == 'sqlite':
        if tmp_path is None:
            msg = 'sqlite-прогон требует tmp_path для изоляции app.db'
            raise ValueError(msg)
        payload['storage']['path'] = str(tmp_path / 'app.db')
    if queue_backend == 'persistqueue':
        if tmp_path is None:
            msg = 'persistqueue-прогон требует tmp_path для изоляции queue.db'
            raise ValueError(msg)
        payload['queue']['path'] = str(tmp_path / 'queue')
    return AngarionSettings.model_validate(payload)


async def run_burst(
    *,
    n: int,
    queue_backend: QueueBackendName = 'memory',
    storage_backend: StorageBackendName = 'memory',
    poll_interval: float = 0.001,
    tmp_path: Path | None = None,
    timeout: float = 60.0,
    sample_interval: float = 0.005,
) -> PerfReport:
    """
    Closed-loop burst: подать N записей, дождаться доставки всех, замерить
    throughput, перцентили латентности и профиль глубины очереди.

    Возвращает :class:`PerfReport`. ``timeout`` страхует от подвисшего
    прогона (диагностический ``TimeoutError``).
    """
    latencies: list[float] = []
    sink = CountingSink(
        on_delivery=lambda record: latencies.append(
            perf_counter() - float(record.text or '0')
        )
    )
    settings = build_perf_settings(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        poll_interval=poll_interval,
        tmp_path=tmp_path,
    )
    app = build_app(settings, plugins=build_perf_plugins(sink))
    await app.start()
    try:
        listener = app.listeners[0]
        if not isinstance(listener, MemoryListener):
            msg = 'ожидался MemoryListener в гарнесе'
            raise TypeError(msg)
        peak_depth, emit_start = await _drive_burst(
            app=app,
            listener=listener,
            sink=sink,
            n=n,
            timeout=timeout,
            sample_interval=sample_interval,
        )
        elapsed = (sink.last_delivered_at or perf_counter()) - emit_start
        final = await app.queue.depth()
    finally:
        await app.stop()

    latencies.sort()
    return PerfReport(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        n=n,
        poll_interval=poll_interval,
        delivered=sink.count,
        elapsed_s=elapsed,
        throughput_rps=sink.count / elapsed if elapsed > 0 else 0.0,
        lat_p50_ms=_percentile(latencies, 0.50) * 1000,
        lat_p95_ms=_percentile(latencies, 0.95) * 1000,
        lat_p99_ms=_percentile(latencies, 0.99) * 1000,
        peak_depth=peak_depth,
        final_depth=final.pending + final.unacked,
    )


async def _drive_burst(
    *,
    app: AngarionApp,
    listener: MemoryListener,
    sink: CountingSink,
    n: int,
    timeout: float,
    sample_interval: float,
) -> tuple[int, float]:
    """Эмит N записей, ожидание доставки всех; вернуть (пик глубины, старт)."""
    emit_start = perf_counter()
    for i in range(n):
        await listener.emit(
            {
                'address': SRC_CHAT,
                'external_id': str(i),
                'text': repr(perf_counter()),
            }
        )
    peak_depth = 0
    deadline = perf_counter() + timeout
    while sink.count < n:
        depth = await app.queue.depth()
        peak_depth = max(peak_depth, depth.pending + depth.unacked)
        if perf_counter() > deadline:
            msg = f'burst не завершился за {timeout}s: {sink.count}/{n} доставлено'
            raise TimeoutError(msg)
        await asyncio.sleep(sample_interval)
    return peak_depth, emit_start


async def run_sustained(
    *,
    target_rps: float,
    duration_s: float,
    queue_backend: QueueBackendName = 'memory',
    storage_backend: StorageBackendName = 'memory',
    poll_interval: float = 0.001,
    tmp_path: Path | None = None,
    sample_interval: float = 0.05,
) -> SustainedReport:
    """
    Open-loop sustained: подавать записи с целевым темпом ``target_rps`` в
    течение ``duration_s`` секунд, сэмплируя глубину очереди. Если конвейер
    не успевает (delivery < target), очередь растёт — это и есть насыщение;
    наклон глубины (``depth_slope_rps``) — мера отставания.
    """
    sink = CountingSink()
    settings = build_perf_settings(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        poll_interval=poll_interval,
        tmp_path=tmp_path,
    )
    app = build_app(settings, plugins=build_perf_plugins(sink))
    await app.start()
    try:
        listener = app.listeners[0]
        if not isinstance(listener, MemoryListener):
            msg = 'ожидался MemoryListener в гарнесе'
            raise TypeError(msg)
        samples, submitted, submit_elapsed = await _drive_sustained(
            app=app,
            listener=listener,
            sink=sink,
            target_rps=target_rps,
            duration_s=duration_s,
            sample_interval=sample_interval,
        )
        delivered = sink.count
        final = await app.queue.depth()
    finally:
        await app.stop()

    depths = [s.depth for s in samples]
    slope = _slope(samples)
    return SustainedReport(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        target_rps=target_rps,
        duration_s=duration_s,
        poll_interval=poll_interval,
        submitted=submitted,
        delivered=delivered,
        submit_rps=submitted / submit_elapsed if submit_elapsed > 0 else 0.0,
        delivery_rps=delivered / submit_elapsed if submit_elapsed > 0 else 0.0,
        peak_depth=max(depths, default=0),
        final_depth=final.pending + final.unacked,
        depth_slope_rps=slope,
        # насыщение: либо подача не дотягивает до target (ingest-bound),
        # либо очередь растёт быстрее 5% target (delivery-bound).
        saturated=(
            submitted / submit_elapsed < target_rps * 0.9
            if submit_elapsed > 0
            else True
        )
        or slope > target_rps * 0.05,
        samples=tuple(samples),
    )


async def _drive_sustained(
    *,
    app: AngarionApp,
    listener: MemoryListener,
    sink: CountingSink,
    target_rps: float,
    duration_s: float,
    sample_interval: float,
) -> tuple[list[DepthSample], int, float]:
    """Подача с целевым темпом + сэмплирование глубины; (профиль, подано, время)."""
    start = perf_counter()
    deadline = start + duration_s
    next_sample = start + sample_interval
    samples: list[DepthSample] = []
    submitted = 0
    while perf_counter() < deadline:
        due = int((perf_counter() - start) * target_rps)
        while submitted <= due and perf_counter() < deadline:
            await listener.emit(
                {
                    'address': SRC_CHAT,
                    'external_id': str(submitted),
                    'text': repr(perf_counter()),
                }
            )
            submitted += 1
        if perf_counter() >= next_sample:
            depth = await app.queue.depth()
            samples.append(
                DepthSample(
                    t_s=perf_counter() - start,
                    depth=depth.pending + depth.unacked,
                    delivered=sink.count,
                )
            )
            next_sample += sample_interval
        next_emit_at = start + (submitted + 1) / target_rps
        sleep_for = min(next_emit_at, next_sample) - perf_counter()
        if sleep_for > 0:
            await asyncio.sleep(min(sleep_for, sample_interval))
    return samples, submitted, perf_counter() - start


def maybe_write_json(report: BaseModel, *, label: str) -> Path | None:
    """
    Если задан ``ANGARION_PERF_JSON_DIR`` — сохранить отчёт JSON-артефактом
    ``<dir>/<label>.json`` (для сравнения прогонов). Иначе ничего не делает.
    """
    directory = os.environ.get('ANGARION_PERF_JSON_DIR')
    if not directory:
        return None
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f'{label}.json'
    path.write_text(report.model_dump_json(indent=2), encoding='utf-8')
    return path


def maybe_assert_min_rps(throughput_rps: float) -> None:
    """
    Опц. soft-порог: если задан ``ANGARION_PERF_MIN_RPS`` и измеренный
    throughput ниже — поднять ``AssertionError`` (лёгкий regression-сигнал).
    По умолчанию (переменная не задана) — no-op.
    """
    raw = os.environ.get('ANGARION_PERF_MIN_RPS')
    if not raw:
        return
    floor = float(raw)
    if throughput_rps < floor:
        msg = (
            f'throughput {throughput_rps:.1f} rec/s ниже порога '
            f'ANGARION_PERF_MIN_RPS={floor}'
        )
        raise AssertionError(msg)
