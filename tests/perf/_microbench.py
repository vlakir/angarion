"""
Компонентные микро-бенчи портов в изоляции (T044, фаза C, метод (a)).

В отличие от ``_harness.py`` (сквозной конвейер), здесь каждый driven-порт
гоняется **отдельно**, в тугом цикле, без остального конвейера — чтобы
назвать стоимость одной операции каждого порта в rec/s и локализовать
сериализующую точку количественно (а не «на глаз»). Те же два бэкенда из
:data:`_harness.BACKENDS`: memory-потолок и persistqueue+SQLite.

Бенчуются операции горячего пути конвейера:

- ``queue`` — цикл ``put → get → ack`` (round-trip одного envelope);
- ``dedup`` — ``mark_inbound`` (insert-or-ignore нового ключа);
- ``registry`` — ``upsert`` (ветка ``IS_NEW`` — вставка нового сообщения);
- ``outbox`` — ``put`` (insert-if-absent нового исходящего);
- ``analytics`` — ``record`` (append события).

Входные DTO **пред-собираются** до старта таймера: меряем стоимость самой
операции порта (round-trip к бэкенду), а не конструирование pydantic-моделей.

Модуль не собирается pytest'ом (префикс ``_``); сценарий — в
``test_microbench.py`` рядом.
"""

from __future__ import annotations

from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING, Final
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from angarion.bootstrap import build_queue, build_storage
from angarion.domain.models import (
    AccountRef,
    AnalyticsEvent,
    Endpoint,
    OutboundRecord,
    QueueEnvelope,
    Record,
    RecordKind,
    RegistryRecord,
)
from angarion.testing import CountingSink

from _harness import (
    ACCOUNT,
    DST_CHAT,
    SRC_CHAT,
    QueueBackendName,
    StorageBackendName,
    build_perf_plugins,
    build_perf_settings,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence
    from pathlib import Path

    from angarion.domain.plugin import StorageBundle
    from angarion.domain.ports import EventQueuePort

TRANSPORT: Final = 'memory'

#: Порядок компонентов в отчёте — фиксированный (детерминизм вывода).
COMPONENTS: Final = ('queue', 'dedup', 'registry', 'outbox', 'analytics')


class MicrobenchReport(BaseModel):
    """Сводка микро-бенча: конфигурация + rec/s по каждому порту."""

    model_config = ConfigDict(frozen=True)

    queue_backend: QueueBackendName
    storage_backend: StorageBackendName
    iterations: int
    components: dict[str, float]

    def slowest(self) -> str:
        """Имя самого медленного компонента (наименьший rec/s) — узкое место."""
        return min(self.components, key=lambda name: self.components[name])

    def render(self) -> str:
        """Человекочитаемый отчёт: rec/s по портам + условия прогона."""
        lines = [
            f'микро-бенч портов: queue={self.queue_backend} '
            f'storage={self.storage_backend} iters={self.iterations}',
        ]
        lines.extend(
            f'  {name:<10} {self.components[name]:>12.1f} rec/s' for name in COMPONENTS
        )
        lines.append(f'  → самый медленный: {self.slowest()}')
        return '\n'.join(lines)


def _now() -> datetime:
    return datetime.now(UTC)


def _sample_record(i: int) -> Record:
    """Минимально валидная входящая запись для envelope очереди."""
    now = _now()
    return Record(
        uid=uuid4(),
        kind=RecordKind.NEW,
        dedup_key=f'memory:{ACCOUNT}:{SRC_CHAT}:{i}',
        origin='live',
        source=Endpoint(transport=TRANSPORT, address=SRC_CHAT),
        received_by=AccountRef(transport=TRANSPORT, account_id=ACCOUNT),
        external_id=str(i),
        text='x',
        event_at=now,
        received_at=now,
    )


def _sample_outbound(i: int) -> OutboundRecord:
    return OutboundRecord(
        idempotency_key=f'perf-out-{i}',
        target=Endpoint(transport=TRANSPORT, address=DST_CHAT),
        send_via=AccountRef(transport=TRANSPORT, account_id=ACCOUNT),
        text='x',
    )


def _sample_registry(i: int) -> RegistryRecord:
    return RegistryRecord(
        source_key=f'memory:{ACCOUNT}:{SRC_CHAT}',
        external_id=str(i),
        text='x',
        event_at=_now(),
    )


def _sample_event() -> AnalyticsEvent:
    return AnalyticsEvent(uid=uuid4(), kind='perf', at=_now())


async def _bench[T](op: Callable[[T], Awaitable[object]], inputs: Sequence[T]) -> float:
    """Прогнать ``op`` по пред-собранным ``inputs``; вернуть rec/s."""
    start = perf_counter()
    for item in inputs:
        await op(item)
    elapsed = perf_counter() - start
    return len(inputs) / elapsed if elapsed > 0 else 0.0


async def _bench_queue(queue: EventQueuePort, iterations: int) -> float:
    """Round-trip ``put → get → ack`` на пред-собранных envelope; rec/s."""
    envelopes = [
        QueueEnvelope(pipeline='perf', record=_sample_record(i))
        for i in range(iterations)
    ]
    start = perf_counter()
    for envelope in envelopes:
        await queue.put(envelope)
        item = await queue.get()
        await queue.ack(item)
    elapsed = perf_counter() - start
    return iterations / elapsed if elapsed > 0 else 0.0


async def _dispose(storage: StorageBundle) -> None:
    """Закрыть пул движка sqlite, если бэкенд его держит (memory — no-op)."""
    dispose = getattr(storage, 'dispose', None)
    if callable(dispose):
        await dispose()


def _close(queue: EventQueuePort) -> None:
    """Закрыть соединения persistqueue, если бэкенд их держит (memory — no-op)."""
    close = getattr(queue, 'close', None)
    if callable(close):
        close()


async def run_microbench(
    *,
    queue_backend: QueueBackendName = 'memory',
    storage_backend: StorageBackendName = 'memory',
    iterations: int = 2000,
    tmp_path: Path | None = None,
) -> MicrobenchReport:
    """
    Прогнать каждый порт в изоляции ``iterations`` раз и вернуть rec/s по
    каждому. ``queue`` использует ``queue_backend``; dedup/registry/outbox/
    analytics — ``storage_backend``. sqlite/persistqueue пишут в изолированный
    ``tmp_path`` (как в сквозном гарнесе).
    """
    settings = build_perf_settings(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        poll_interval=0.001,
        tmp_path=tmp_path,
    )
    plugins = build_perf_plugins(CountingSink())
    storage = build_storage(settings, plugins=plugins)
    queue = build_queue(settings, plugins=plugins)
    try:
        components = {
            'queue': await _bench_queue(queue, iterations),
            'dedup': await _bench(
                storage.dedup.mark_inbound,
                [f'perf-dedup-{i}' for i in range(iterations)],
            ),
            'registry': await _bench(
                storage.registry.upsert,
                [_sample_registry(i) for i in range(iterations)],
            ),
            'outbox': await _bench(
                storage.outbox.put, [_sample_outbound(i) for i in range(iterations)]
            ),
            'analytics': await _bench(
                storage.analytics.record, [_sample_event() for _ in range(iterations)]
            ),
        }
    finally:
        _close(queue)
        await _dispose(storage)

    return MicrobenchReport(
        queue_backend=queue_backend,
        storage_backend=storage_backend,
        iterations=iterations,
        components=components,
    )
