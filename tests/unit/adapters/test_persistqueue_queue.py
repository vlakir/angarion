"""
Частности persistqueue-адаптера сверх контракта (FR-1, FR-2 спеки
T003): персистентность через переоткрытие, JSON-хранение (не pickle),
receipt = pqid, отменяемость get(), фабрика бэкенда.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from angarion.testing import make_envelope

from angarion.adapters.queue.persistqueue_ import QUEUE_BACKEND, PersistQueue
from angarion.config import QueueConfig
from angarion.domain.errors import ConfigError
from angarion.domain.models import QueueDepth
from angarion.domain.ports import EventQueuePort

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


@pytest.fixture
def open_queue(tmp_path: Path) -> Iterator[Callable[[], PersistQueue]]:
    """
    Фабрика «рестарта процесса»: каждый вызов — новый инстанс поверх
    того же файла ``queue.db``; по завершении теста соединения
    закрываются.
    """
    opened: list[PersistQueue] = []

    def factory() -> PersistQueue:
        queue = PersistQueue(path=tmp_path / 'queue.db')
        opened.append(queue)
        return queue

    yield factory
    for queue in opened:
        queue.close()


async def test_pending_envelope_survives_reopen(
    open_queue: Callable[[], PersistQueue],
) -> None:
    envelope = make_envelope()
    await open_queue().put(envelope)
    assert (await open_queue().get()).envelope == envelope


async def test_unacked_stays_unacked_until_recover_after_reopen(
    open_queue: Callable[[], PersistQueue],
) -> None:
    """Падение после get(): элемент не теряется и не выдаётся повторно
    без явного recover() (§8 — recover вызывает ядро на старте)."""
    await open_queue().put(make_envelope())
    await open_queue().get()

    restarted = open_queue()
    assert await restarted.depth() == QueueDepth(pending=0, unacked=1)
    assert await restarted.recover() == 1
    assert await restarted.depth() == QueueDepth(pending=1, unacked=0)


async def test_acked_envelope_not_redelivered_after_reopen(
    open_queue: Callable[[], PersistQueue],
) -> None:
    queue = open_queue()
    await queue.put(make_envelope())
    await queue.ack(await queue.get())

    restarted = open_queue()
    assert await restarted.recover() == 0
    assert await restarted.depth() == QueueDepth(pending=0, unacked=0)


def _row_count(tmp_path: Path) -> int:
    """Всего строк в таблице очереди (pending + unacked + acked + failed)."""
    with closing(sqlite3.connect(tmp_path / 'queue.db')) as conn:
        (count,) = conn.execute('SELECT COUNT(*) FROM ack_queue_default').fetchone()
    return int(count)


async def _put_ack(queue: PersistQueue, count: int) -> None:
    """Положить и сразу подтвердить ``count`` envelope'ов (→ acked-строки)."""
    for i in range(count):
        await queue.put(make_envelope(pipeline=f'p{i}'))
    for _ in range(count):
        await queue.ack(await queue.get())


async def test_purge_acked_deletes_old_keeping_latest(
    open_queue: Callable[[], PersistQueue], tmp_path: Path
) -> None:
    """T016: ретеншн оставляет новейшие ``keep_latest`` acked-строк."""
    total, keep = 5, 2
    queue = open_queue()
    await _put_ack(queue, total)
    assert _row_count(tmp_path) == total  # acked копятся в queue.db

    deleted = await queue.purge_acked(keep_latest=keep)

    assert deleted == total - keep
    assert _row_count(tmp_path) == keep
    # чистка персистентна — переживает «рестарт процесса»
    assert _row_count(tmp_path) == keep
    restarted = open_queue()
    assert await restarted.recover() == 0  # ничего не воскрешается


async def test_purge_acked_keep_zero_deletes_all_acked(
    open_queue: Callable[[], PersistQueue], tmp_path: Path
) -> None:
    queue = open_queue()
    await _put_ack(queue, 4)
    assert await queue.purge_acked(keep_latest=0) == 4
    assert _row_count(tmp_path) == 0


async def test_purge_acked_leaves_pending_and_unacked(
    open_queue: Callable[[], PersistQueue], tmp_path: Path
) -> None:
    """Чистятся только acked: pending и unacked остаются на диске."""
    queue = open_queue()
    await queue.put(make_envelope(pipeline='acked'))
    await queue.ack(await queue.get())  # 1 acked
    await queue.put(make_envelope(pipeline='pending'))
    await queue.get()  # взята → 1 unacked
    await queue.put(make_envelope(pipeline='pending2'))  # 1 pending

    deleted = await queue.purge_acked(keep_latest=0)

    assert deleted == 1  # удалена только подтверждённая
    assert _row_count(tmp_path) == 2  # 1 pending + 1 unacked уцелели
    assert await queue.depth() == QueueDepth(pending=1, unacked=1)


async def test_purge_acked_is_idempotent(
    open_queue: Callable[[], PersistQueue],
) -> None:
    queue = open_queue()
    await _put_ack(queue, 3)
    await queue.purge_acked(keep_latest=0)
    assert await queue.purge_acked(keep_latest=0) == 0


async def test_purge_acked_after_close_raises(tmp_path: Path) -> None:
    queue = PersistQueue(path=tmp_path / 'queue.db')
    queue.close()
    with pytest.raises(RuntimeError, match='закрыта'):
        await queue.purge_acked(keep_latest=0)


async def test_db_stores_envelope_as_json_text_not_pickle(
    open_queue: Callable[[], PersistQueue], tmp_path: Path
) -> None:
    """FR-1: сериализация — model_dump_json(), в БД лежит JSON-текст."""
    envelope = make_envelope()
    await open_queue().put(envelope)

    with closing(sqlite3.connect(tmp_path / 'queue.db')) as conn:
        (raw,) = conn.execute('SELECT data FROM ack_queue_default').fetchone()
    assert isinstance(raw, str)
    assert json.loads(raw) == json.loads(envelope.model_dump_json())


async def test_waiting_get_polls_and_stays_cancellable(tmp_path: Path) -> None:
    """A-7: ожидание — поллинг коротким таймаутом, задача отменяема."""
    queue = PersistQueue(path=tmp_path / 'queue.db', poll_interval=0.01)
    task = asyncio.create_task(queue.get())
    await asyncio.sleep(0.05)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    queue.close()


async def test_receipt_is_persistqueue_pqid(
    open_queue: Callable[[], PersistQueue],
) -> None:
    queue = open_queue()
    await queue.put(make_envelope())
    item = await queue.get()
    assert isinstance(item.receipt, int)


async def test_close_right_after_cancel_waits_for_inflight_poll(
    tmp_path: Path,
) -> None:
    """Регрессия (CI 3.14, сегфолт): отмена get() не прерывает поток
    to_thread с блокирующим поллом; close() обязан дождаться его
    завершения, а не закрывать соединения под живым запросом."""
    queue = PersistQueue(path=tmp_path / 'queue.db', poll_interval=0.05)
    task = asyncio.create_task(queue.get())
    await asyncio.sleep(0.02)  # полл ушёл в blocking get
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    queue.close()  # сразу после отмены: ждёт in-flight, не падает


async def test_operations_after_close_raise(tmp_path: Path) -> None:
    queue = PersistQueue(path=tmp_path / 'queue.db')
    queue.close()
    with pytest.raises(RuntimeError, match='закрыта'):
        await queue.put(make_envelope())


async def test_close_is_idempotent(
    open_queue: Callable[[], PersistQueue],
) -> None:
    queue = open_queue()
    await queue.put(make_envelope())
    queue.close()
    queue.close()


def test_adapter_satisfies_port(
    open_queue: Callable[[], PersistQueue],
) -> None:
    assert isinstance(open_queue(), EventQueuePort)


def test_backend_factory_builds_queue_from_config(tmp_path: Path) -> None:
    config = QueueConfig.model_validate(
        {'backend': 'persistqueue', 'path': str(tmp_path / 'queue.db')}
    )
    assert QUEUE_BACKEND.name == 'persistqueue'
    queue = QUEUE_BACKEND.make(config)
    assert isinstance(queue, EventQueuePort)
    queue.close()


def test_backend_factory_without_path_fails(tmp_path: Path) -> None:
    config = QueueConfig.model_validate({'backend': 'persistqueue'})
    with pytest.raises(ConfigError, match='path'):
        QUEUE_BACKEND.make(config)
