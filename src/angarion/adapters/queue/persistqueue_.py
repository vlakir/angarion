"""
Персистентная очередь событий на persist-queue (§12.2; FR-1, FR-2
спеки T003): ``SQLiteAckQueue`` поверх файла ``queue.db``.

Envelope хранится строкой ``model_dump_json()`` — в БД лежит JSON-текст,
не pickle. Синхронный API persistqueue вызывается через
``asyncio.to_thread()``; блокирующий ``get()`` — поллинг коротким
таймаутом, чтобы ожидающая задача оставалась отменяемой (A-7).

Имя модуля с подчёркиванием — чтобы не перекрывать импорт самой
библиотеки ``persistqueue``.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Final

from persistqueue import SQLiteAckQueue
from persistqueue.exceptions import Empty
from pydantic import BaseModel, ConfigDict, ValidationError

from angarion.domain.errors import ConfigError
from angarion.domain.models import QueueDepth, QueueEnvelope, QueueItem
from angarion.domain.plugin import QueueBackend

if TYPE_CHECKING:
    from collections.abc import Callable

    from angarion.config import QueueConfig

DEFAULT_POLL_INTERVAL: Final = 0.1
"""Таймаут одного цикла поллинга блокирующего ``get()`` (секунды, A-7)."""


class _JsonStringSerializer:
    """
    Identity-сериализатор для ``SQLiteAckQueue``: envelope уже
    сериализован ``model_dump_json()``, в колонку ``data`` кладётся
    сама JSON-строка (FR-1: строки, не pickle).
    """

    @staticmethod
    def dumps(item: str) -> str:
        return item

    @staticmethod
    def loads(data: bytes | str) -> str:
        return data.decode() if isinstance(data, bytes) else data


class PersistQueue:
    """
    ``EventQueuePort`` на ``persistqueue.SQLiteAckQueue``: FIFO,
    at-least-once, переживает рестарт процесса (kill -9 включительно).

    Receipt — ``pqid`` persistqueue (непрозрачен ядру по контракту).
    Выданные receipt'ы учитываются локально: ack/nack чужого или уже
    закрытого receipt'а — no-op (persistqueue сам по id статусы не
    проверяет). ``auto_resume`` выключен — возврат unacked в очередь
    делает только явный ``recover()`` на старте (§8).
    """

    def __init__(
        self, path: Path | str, poll_interval: float = DEFAULT_POLL_INTERVAL
    ) -> None:
        db_file = Path(path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        self._queue = SQLiteAckQueue(
            path=str(db_file.parent),
            db_file_name=db_file.name,
            auto_resume=False,
            multithreading=True,
            serializer=_JsonStringSerializer,
        )
        self._poll_interval = poll_interval
        self._issued: set[int] = set()
        self._mutex = threading.Lock()
        self._idle = threading.Condition(self._mutex)
        self._inflight = 0
        self._closed = False

    def _call[T](self, op: Callable[..., T], /, *args: object, **kwargs: object) -> T:
        """
        Выполнить операцию persistqueue с учётом in-flight (для
        ``close()``): отмена задачи не прерывает поток ``to_thread``,
        и закрытие соединений под живым запросом роняет ``sqlite3``
        сегфолтом (поймано CI на 3.14).
        """
        with self._mutex:
            if self._closed:
                msg = 'очередь закрыта'
                raise RuntimeError(msg)
            self._inflight += 1
        try:
            return op(*args, **kwargs)
        finally:
            with self._idle:
                self._inflight -= 1
                if self._inflight == 0:
                    self._idle.notify_all()

    async def put(self, item: QueueEnvelope) -> None:
        """Положить envelope в хвост очереди (JSON-строкой)."""
        await asyncio.to_thread(self._call, self._queue.put, item.model_dump_json())

    async def get(self) -> QueueItem:
        """Ждать голову очереди поллингом; элемент переходит в unacked."""
        while True:
            try:
                raw = await asyncio.to_thread(
                    self._call,
                    self._queue.get,
                    block=True,
                    timeout=self._poll_interval,
                    raw=True,
                )
            except Empty:
                continue
            receipt: int = raw['pqid']
            self._issued.add(receipt)
            envelope = QueueEnvelope.model_validate_json(raw['data'])
            return QueueItem(envelope=envelope, receipt=receipt)

    async def ack(self, item: QueueItem) -> None:
        """Подтвердить обработку; повторный ack — no-op."""
        if item.receipt not in self._issued:
            return
        self._issued.discard(item.receipt)
        await asyncio.to_thread(self._call, self._queue.ack, id=item.receipt)

    async def nack(self, item: QueueItem) -> None:
        """Аварийный возврат в очередь; после ack — no-op."""
        if item.receipt not in self._issued:
            return
        self._issued.discard(item.receipt)
        await asyncio.to_thread(self._call, self._queue.nack, id=item.receipt)

    async def recover(self) -> int:
        """Вернуть все unacked (включая оставшиеся от падения) в очередь."""
        return await asyncio.to_thread(self._call, self._recover_sync)

    def _recover_sync(self) -> int:
        count: int = self._queue.unack_count()
        self._queue.resume_unack_tasks()
        self._issued.clear()
        return count

    async def depth(self) -> QueueDepth:
        """Текущие pending/unacked (§17.5)."""
        return await asyncio.to_thread(self._call, self._depth_sync)

    async def purge_acked(self, keep_latest: int) -> int:
        """
        Ретеншн (§17.3, T016): удалить acked-строки сверх новейших
        ``keep_latest``, вернуть число удалённых. SQLiteAckQueue не
        вычищает подтверждённые записи сам — без чистки ``queue.db`` растёт
        бессрочно (T016). nacked/failed (``clear_ack_failed=False``) и
        pending/unacked не затрагиваются.
        """
        return await asyncio.to_thread(self._call, self._purge_acked_sync, keep_latest)

    def _purge_acked_sync(self, keep_latest: int) -> int:
        before: int = self._queue.acked_count()
        if before <= keep_latest:  # чистить нечего сверх буфера
            return 0  # и заодно: clear_acked_data c OFFSET без LIMIT — syntax error
        # max_delete = before (а не 0): за вызов выгребаем весь хвост сверх
        # буфера — активная очередь не должна копить acked между prune-циклами;
        # одновременно это даёт LIMIT, без которого SQLite отвергает OFFSET.
        self._queue.clear_acked_data(max_delete=before, keep_latest=keep_latest)
        return before - self._queue.acked_count()

    def close(self) -> None:
        """
        Закрыть соединения с ``queue.db``; идемпотентно. Сверх порта
        ``EventQueuePort``: нужно тестам и graceful shutdown (вызов из
        жизненного цикла приложения — M3).

        Дожидается завершения in-flight операций: после отмены
        ожидающего ``get()`` его блокирующий полл ещё ≤ poll_interval
        живёт в потоке ``to_thread``. Операции после закрытия —
        ``RuntimeError``.
        """
        with self._idle:
            first_close = not self._closed
            self._closed = True
            while self._inflight > 0:
                self._idle.wait()
        if first_close:
            self._queue.close()

    def _depth_sync(self) -> QueueDepth:
        return QueueDepth(
            pending=self._queue.qsize(), unacked=self._queue.unack_count()
        )


class _PersistQueueSettings(BaseModel):
    """Бэкенд-специфичные ключи секции ``[queue]`` (FR-2): путь к ``queue.db``."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    path: Path


def _make_queue(config: QueueConfig) -> PersistQueue:
    """Фабрика очереди для entry point ``angarion.queues`` (FR-2)."""
    try:
        settings = _PersistQueueSettings.model_validate(config.model_dump())
    except ValidationError as exc:
        msg = f'[queue] backend="persistqueue": некорректная секция: {exc}'
        raise ConfigError(msg) from exc
    return PersistQueue(path=settings.path)


QUEUE_BACKEND: Final = QueueBackend(name='persistqueue', make=_make_queue)
"""Значение entry point ``angarion.queues:persistqueue``."""
