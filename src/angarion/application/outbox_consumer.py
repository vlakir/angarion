"""
Consumer командного outbox (§12.9, FR-5; T024, M5/C, фаза 4).

Pipeline-процесс фоном опрашивает командный outbox: атомарно
захватывает ``pending`` → ``taken`` (``CommandOutboxPort.take``),
диспетчеризует по виду и помечает терминально ``done`` / ``failed``.
Семантика at-least-once с защитой по статусу обеспечивается портом
(захват/пометка — ``UPDATE ... WHERE status=...``); исполнение здесь
идемпотентно по построению (отправка/catch-up повторяемы).

Виды команд v1: ``notify`` (отправка через ``SinkPort``),
``catchup`` (ручной catch-up источника через инъецированный колбэк над
listener'ами), ``restart_pipeline`` (взвести событие graceful-остановки
процесса — супервизор поднимет, §3.2). Расширение — новый член
``CommandKind`` + ветка диспетчеризации, механизм outbox не меняется.

Сбой исполнения → ``mark_failed`` + событие аналитики (``notify_failed``
для notify, иначе ``command_failed``); команда видна в аудите (разбор
ручной, аналог DLQ). Для ``notify`` это и есть «неблокирующее
уведомление» (§12.7): сбой не влияет на регистрацию, она уже состоялась.

Модуль без ``from __future__ import annotations``: pydantic-модель
``OutboundRecord`` собирается из payload в runtime.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Final
from uuid import uuid4

import structlog
from structlog.typing import FilteringBoundLogger

from angarion.domain.models import (
    AnalyticsEvent,
    CommandKind,
    OutboundRecord,
    OutboxCommand,
)
from angarion.domain.ports import AnalyticsPort, CommandOutboxPort, SinkPort

NOTIFY_FAILED: Final = 'notify_failed'
"""Вид события аналитики при сбое исполнения ``notify`` (§12.9)."""

COMMAND_FAILED: Final = 'command_failed'
"""Вид события аналитики при сбое исполнения прочих команд (§12.9)."""

CatchupFn = Callable[[str], Awaitable[None]]
"""Колбэк ручного catch-up источника (диспетчеризация по listener'ам)."""

RestartFn = Callable[[], None]
"""Колбэк graceful-перезапуска (взводит событие остановки процесса)."""


class OutboxConsumer:
    """Фоновый поллинг командного outbox с диспетчеризацией (§12.9)."""

    def __init__(
        self,
        *,
        command_outbox: CommandOutboxPort,
        sink: SinkPort,
        analytics: AnalyticsPort,
        catchup: CatchupFn,
        request_restart: RestartFn,
        poll_seconds: float = 5.0,
        take_limit: int = 10,
        log: FilteringBoundLogger | None = None,
    ) -> None:
        self._outbox = command_outbox
        self._sink = sink
        self._analytics = analytics
        self._catchup = catchup
        self._request_restart = request_restart
        self._poll_seconds = poll_seconds
        self._take_limit = take_limit
        self._log: FilteringBoundLogger = (
            log if log is not None else structlog.get_logger('angarion.outbox')
        )

    async def run(self) -> None:
        """Бесконечный цикл поллинга; отмена срабатывает на ``sleep`` (§14.7)."""
        while True:
            await self.poll_once()
            await asyncio.sleep(self._poll_seconds)

    async def poll_once(self) -> int:
        """Захватить и исполнить пачку команд; вернуть число обработанных."""
        commands = await self._outbox.take(self._take_limit)
        for command in commands:
            await self._dispatch(command)
        return len(commands)

    async def _dispatch(self, command: OutboxCommand) -> None:
        """Исполнить одну захваченную команду и пометить терминально."""
        try:
            result = await self._execute(command)
        except Exception as exc:  # изоляция: одна команда не валит цикл (§12.9)
            error = f'{type(exc).__name__}: {exc}'
            await self._outbox.mark_failed(command.uid, error)
            await self._record_failure(command, error)
            self._log.warning(
                'command_failed',
                kind=command.kind.value,
                command_uid=str(command.uid),
                error=error,
            )
            return
        await self._outbox.mark_done(command.uid, result=result)

    async def _execute(self, command: OutboxCommand) -> str | None:
        """Диспетчеризация по виду; возвращает короткий результат для аудита."""
        if command.kind is CommandKind.NOTIFY:
            record = OutboundRecord.model_validate(command.payload['record'])
            receipt = await self._sink.send(record)
            return f'sent:{receipt.external_id}'
        if command.kind is CommandKind.CATCHUP:
            source_key = command.payload['source_key']
            await self._catchup(source_key)
            return f'catchup:{source_key}'
        if command.kind is CommandKind.RESTART_PIPELINE:
            # пометка done — до взвода остановки: команда зафиксирована в БД
            # раньше, чем процесс начнёт гаснуть и отменит этот цикл (§3.2)
            await self._outbox.mark_done(command.uid, result='restarting')
            self._request_restart()
            return None
        msg = f'неизвестный вид команды: {command.kind!r}'  # pragma: no cover
        raise ValueError(msg)  # pragma: no cover

    async def _record_failure(self, command: OutboxCommand, error: str) -> None:
        kind = NOTIFY_FAILED if command.kind is CommandKind.NOTIFY else COMMAND_FAILED
        await self._analytics.record(
            AnalyticsEvent(
                uid=uuid4(),
                kind=kind,
                payload={
                    'command_kind': command.kind.value,
                    'command_uid': str(command.uid),
                    'error': error,
                },
                at=datetime.now(UTC),
            )
        )
