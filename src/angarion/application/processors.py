"""
Реестр процессоров и встроенный ``passthrough`` (§10.1–10.2 ТЗ, FR-17).

Регистрация: ``@processor('name')`` для async-функций с сигнатурой
``(event, ctx, svc) -> ProcessingResult`` либо ``register()`` для
готовых объектов ``ProcessorPort``. Загрузка внешних процессоров
через entry points ``angarion.processors`` — bootstrap (Фаза 4).

Процессор ``template`` (Jinja2) отложен к M4 (C-3).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

from collections.abc import Awaitable, Callable
from typing import Final

from pydantic import BaseModel, ConfigDict

from angarion.domain.errors import ConfigError
from angarion.domain.models import (
    InboundEvent,
    OutboundMessage,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Verdict,
)
from angarion.domain.ports import ProcessorPort

ProcessorFn = Callable[
    [InboundEvent, PipelineContextData, ProcessorServices],
    Awaitable[ProcessingResult],
]
"""Сигнатура функции-процессора (§10.1)."""


class FunctionProcessor(BaseModel):
    """
    Адаптер async-функции к ``ProcessorPort``. Конструкция композиции
    (A-2): JSON-контракт DTO на неё не распространяется.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    fn: ProcessorFn

    async def process(
        self,
        event: InboundEvent,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Делегировать обработку обёрнутой функции."""
        return await self.fn(event, ctx, svc)


_registry: dict[str, ProcessorPort] = {}


def register(proc: ProcessorPort) -> None:
    """Зарегистрировать процессор; повторное имя → ``ConfigError``."""
    if proc.name in _registry:
        msg = f'процессор уже зарегистрирован: {proc.name!r}'
        raise ConfigError(msg)
    _registry[proc.name] = proc


def processor(name: str) -> Callable[[ProcessorFn], ProcessorFn]:
    """Декоратор §10.1: регистрирует функцию, возвращая её как есть."""

    def decorate(fn: ProcessorFn) -> ProcessorFn:
        register(FunctionProcessor(name=name, fn=fn))
        return fn

    return decorate


def get_processor(name: str) -> ProcessorPort:
    """Процессор по имени; неизвестное имя → ``ConfigError`` с перечнем."""
    proc = _registry.get(name)
    if proc is None:
        known = ', '.join(sorted(_registry)) or '<пусто>'
        msg = f'неизвестный процессор: {name!r}; зарегистрированы: {known}'
        raise ConfigError(msg)
    return proc


def registered() -> dict[str, ProcessorPort]:
    """Снимок реестра (имя → процессор); мутации снимка реестр не трогают."""
    return dict(_registry)


@processor('passthrough')
async def passthrough(
    event: InboundEvent,
    ctx: PipelineContextData,
    svc: ProcessorServices,
) -> ProcessingResult:
    """
    Ретрансляция текста события во все цели как есть (§10.2).

    Событие без текста (DELETED, не восстановленный реестром) —
    DROP: ретранслировать нечего; ``text=None`` обязан переживать
    каждый процессор (§10.1).
    """
    if event.text is None:
        return ProcessingResult(
            verdict=Verdict.DROP, note='passthrough: событие без текста'
        )
    outbound = [
        OutboundMessage(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=event.text,
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


PASSTHROUGH: Final[ProcessorPort] = get_processor('passthrough')
"""Объект встроенного процессора — значение entry point
``angarion.processors`` библиотеки (§10.1, FR-17)."""
