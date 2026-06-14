"""
Реестр процессоров и встроенный ``passthrough`` (§10.1–10.2 ТЗ, FR-17).

Регистрация: ``@processor('name')`` для async-функций с сигнатурой
``(event, ctx, svc) -> ProcessingResult`` либо ``register()`` для
готовых объектов ``ProcessorPort``. Загрузка внешних процессоров
через entry points ``angarion.processors`` — bootstrap (Фаза 4).

Встроенные ``passthrough`` и ``template`` регистрируются при импорте
модуля (``jinja2`` — core-зависимость); ``llm`` живёт в отдельном
модуле под extra ``angarion[llm]`` и подключается через entry points.

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

from collections.abc import Awaitable, Callable
from typing import Final

from jinja2 import Template, TemplateSyntaxError
from pydantic import BaseModel, ConfigDict, PrivateAttr, SkipValidation, ValidationError

from angarion.application.templating import compile_event_template, render_compiled
from angarion.domain.errors import ConfigError
from angarion.domain.models import (
    EventKind,
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

    def config_model(self) -> type[BaseModel] | None:
        """Функция-процессор не объявляет схему конфига (FR-0 T021)."""
        return None

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


class TemplateProcessorConfig(BaseModel):
    """
    ``processor_config`` процессора ``template`` (§10.2, FR §3 спеки T007):
    базовый ``template`` плюс опциональные пер-видовые ``edited``/``deleted``
    (fallback на базовый, если пер-видовой не задан).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    template: str
    edited: str | None = None
    deleted: str | None = None


class _CompiledTemplates(BaseModel):
    """Скомпилированные шаблоны видов события (кэш по пайплайну, W1)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    base: SkipValidation[Template]
    edited: SkipValidation[Template] | None = None
    deleted: SkipValidation[Template] | None = None

    def select(self, kind: EventKind) -> Template:
        """Шаблон под вид события: пер-видовой при наличии, иначе базовый."""
        if kind is EventKind.MESSAGE_EDITED and self.edited is not None:
            return self.edited
        if kind is EventKind.MESSAGE_DELETED and self.deleted is not None:
            return self.deleted
        return self.base


class TemplateProcessor(BaseModel):
    """
    Встроенный процессор ``template`` (§10.2): детерминированное
    переписывание события Jinja2-шаблоном по его полям, без LLM.

    Шаблон выбирается по виду события (база / ``edited`` / ``deleted`` с
    fallback на базу). Пустой рендер → DROP (FR §3, N1): для DELETED без
    ``deleted``-шаблона базовый ``{{ text }}`` (``text=None``) отрендерится
    пусто.

    ``processor_config`` разбирается и шаблоны **компилируются** один раз
    на пайплайн, результат кэшируется (W1: worker конкурентен = 1,
    процессор — синглтон, конфиг фиксирован при старте). Структура конфига
    валидируется на старте через ``config_model`` (FR-0 T021,
    ``build_app``); синтаксис Jinja2-шаблона всплывает ``ConfigError`` уже
    лениво, на первом событии (компиляция в ``_config``).

    Конструкция композиции (A-2): JSON-контракт DTO не действует.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str = 'template'
    _compiled: dict[str, _CompiledTemplates] = PrivateAttr(default_factory=dict)

    def config_model(self) -> type[BaseModel] | None:
        """
        Схема ``processor_config`` для fail-fast на старте (FR-0 T021).

        Валидируется структура; синтаксис Jinja2-шаблонов по-прежнему
        компилируется лениво на первом событии (см. ``_config``, W1).
        """
        return TemplateProcessorConfig

    def _config(self, ctx: PipelineContextData) -> _CompiledTemplates:
        """Разобрать, скомпилировать и закэшировать шаблоны по пайплайну (W1)."""
        cached = self._compiled.get(ctx.pipeline)
        if cached is not None:
            return cached
        try:
            cfg = TemplateProcessorConfig.model_validate(ctx.settings)
            compiled = _CompiledTemplates(
                base=compile_event_template(cfg.template),
                edited=compile_event_template(cfg.edited) if cfg.edited else None,
                deleted=compile_event_template(cfg.deleted) if cfg.deleted else None,
            )
        except (ValidationError, TemplateSyntaxError) as exc:
            msg = (
                f"процессор 'template', пайплайн {ctx.pipeline!r}: "
                f'некорректный processor_config: {exc}'
            )
            raise ConfigError(msg) from exc
        self._compiled[ctx.pipeline] = compiled
        return compiled

    async def process(
        self,
        event: InboundEvent,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Отрендерить шаблон вида события → OutboundMessage на каждую цель."""
        text = render_compiled(self._config(ctx).select(event.kind), event)
        if not text:
            return ProcessingResult(
                verdict=Verdict.DROP, note='template: пустой рендер'
            )
        outbound = [
            OutboundMessage(
                idempotency_key=svc.make_idempotency_key(event, spec.target, n),
                target=spec.target,
                send_via=spec.send_via,
                text=text,
            )
            for n, spec in enumerate(ctx.targets)
        ]
        return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


register(TemplateProcessor())
TEMPLATE: Final[ProcessorPort] = get_processor('template')
"""Объект встроенного процессора ``template`` — eager-регистрация при
импорте (``jinja2`` core); §10.2, FR §3 спеки T007."""
