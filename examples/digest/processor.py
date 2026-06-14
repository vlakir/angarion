"""
Пример stateful-процессора «дайджест» (§10.3, §16 M4; задача T019).

Демонстрирует, как написать **свой** процессор с состоянием поверх
публичного контракта angarion:

- состояние живёт в ``svc.state`` (KV с JSON-значениями, namespace =
  имя пайплайна) — здесь это одна Pydantic-модель ``DigestState`` под
  ключом ``state``;
- на каждом событии текст добавляется в накопитель, а при срабатывании
  условия сброса (по числу ``n`` и/или «возрасту» старейшего элемента
  ``max_age_s``) накопленное прогоняется через локальную
  OpenAI-совместимую модель (Ollama) и единой сводкой уходит на каждую
  ``ctx.targets``;
- **идемпотентность под at-least-once** (§10.3): учтённые ``dedup_key``
  хранятся в состоянии — в ``seen`` текущего батча и в ограниченном
  «хвосте» ``recent`` уже сброшенных, поэтому повторная доставка того
  же события ни до, ни после сброса не задваивает накопитель.

LLM-вызов переиспользует тонкую границу ``LlmHttpClientPort`` из
``angarion.application.llm`` (та же, что у встроенного ``llm``): в
демо — реальная ``HttpxLlmClient``, в unit-тесте подставляется fake,
поэтому логика дайджеста проверяется без сети.

Это **пример** (код в ``examples/``), а не встроенный процессор
``src/``: его задача — показать контракт stateful-процессора.

Модуль без ``from __future__ import annotations``: аннотации
Pydantic-моделей вычисляются в runtime.
"""

import os
from collections.abc import Callable
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Any, Final

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SkipValidation,
    ValidationError,
)

from angarion.application.llm import (
    ChatCompletionResponse,
    ChatMessage,
    HttpxLlmClient,
    LlmHttpClientPort,
    LlmTransportError,
)
from angarion.domain.errors import ConfigError, ProcessingError
from angarion.domain.models import (
    InboundEvent,
    OutboundMessage,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Verdict,
)
from angarion.domain.ports import ProcessorPort

_STATE_KEY: Final = 'state'


def _now_utc() -> datetime:
    """Текущее время в UTC (инъектируется в тестах для ``max_age_s``)."""
    return datetime.now(tz=UTC)


class DigestConfig(BaseModel):
    """
    ``processor_config`` дайджеста.

    Сброс — по числу ``n`` И/ИЛИ «возрасту» старейшего элемента
    ``max_age_s`` (проверка только на приходящем событии: worker
    событийный, без фонового шедулера — при затишье дайджест ждёт).
    ``recent_cap`` — размер хвоста сброшенных ``dedup_key`` (окно
    дедупа после сброса). Поля ``base_url``..``temperature`` —
    параметры LLM-саммари (как у встроенного ``llm``); ``api_key_env``
    — **имя** env-переменной с ключом, не сам ключ (§17.7).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    n: int = Field(default=5, ge=1)
    max_age_s: float | None = Field(default=None, gt=0)
    recent_cap: int = Field(default=100, ge=0)
    base_url: str
    model: str
    system_prompt: str
    user_prompt: str
    api_key_env: str | None = None
    timeout_s: float = Field(default=60.0, gt=0)
    temperature: float | None = None


class DigestItem(BaseModel):
    """Один накопленный элемент дайджеста."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    sender: str
    text: str
    event_at: AwareDatetime


class DigestState(BaseModel):
    """
    Состояние дайджеста в ``svc.state`` (JSON под ключом ``state``).

    ``items`` — накопленные элементы; ``seen`` — их ``dedup_key`` (дедуп
    внутри батча); ``recent`` — ограниченный хвост ``dedup_key`` уже
    сброшенных батчей (дедуп повторной доставки после сброса).
    ``started_at`` — момент (настенные часы) добавления первого элемента
    текущего батча: по нему считается возраст накопления для ``max_age_s``
    (не ``event_at`` сообщений — те могут быть старыми в catch-up).
    """

    model_config = ConfigDict(extra='forbid')

    items: list[DigestItem] = Field(default_factory=list)
    seen: list[str] = Field(default_factory=list)
    recent: list[str] = Field(default_factory=list)
    started_at: AwareDatetime | None = None


class DigestProcessor(BaseModel):
    """
    Stateful-процессор «дайджест»: накопление в ``svc.state`` + сброс
    LLM-сводкой на каждую цель (§10.3, §16 M4).

    ``http`` — граница вызова LLM (по умолчанию реальная
    ``HttpxLlmClient``; тест подставляет fake). ``now`` инъектируется —
    проверка ``max_age_s`` тестируется детерминированно.

    Конструкция композиции (A-2): JSON-контракт DTO на неё не действует.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    name: str = 'digest'
    http: SkipValidation[LlmHttpClientPort] = Field(default_factory=HttpxLlmClient)
    now: SkipValidation[Callable[[], datetime]] = _now_utc
    _config_cache: dict[str, DigestConfig] = PrivateAttr(default_factory=dict)

    def config_model(self) -> type[BaseModel] | None:
        """Схема ``processor_config`` для fail-fast на старте (FR-0 T021)."""
        return DigestConfig

    def _config(self, ctx: PipelineContextData) -> DigestConfig:
        """Разобрать и закэшировать ``processor_config`` по пайплайну."""
        cached = self._config_cache.get(ctx.pipeline)
        if cached is not None:
            return cached
        try:
            cfg = DigestConfig.model_validate(ctx.settings)
        except ValidationError as exc:
            msg = (
                f"процессор 'digest', пайплайн {ctx.pipeline!r}: "
                f'некорректный processor_config: {exc}'
            )
            raise ConfigError(msg) from exc
        self._config_cache[ctx.pipeline] = cfg
        return cfg

    @staticmethod
    def _api_key(cfg: DigestConfig) -> str | None:
        """Ключ из env по имени ``api_key_env``; пустой/нет → ConfigError."""
        if not cfg.api_key_env:
            return None
        api_key = os.environ.get(cfg.api_key_env)
        if not api_key:
            msg = (
                f"процессор 'digest': env-переменная {cfg.api_key_env!r} "
                f'(api_key_env) пуста или не задана'
            )
            raise ConfigError(msg)
        return api_key

    @staticmethod
    async def _load(svc: ProcessorServices) -> DigestState:
        """Прочитать состояние из ``svc.state`` (пусто → новое)."""
        raw = await svc.state.get(_STATE_KEY)
        if raw is None:
            return DigestState()
        return DigestState.model_validate_json(raw)

    @staticmethod
    async def _save(svc: ProcessorServices, state: DigestState) -> None:
        """Записать состояние в ``svc.state`` (JSON-строка)."""
        await svc.state.set(_STATE_KEY, state.model_dump_json())

    def _should_flush(self, state: DigestState, cfg: DigestConfig) -> bool:
        """Сброс по числу ``n`` или возрасту старейшего элемента."""
        if not state.items:
            return False
        if len(state.items) >= cfg.n:
            return True
        if cfg.max_age_s is not None and state.started_at is not None:
            age = (self.now() - state.started_at).total_seconds()
            if age >= cfg.max_age_s:
                return True
        return False

    @staticmethod
    def _flushed(state: DigestState, cfg: DigestConfig) -> DigestState:
        """Состояние после сброса: накопитель пуст, хвост ``recent`` обрезан."""
        merged = state.recent + state.seen
        tail = merged[-cfg.recent_cap :] if cfg.recent_cap else []
        return DigestState(items=[], seen=[], recent=tail)

    async def _summarize(self, state: DigestState, cfg: DigestConfig) -> str:
        """Прогнать накопленный батч через LLM; пустой ответ → сырой батч."""
        body = '\n'.join(f'- {it.sender}: {it.text}' for it in state.items)
        messages = [
            ChatMessage(role='system', content=cfg.system_prompt).model_dump(),
            ChatMessage(
                role='user', content=f'{cfg.user_prompt}\n\n{body}'
            ).model_dump(),
        ]
        payload: dict[str, Any] = {'model': cfg.model, 'messages': messages}
        if cfg.temperature is not None:
            payload['temperature'] = cfg.temperature
        try:
            result = await self.http.post_chat(
                base_url=cfg.base_url,
                payload=payload,
                api_key=self._api_key(cfg),
                timeout_s=cfg.timeout_s,
            )
        except LlmTransportError as exc:
            msg = f"процессор 'digest': сбой вызова LLM — {exc}"
            raise ProcessingError(msg) from exc
        if result.status_code >= HTTPStatus.BAD_REQUEST:
            msg = f"процессор 'digest': LLM вернула {result.status_code}"
            raise ProcessingError(msg)
        content = ChatCompletionResponse.model_validate(result.body).content
        return content or body

    async def process(
        self,
        event: InboundEvent,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Накопить событие; при срабатывании условия — сбросить дайджест."""
        cfg = self._config(ctx)
        state = await self._load(svc)
        known = event.dedup_key in state.seen or event.dedup_key in state.recent
        if not known:
            if event.text is None:
                return ProcessingResult(
                    verdict=Verdict.DROP, note='digest: событие без текста'
                )
            if not state.items:
                state.started_at = self.now()
            state.items.append(
                DigestItem(
                    sender=event.sender_name or 'unknown',
                    text=event.text,
                    event_at=event.event_at,
                )
            )
            state.seen.append(event.dedup_key)
            # Персистим накопление ДО вызова LLM: если сброс упадёт и
            # событие переотправят, dedup_key уже в seen — повтор не
            # задвоит элемент (идемпотентность под at-least-once).
            await self._save(svc, state)
        if not self._should_flush(state, cfg):
            return ProcessingResult(
                verdict=Verdict.DROP, note=f'digest: накоплено {len(state.items)}'
            )
        summary = await self._summarize(state, cfg)
        outbound = [
            OutboundMessage(
                idempotency_key=svc.make_idempotency_key(event, spec.target, n),
                target=spec.target,
                send_via=spec.send_via,
                text=summary,
            )
            for n, spec in enumerate(ctx.targets)
        ]
        await self._save(svc, self._flushed(state, cfg))
        return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


DIGEST: Final[ProcessorPort] = DigestProcessor()
"""Готовый объект процессора — его ``run.py`` регистрирует перед запуском
(``angarion.application.processors.register``); §10.1."""
