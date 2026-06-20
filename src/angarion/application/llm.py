"""
Встроенный процессор ``llm`` (§10.2, M4, T007): обработка текста события
через OpenAI-совместимую модель по HTTP (``/v1/chat/completions``).

Архитектура повторяет sender M3: вся логика — над **узкой границей**
(``LlmHttpClientPort``), реальная обёртка над ``httpx.AsyncClient``
бритвенно-тонкая, а сетевые ошибки нормализованы в telethon-free
``LlmTransportError`` — поэтому процессор тестируется на fake-границе
или ``httpx.MockTransport`` без сети (FR «Тестируемость»).

Промпты (``system``/``user``, опц. пер-видовые) рендерятся общим
Jinja2-движком (``application.templating``), что и ``template``. Ответ
парсится типизированной моделью (``ChatCompletionResponse``, W3), а не
индексированием сырого dict.

Ретраи (Q6): сеть/``5xx``/``429`` повторяются (``tenacity``,
``max_attempts``, exp-backoff), ``Retry-After`` при ``429`` уважается с
потолком ``timeout_s`` (W5); ``4xx≠429`` — без ретраев. Исчерпание →
``ProcessingError`` (worker делает re-enqueue по §8). Повторный вызов
под at-least-once расходует токены и может дать иной ответ (W2):
дублирующую доставку гасит outbox/``mark_delivered`` (§7.3).

Ключ авторизации (Q2): из env-переменной с именем ``api_key_env``; без
поля — запрос без заголовка ``Authorization`` (локальные модели).
Секрет не пишется в TOML и не логируется (§17.7, N3).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime. Живёт под extra ``angarion[llm]``
(httpx + tenacity), подключается entry point'ом ``angarion.processors``.
"""

import asyncio
import os
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from typing import Any, Final, Protocol, runtime_checkable

import httpx
from jinja2 import Template, TemplateSyntaxError
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SkipValidation,
    ValidationError,
)
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from angarion.application.templating import compile_record_template, render_compiled
from angarion.domain.errors import ConfigError, ProcessingError
from angarion.domain.models import (
    OutboundRecord,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Record,
    RecordKind,
    Verdict,
)
from angarion.domain.ports import ProcessorPort

_CHAT_COMPLETIONS_PATH: Final = '/chat/completions'


class LlmProcessorConfig(BaseModel):
    """
    ``processor_config`` процессора ``llm`` (FR §3 спеки T007).

    ``api_key_env`` — **имя** env-переменной с ключом, не сам ключ (Q2,
    §17.7). ``user_prompt_edited``/``user_prompt_deleted`` — опциональные
    пер-видовые промпты пользователя (fallback на ``user_prompt``).
    ``temperature``/``max_tokens`` опускаются из запроса при ``None``
    (модель решает сама).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    base_url: str
    model: str
    system_prompt: str
    user_prompt: str
    user_prompt_edited: str | None = None
    user_prompt_deleted: str | None = None
    api_key_env: str | None = None
    timeout_s: float = Field(default=60.0, gt=0)
    temperature: float | None = None
    max_tokens: int | None = Field(default=None, gt=0)
    max_attempts: int = Field(default=3, ge=1)


class ChatMessage(BaseModel):
    """Сообщение запроса OpenAI-совместимого API (``role`` + ``content``)."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    role: str
    content: str


class _ResponseMessage(BaseModel):
    """Сообщение в ``choices[*].message`` ответа; лишние поля игнорируем."""

    model_config = ConfigDict(extra='ignore')

    content: str | None = None


class _ResponseChoice(BaseModel):
    """Элемент ``choices`` ответа; лишние поля игнорируем."""

    model_config = ConfigDict(extra='ignore')

    message: _ResponseMessage = Field(default_factory=_ResponseMessage)


class ChatCompletionResponse(BaseModel):
    """
    Типизированный ответ ``/chat/completions`` (W3): парсим моделью, а не
    индексируем сырой dict. Незнакомые поля провайдера игнорируются.
    """

    model_config = ConfigDict(extra='ignore')

    choices: list[_ResponseChoice] = Field(default_factory=list)

    @property
    def content(self) -> str | None:
        """Текст первого choice (``choices[0].message.content``) или ``None``."""
        if not self.choices:
            return None
        return self.choices[0].message.content


class LlmTransportError(Exception):
    """Сетевой сбой/таймаут вызова LLM — повторяемый (§8, Q6)."""


class LlmHttpResult(BaseModel):
    """
    Результат одного HTTP-вызова на границе (не доменный DTO).

    ``body`` заполняется только разобранным JSON-объектом (иначе ``{}``);
    ``retry_after`` — секунды из заголовка ``Retry-After`` (``None`` для
    HTTP-date-формы — fallback на backoff).
    """

    model_config = ConfigDict(frozen=True)

    status_code: int
    retry_after: float | None = None
    body: dict[str, Any] = Field(default_factory=dict)


@runtime_checkable
class LlmHttpClientPort(Protocol):
    """Узкая граница HTTP-вызова LLM; fake/``MockTransport`` — для тестов."""

    async def post_chat(
        self,
        *,
        base_url: str,
        payload: dict[str, Any],
        api_key: str | None,
        timeout_s: float,
    ) -> LlmHttpResult:
        """POST ``{base_url}/chat/completions``; сеть → ``LlmTransportError``."""
        ...


def _parse_retry_after(value: str | None) -> float | None:
    """``Retry-After`` в секундах; HTTP-date-форму не поддерживаем (→ None)."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    """JSON-объект тела или ``{}`` (не-JSON / не-объект — терпим)."""
    try:
        data: Any = response.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


class HttpxLlmClient(BaseModel):
    """
    Реальная граница над ``httpx.AsyncClient`` (бритвенно-тонкая).

    ``transport`` инъектируется в тестах (``httpx.MockTransport``) —
    логика post/парсинга проверяется без сети. Клиент создаётся на вызов
    (worker конкурентен = 1, §6.3): без долгоживущего пула и хука
    закрытия.
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    transport: SkipValidation[httpx.AsyncBaseTransport | None] = None

    async def post_chat(
        self,
        *,
        base_url: str,
        payload: dict[str, Any],
        api_key: str | None,
        timeout_s: float,
    ) -> LlmHttpResult:
        """Выполнить POST; сетевые ошибки → ``LlmTransportError`` (Q6)."""
        headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
        url = f'{base_url.rstrip("/")}{_CHAT_COMPLETIONS_PATH}'
        try:
            async with httpx.AsyncClient(
                transport=self.transport, timeout=timeout_s
            ) as client:
                response = await client.post(url, json=payload, headers=headers)
        except httpx.RequestError as exc:
            msg = f'сетевой сбой вызова LLM: {exc}'
            raise LlmTransportError(msg) from exc
        return LlmHttpResult(
            status_code=response.status_code,
            retry_after=_parse_retry_after(response.headers.get('retry-after')),
            body=_safe_json(response),
        )


class _RetryableLlmError(Exception):
    """
    Внутренний маркер повторяемой ситуации (сеть/``5xx``/``429``).

    ``retry_after`` — желаемая пауза от сервера (``429``); ``None`` →
    exp-backoff. Конвертирует result-условия (``5xx``/``429``) и
    ``LlmTransportError`` в единый retryable-тип для ``tenacity``.
    """

    def __init__(self, *, detail: str, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(detail)


class _CompiledLlm(BaseModel):
    """Разобранный конфиг + скомпилированные промпты (кэш по пайплайну, W1)."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    config: LlmProcessorConfig
    system: SkipValidation[Template]
    user_base: SkipValidation[Template]
    user_edited: SkipValidation[Template] | None = None
    user_deleted: SkipValidation[Template] | None = None

    def user(self, kind: RecordKind) -> Template:
        """Пер-видовой user-промпт при наличии, иначе базовый."""
        if kind is RecordKind.EDITED and self.user_edited is not None:
            return self.user_edited
        if kind is RecordKind.DELETED and self.user_deleted is not None:
            return self.user_deleted
        return self.user_base


class LlmProcessor(BaseModel):
    """
    Встроенный процессор ``llm`` (§10.2): текст события → ответ
    OpenAI-совместимой модели → ``OutboundRecord`` на каждую цель.

    ``http`` — граница вызова (по умолчанию реальная ``HttpxLlmClient``;
    тесты подставляют fake). ``sleep`` инъектируется — ретраи тестируются
    детерминированно, без реальных пауз. Конфиг разбирается и промпты
    **компилируются** один раз на пайплайн, результат кэшируется (W1:
    worker конкурентен = 1, процессор-синглтон, конфиг фиксирован при
    старте). Структура конфига валидируется на старте через
    ``config_model`` (FR-0 T021, ``build_app``); синтаксис Jinja2-промпта
    всплывает ``ConfigError`` лениво, на первом событии.

    Конструкция композиции (A-2): JSON-контракт DTO не действует.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    name: str = 'llm'
    http: SkipValidation[LlmHttpClientPort] = Field(default_factory=HttpxLlmClient)
    backoff_base: float = 1.0
    backoff_cap: float = 60.0
    sleep: SkipValidation[Callable[[float], Awaitable[None]]] = asyncio.sleep
    _prepared: dict[str, _CompiledLlm] = PrivateAttr(default_factory=dict)

    def config_model(self) -> type[BaseModel] | None:
        """
        Схема ``processor_config`` для fail-fast на старте (FR-0 T021).

        Валидируется структура; синтаксис Jinja2-промптов компилируется
        лениво на первом событии (см. ``_config``, W1).
        """
        return LlmProcessorConfig

    def _config(self, ctx: PipelineContextData) -> _CompiledLlm:
        """Разобрать, скомпилировать промпты и закэшировать по пайплайну (W1)."""
        cached = self._prepared.get(ctx.pipeline)
        if cached is not None:
            return cached
        try:
            cfg = LlmProcessorConfig.model_validate(ctx.settings)
            prepared = _CompiledLlm(
                config=cfg,
                system=compile_record_template(cfg.system_prompt),
                user_base=compile_record_template(cfg.user_prompt),
                user_edited=(
                    compile_record_template(cfg.user_prompt_edited)
                    if cfg.user_prompt_edited
                    else None
                ),
                user_deleted=(
                    compile_record_template(cfg.user_prompt_deleted)
                    if cfg.user_prompt_deleted
                    else None
                ),
            )
        except (ValidationError, TemplateSyntaxError) as exc:
            msg = (
                f"процессор 'llm', пайплайн {ctx.pipeline!r}: "
                f'некорректный processor_config: {exc}'
            )
            raise ConfigError(msg) from exc
        self._prepared[ctx.pipeline] = prepared
        return prepared

    @staticmethod
    def _api_key(cfg: LlmProcessorConfig) -> str | None:
        """Ключ из env по имени ``api_key_env`` (Q2); пустой/нет → ConfigError."""
        if not cfg.api_key_env:
            return None
        api_key = os.environ.get(cfg.api_key_env)
        if not api_key:
            msg = (
                f"процессор 'llm': env-переменная {cfg.api_key_env!r} "
                f'(api_key_env) пуста или не задана'
            )
            raise ConfigError(msg)
        return api_key

    @staticmethod
    def _payload(prepared: _CompiledLlm, record: Record) -> dict[str, Any]:
        """Сформировать тело запроса: messages (Jinja2) + параметры генерации."""
        cfg = prepared.config
        messages = [
            ChatMessage(
                role='system', content=render_compiled(prepared.system, record)
            ).model_dump(),
            ChatMessage(
                role='user',
                content=render_compiled(prepared.user(record.kind), record),
            ).model_dump(),
        ]
        payload: dict[str, Any] = {'model': cfg.model, 'messages': messages}
        if cfg.temperature is not None:
            payload['temperature'] = cfg.temperature
        if cfg.max_tokens is not None:
            payload['max_tokens'] = cfg.max_tokens
        return payload

    async def _attempt(
        self, cfg: LlmProcessorConfig, payload: dict[str, Any], api_key: str | None
    ) -> str | None:
        """Один вызов модели: статус → retryable/ProcessingError/контент."""
        try:
            result = await self.http.post_chat(
                base_url=cfg.base_url,
                payload=payload,
                api_key=api_key,
                timeout_s=cfg.timeout_s,
            )
        except LlmTransportError as exc:
            raise _RetryableLlmError(detail=str(exc)) from exc
        if result.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            raise _RetryableLlmError(
                detail='429 Too Many Requests', retry_after=result.retry_after
            )
        if result.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            raise _RetryableLlmError(detail=f'{result.status_code} server error')
        if result.status_code >= HTTPStatus.BAD_REQUEST:
            msg = f"процессор 'llm': запрос отклонён моделью ({result.status_code})"
            raise ProcessingError(msg)
        return ChatCompletionResponse.model_validate(result.body).content

    async def _generate(
        self, cfg: LlmProcessorConfig, payload: dict[str, Any], api_key: str | None
    ) -> str | None:
        """Вызов с ретраями (Q6); исчерпание попыток → ProcessingError."""
        backoff = wait_exponential(multiplier=self.backoff_base, max=self.backoff_cap)

        def wait(retry_state: RetryCallState) -> float:
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            if isinstance(exc, _RetryableLlmError) and exc.retry_after is not None:
                return min(exc.retry_after, cfg.timeout_s)  # W5: потолок ожидания
            return backoff(retry_state)

        retryer = AsyncRetrying(
            retry=retry_if_exception_type(_RetryableLlmError),
            stop=stop_after_attempt(cfg.max_attempts),
            wait=wait,
            sleep=self.sleep,
            reraise=True,
        )
        try:
            return await retryer(self._attempt, cfg, payload, api_key)
        except _RetryableLlmError as exc:
            msg = f"процессор 'llm': исчерпаны попытки ({cfg.max_attempts}) — {exc}"
            raise ProcessingError(msg) from exc

    async def process(
        self,
        record: Record,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Текст записи → ответ модели → OutboundRecord на каждую цель."""
        if record.text is None:
            return ProcessingResult(verdict=Verdict.DROP, note='llm: запись без текста')
        prepared = self._config(ctx)
        api_key = self._api_key(prepared.config)
        content = await self._generate(
            prepared.config, self._payload(prepared, record), api_key
        )
        if not content:
            return ProcessingResult(
                verdict=Verdict.DROP, note='llm: пустой ответ модели'
            )
        outbound = [
            OutboundRecord(
                idempotency_key=svc.make_idempotency_key(record, spec.target, n),
                target=spec.target,
                send_via=spec.send_via,
                text=content,
            )
            for n, spec in enumerate(ctx.targets)
        ]
        return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


LLM: Final[ProcessorPort] = LlmProcessor()
"""Объект встроенного процессора ``llm`` — значение entry point
``angarion.processors:llm`` (подключается в фазе 3); §10.2, FR §3."""
