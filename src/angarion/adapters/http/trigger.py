"""
HTTP write-ручки ручного триггера (T038, §12.5): первое штатное write-
расширение встроенного API помимо admin-управления.

Две ручки под ``/api/v1``: ``POST /trigger`` (event-семантика — впрыск
события через ``IngestService`` → router/dedup/реестр/fan-out) и
``POST /run/{pipeline}`` (pipeline-семантика — сырой ``QueueEnvelope`` в
очередь, минуя router/dedup). Тело — упрощённый :class:`ManualEvent`
**или** готовый ``Record`` (ровно одно из ``event``/``record``).

Авторизация — **отдельный API-ключ** (заголовок ``X-API-Key``, секрет
``api.trigger_token``), не admin-сессия fastapi-users: машинный путь для
служб/CI (A4). Пустой токен → ручки выключены (``503``); нет ключа →
``401``; неверный → ``403``. Сравнение — постоянного времени.

Диспетчеризация combined/split по ``deps.ingest`` (A1/A8): combined зовёт
``ingest`` напрямую (меньше латентность); api-роль (``ingest is None``)
кладёт команду ``INJECT`` в ``CommandOutbox`` — исполнит consumer pipeline-
процесса. Pipeline-путь идёт через ``queue.put`` в обоих режимах (api-
процесс уже пишет в общий ``queue.db``).

Без ``from __future__ import annotations``: pydantic-схемы и DI fastapi
вычисляются в runtime.
"""

import secrets
import uuid
from typing import Annotated, Final, Self, cast

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, ConfigDict, model_validator

from angarion.adapters.http.deps import AngarionDeps, DepsDep
from angarion.adapters.http.ops import record_admin_op, request_inject
from angarion.application.manual import ManualEvent, build_manual_record
from angarion.domain.models import QueueEnvelope, Record

API_KEY_HEADER: Final = 'X-API-Key'
"""Заголовок машинного API-ключа ручного триггера."""

API_KEY_ACTOR: Final = 'api-key'
"""Субъект аудита (``by``) для триггера по API-ключу (не пользователь сессии)."""

RUN_PIPELINE_OP: Final = 'run_pipeline'
"""Вид аудита прямого запуска именованного пайплайна (§12.8)."""


async def require_api_key(
    deps: DepsDep,
    x_api_key: Annotated[str | None, Header(alias=API_KEY_HEADER)] = None,
) -> str:
    """
    Авторизация write-ручки ручного триггера по API-ключу (T038).

    Пустой ``api.trigger_token`` → ручка выключена (``503``); отсутствие
    заголовка → ``401``; несовпадение (постоянного времени) → ``403``.
    """
    token = deps.settings.api.trigger_token
    if not token:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, 'manual trigger disabled'
        )
    if x_api_key is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, 'API key required')
    if not secrets.compare_digest(x_api_key, token):
        raise HTTPException(status.HTTP_403_FORBIDDEN, 'invalid API key')
    return x_api_key


ApiKeyDep = Annotated[str, Depends(require_api_key)]


class TriggerBody(BaseModel):
    """
    Тело ручного триггера: ровно одно из ``event`` (упрощённый payload) /
    ``record`` (готовый ``Record``). Оба заданы или оба пусты → ``422``.
    """

    model_config = ConfigDict(extra='forbid')

    event: ManualEvent | None = None
    record: Record | None = None

    @model_validator(mode='after')
    def _exactly_one(self) -> Self:
        if (self.event is None) == (self.record is None):
            msg = 'ровно одно из event/record должно быть задано'
            raise ValueError(msg)
        return self


class TriggerAccepted(BaseModel):
    """Подтверждение приёма ручного триггера (202): uid записи и режим."""

    record_uid: uuid.UUID
    mode: str
    """``ingested`` (combined event), ``queued`` (split event через outbox)
    или ``staged`` (прямой запуск пайплайна)."""


def _to_record(body: TriggerBody) -> Record:
    """
    Упрощённый ``event`` — через фабрику; готовый ``record`` — с
    форсированным ``origin='manual'`` (контракт ручного триггера, см.
    ``AngarionApp._coerce``).
    """
    if body.event is not None:
        return build_manual_record(body.event)
    record = cast('Record', body.record)  # валидатор гарантирует непустой record
    return record.model_copy(update={'origin': 'manual'})


async def submit_event(deps: AngarionDeps, *, record: Record, by: str) -> str:
    """
    Впрыск события (event-семантика): combined → прямой ``ingest``; split
    (``ingest is None``) → команда ``INJECT`` в ``CommandOutbox``. Возвращает
    режим (``ingested`` / ``queued``) для подтверждения.
    """
    if deps.ingest is not None:
        await deps.ingest.ingest(record)
        return 'ingested'
    await request_inject(deps.command_outbox, deps.analytics, record=record, by=by)
    return 'queued'


async def run_pipeline(
    deps: AngarionDeps, *, pipeline: str, record: Record, by: str
) -> None:
    """
    Прямой запуск именованного пайплайна (pipeline-семантика): сырой
    ``QueueEnvelope`` в очередь, минуя router/dedup (Q7). Имя обязано быть
    в конфиге — иначе ``ValueError`` (роут → ``422``). Пишет аудит.
    """
    if pipeline not in deps.settings.pipelines:
        msg = f'неизвестный пайплайн: {pipeline!r}'
        raise ValueError(msg)
    await deps.queue.put(QueueEnvelope(pipeline=pipeline, record=record))
    await record_admin_op(
        deps.analytics,
        operation=RUN_PIPELINE_OP,
        by=by,
        pipeline=pipeline,
        details={'record_uid': str(record.uid)},
    )


router = APIRouter(prefix='/api/v1', tags=['trigger'])


@router.post('/trigger', status_code=status.HTTP_202_ACCEPTED)
async def post_trigger(
    body: TriggerBody, deps: DepsDep, _: ApiKeyDep
) -> TriggerAccepted:
    """Впрыск события в работающий конвейер (API-ключ; event-семантика)."""
    record = _to_record(body)
    mode = await submit_event(deps, record=record, by=API_KEY_ACTOR)
    return TriggerAccepted(record_uid=record.uid, mode=mode)


@router.post('/run/{pipeline}', status_code=status.HTTP_202_ACCEPTED)
async def post_run(
    pipeline: str, body: TriggerBody, deps: DepsDep, _: ApiKeyDep
) -> TriggerAccepted:
    """Прямой запуск именованного пайплайна (API-ключ; pipeline-семантика)."""
    record = _to_record(body)
    try:
        await run_pipeline(deps, pipeline=pipeline, record=record, by=API_KEY_ACTOR)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    return TriggerAccepted(record_uid=record.uid, mode='staged')
