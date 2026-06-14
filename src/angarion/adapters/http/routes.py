"""
Встроенный read-only роутер ``/api/v1`` (§12.5): health / diagnostics /
events. Образец для пользовательских ручек — дотягивается до системы
теми же опубликованными DI-зависимостями поверх портов
(``angarion.adapters.http.deps``), не зная про ORM/Telethon.
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Final, cast

from fastapi import APIRouter, Query, Request

from angarion import __version__
from angarion.adapters.http._data import source_keys
from angarion.adapters.http.deps import (
    AnalyticsDep,
    CursorsDep,
    QueueDep,
    get_deps,
)
from angarion.adapters.http.schemas import (
    CursorStateSchema,
    DiagnosticsResponse,
    EventSchema,
    EventsResponse,
    HealthResponse,
    QueueDepthSchema,
)

router = APIRouter(prefix='/api/v1', tags=['angarion'])

_DIAGNOSTICS_WINDOW: Final = timedelta(hours=24)


@router.get('/health')
async def health() -> HealthResponse:
    """Liveness без обращения к портам (§12.5)."""
    return HealthResponse(status='ok', version=__version__)


@router.get('/diagnostics')
async def diagnostics(
    request: Request,
    queue: QueueDep,
    analytics: AnalyticsDep,
    cursors: CursorsDep,
) -> DiagnosticsResponse:
    """Состояние системы: очередь, события за 24 ч, курсоры, пайплайны, uptime."""
    deps = get_deps(request)
    depth = await queue.depth()
    since = datetime.now(UTC) - _DIAGNOSTICS_WINDOW
    counts = await analytics.counts_by_kind(since=since)
    cursor_states: list[CursorStateSchema] = []
    for source_key in source_keys(deps.settings):
        cursor = await cursors.load(source_key)
        cursor_states.append(
            CursorStateSchema(
                source_key=source_key,
                updated_at=cursor.updated_at if cursor is not None else None,
            )
        )
    started_at = cast('datetime', request.app.state.started_at)
    uptime = (datetime.now(UTC) - started_at).total_seconds()
    return DiagnosticsResponse(
        queue=QueueDepthSchema(pending=depth.pending, unacked=depth.unacked),
        events_24h=counts,
        cursors=cursor_states,
        pipelines=sorted(deps.settings.pipelines),
        uptime_seconds=uptime,
    )


@router.get('/events')
async def events(
    analytics: AnalyticsDep,
    kind: Annotated[str | None, Query()] = None,
    pipeline: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> EventsResponse:
    """Последние события аналитики; фильтры ``kind``/``pipeline`` по равенству."""
    recent = await analytics.recent(kind=kind, pipeline=pipeline, limit=limit)
    return EventsResponse(
        events=[EventSchema.model_validate(ev.model_dump()) for ev in recent]
    )
