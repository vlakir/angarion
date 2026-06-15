"""
Встроенный SSR-роутер Web UI (§12.6): дашборд ``/ui``, журнал
``/ui/events`` и htmx-фрагменты ``/ui/fragments/*`` для поллинга.

Данные берутся из тех же портов, что отдаёт JSON ``/api/v1`` (один
источник — два представления); прямого доступа к ORM в шаблонном слое
нет (§12.6). Шаблоны и навигация резолвятся через ``app.state.templates``
(собран ``create_app`` с учётом пользовательских ``pages``).
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Final, cast

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from angarion.adapters.http._data import source_keys
from angarion.adapters.http.deps import AnalyticsDep, CursorsDep, QueueDep, get_deps
from angarion.adapters.http.viz import build_pipeline_graph

router = APIRouter(prefix='/ui', tags=['ui'])

_WINDOW: Final = timedelta(hours=24)


def get_templates(request: Request) -> Jinja2Templates:
    """Достать ``Jinja2Templates`` из ``app.state`` (собран ``create_app``)."""
    return cast('Jinja2Templates', request.app.state.templates)


TemplatesDep = Annotated[Jinja2Templates, Depends(get_templates)]


async def _cursor_rows(request: Request, cursors: CursorsDep) -> list[dict[str, Any]]:
    """Состояние курсоров по источникам конфига (для дашборда/фрагмента)."""
    deps = get_deps(request)
    rows: list[dict[str, Any]] = []
    for key in source_keys(deps.settings):
        cursor = await cursors.load(key)
        rows.append(
            {
                'source_key': key,
                'updated_at': cursor.updated_at if cursor is not None else None,
            }
        )
    return rows


@router.get('', response_class=HTMLResponse)
async def dashboard(
    request: Request,
    templates: TemplatesDep,
    queue: QueueDep,
    analytics: AnalyticsDep,
    cursors: CursorsDep,
) -> HTMLResponse:
    """Дашборд: очередь, события за 24 ч, курсоры, пайплайны (§12.6)."""
    deps = get_deps(request)
    since = datetime.now(UTC) - _WINDOW
    context = {
        'depth': await queue.depth(),
        'counts': await analytics.counts_by_kind(since=since),
        'cursors': await _cursor_rows(request, cursors),
        'pipelines': sorted(deps.settings.pipelines),
    }
    return templates.TemplateResponse(request, 'angarion/dashboard.html', context)


async def render_pipelines_fragment(
    request: Request, templates: Jinja2Templates
) -> HTMLResponse:
    """
    Отрендерить фрагмент графа топологии (``#pipeline-graph``).

    Переиспользуется htmx-поллингом ``/ui/fragments/pipelines`` и
    админским pause/resume по узлу (``ops_pages``) — клик подменяет тот
    же корень обновлённым графом.
    """
    graph = await build_pipeline_graph(get_deps(request))
    return templates.TemplateResponse(
        request, 'angarion/fragments/pipelines.html', {'graph': graph}
    )


@router.get('/pipelines', response_class=HTMLResponse)
async def pipelines_page(request: Request, templates: TemplatesDep) -> HTMLResponse:
    """Трёхдольный граф «источники → пайплайны → получатели» (§12.6)."""
    graph = await build_pipeline_graph(get_deps(request))
    return templates.TemplateResponse(
        request, 'angarion/pipelines.html', {'graph': graph}
    )


@router.get('/fragments/pipelines', response_class=HTMLResponse)
async def fragment_pipelines(request: Request, templates: TemplatesDep) -> HTMLResponse:
    """htmx-партиал графа топологии (поллинг и подмена после pause/resume)."""
    return await render_pipelines_fragment(request, templates)


@router.get('/events', response_class=HTMLResponse)
async def events_page(
    request: Request,
    templates: TemplatesDep,
    analytics: AnalyticsDep,
    kind: Annotated[str | None, Query()] = None,
    pipeline: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> HTMLResponse:
    """Журнал аналитики с фильтрами ``kind``/``pipeline`` (htmx-формы)."""
    events = await analytics.recent(kind=kind, pipeline=pipeline, limit=limit)
    context = {'events': events, 'kind': kind or '', 'pipeline': pipeline or ''}
    return templates.TemplateResponse(request, 'angarion/events.html', context)


@router.get('/fragments/queue', response_class=HTMLResponse)
async def fragment_queue(
    request: Request, templates: TemplatesDep, queue: QueueDep
) -> HTMLResponse:
    """htmx-партиал глубины очереди."""
    context = {'depth': await queue.depth()}
    return templates.TemplateResponse(request, 'angarion/fragments/queue.html', context)


@router.get('/fragments/event-counts', response_class=HTMLResponse)
async def fragment_event_counts(
    request: Request, templates: TemplatesDep, analytics: AnalyticsDep
) -> HTMLResponse:
    """htmx-партиал счётчиков событий за 24 ч по видам."""
    since = datetime.now(UTC) - _WINDOW
    context = {'counts': await analytics.counts_by_kind(since=since)}
    return templates.TemplateResponse(
        request, 'angarion/fragments/event_counts.html', context
    )


@router.get('/fragments/cursors', response_class=HTMLResponse)
async def fragment_cursors(
    request: Request, templates: TemplatesDep, cursors: CursorsDep
) -> HTMLResponse:
    """htmx-партиал состояния курсоров."""
    context = {'cursors': await _cursor_rows(request, cursors)}
    return templates.TemplateResponse(
        request, 'angarion/fragments/cursors.html', context
    )


@router.get('/fragments/events-table', response_class=HTMLResponse)
async def fragment_events_table(
    request: Request,
    templates: TemplatesDep,
    analytics: AnalyticsDep,
    kind: Annotated[str | None, Query()] = None,
    pipeline: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> HTMLResponse:
    """htmx-партиал таблицы событий (тело журнала при фильтрации)."""
    events = await analytics.recent(kind=kind, pipeline=pipeline, limit=limit)
    return templates.TemplateResponse(
        request, 'angarion/fragments/events_table.html', {'events': events}
    )
