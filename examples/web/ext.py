"""
Кастомное расширение Web UI/API примера ``web``: одна JSON-ручка и одна
UI-страница поверх портов angarion (§12.5/§12.6).

Расширение дотягивается до системы только через порты — здесь через
``AnalyticsDep`` (read-сторона аналитики). Ни ORM, ни сессий, ни Telethon
расширение не видит: ``create_app`` подставит нужный порт из ``app.state``
по типизированной зависимости.

JSON-роутер (``ext_router``) монтируется под ``CurrentUser`` как
встроенный ``/api/v1``; страница (``EXT_PAGE``) добавляет пункт навигации
и рендерится серверным Jinja-шаблоном, наследующим ``angarion/base.html``.
Шаблоны страница несёт сама в ``Page.template_dirs`` (T036) — ``create_app``
подмешивает каталог в общий ``ChoiceLoader``, поэтому лаунчеру не нужно
передавать ``template_dirs`` отдельно (а entry-point-страница чистого CLI
наследует ``base.html`` без лаунчера вовсе). Оба передаются в
``create_app(routers=[...], pages=[...])`` — см. ``run.py``.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from angarion.adapters.http import Page
from angarion.adapters.http.deps import AnalyticsDep

_TEMPLATES = Path(__file__).parent / 'templates'

ext_router = APIRouter(prefix='/api/v1/ext', tags=['ext'])


@ext_router.get('/stats')
async def stats(analytics: AnalyticsDep) -> dict[str, int]:
    """Количество событий аналитики по видам за последние сутки."""
    since = datetime.now(UTC) - timedelta(days=1)
    return await analytics.counts_by_kind(since=since)


_ui_router = APIRouter()


@_ui_router.get('/ui/ext', response_class=HTMLResponse)
async def ext_page(request: Request, analytics: AnalyticsDep) -> HTMLResponse:
    """Страница «Activity»: последние события аналитики + сводка за сутки."""
    recent = await analytics.recent(limit=20)
    since = datetime.now(UTC) - timedelta(days=1)
    counts = await analytics.counts_by_kind(since=since)
    templates = request.app.state.templates
    context = {'recent': recent, 'counts': counts}
    return templates.TemplateResponse(request, 'ext/activity.html', context)


EXT_PAGE = Page(
    title='Activity',
    path='/ui/ext',
    router=_ui_router,
    template_dirs=(_TEMPLATES,),
)
"""Дескриптор страницы: пункт навигации + собственный каталог шаблонов (T036)."""
