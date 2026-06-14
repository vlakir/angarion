"""
Фабрика FastAPI-приложения (§12.5/§12.6): второй driving-адаптер,
симметричный Telethon-listener'у. Кладёт контейнер портов в
``app.state``, монтирует встроенный JSON ``/api/v1``, SSR Web UI
``/ui`` с упакованными офлайн-ассетами, пользовательские JSON-роутеры,
UI-страницы (``pages``) и webhook-роутеры адаптеров
(``push_transport="webhook"``, §12.11).
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from fastapi import APIRouter, FastAPI
from fastapi.staticfiles import StaticFiles

from angarion.adapters.http.deps import AngarionDeps
from angarion.adapters.http.routes import router as api_v1_router
from angarion.adapters.http.templating import Page, build_nav, build_templates
from angarion.adapters.http.ui import router as ui_router

_STATIC_DIR = Path(__file__).parent / 'static'


def create_app(
    deps: AngarionDeps,
    *,
    routers: Sequence[APIRouter] = (),
    pages: Sequence[Page] = (),
    template_dirs: Sequence[Path] = (),
    title: str = 'angarion',
) -> FastAPI:
    """
    Собрать ASGI-приложение поверх портов (§12.5/§12.6).

    ``routers`` — пользовательские JSON-роутеры; ``pages`` — пользовательские
    UI-страницы (``Page``): их роутеры монтируются, а заголовок/путь
    автоматически появляются в навигации дашборда. ``template_dirs`` —
    каталоги пользовательских Jinja-шаблонов (``ChoiceLoader``: ищутся
    раньше встроенных, можно расширять ``{% extends "angarion/base.html" %}``).
    webhook-роутеры адаптеров берутся из ``deps.webhook_routers`` и
    приводятся к ``APIRouter`` (в ядре поле типизировано ``Any``, §14.9).
    """
    app = FastAPI(title=title)
    app.state.angarion_deps = deps
    app.state.started_at = datetime.now(UTC)
    app.state.templates = build_templates(
        template_dirs=template_dirs, nav=build_nav(pages)
    )
    app.include_router(api_v1_router)
    app.include_router(ui_router)
    app.mount('/ui/static', StaticFiles(directory=_STATIC_DIR), name='static')
    for user_router in routers:
        app.include_router(user_router)
    for page in pages:
        app.include_router(page.router)
    for webhook_router in deps.webhook_routers:
        app.include_router(cast('APIRouter', webhook_router))
    return app
