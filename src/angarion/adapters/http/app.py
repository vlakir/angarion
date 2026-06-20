"""
Фабрика FastAPI-приложения (§12.5/§12.6/§12.7): второй driving-адаптер,
симметричный Telethon-listener'у. Кладёт контейнер портов и конфиг
аутентификации в ``app.state``, монтирует встроенный JSON ``/api/v1``,
SSR Web UI ``/ui`` с упакованными офлайн-ассетами, роутеры логина/
регистрации, пользовательские JSON-роутеры, UI-страницы (``pages``) и
webhook-роутеры адаптеров (``push_transport="webhook"``, §12.11).

Авторизация навешивается **на уровне роутеров** (§12.7): встроенные
diagnostics/events и UI закрыты ``CurrentUser``; публичны ``GET /health``,
``/api/v1/auth/*`` и статика. Пользовательские роутеры по умолчанию
закрыты, открытость — осознанный ``Page(public=True)``.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from fastapi import APIRouter, Depends, FastAPI
from fastapi.staticfiles import StaticFiles

from angarion.adapters.http.auth.admin import router as users_api_router
from angarion.adapters.http.auth.bootstrap import validate_auth_secret
from angarion.adapters.http.auth.deps import require_admin, require_user
from angarion.adapters.http.auth.pages import router_admin, router_public
from angarion.adapters.http.auth.state import AuthState
from angarion.adapters.http.auth.users import login_router, register_router
from angarion.adapters.http.deps import AngarionDeps
from angarion.adapters.http.ops import router as admin_api_router
from angarion.adapters.http.ops_pages import router_ops
from angarion.adapters.http.routes import public_router
from angarion.adapters.http.routes import router as api_v1_router
from angarion.adapters.http.templating import Page, build_nav, build_templates
from angarion.adapters.http.trigger import router as trigger_router
from angarion.adapters.http.ui import router as ui_router
from angarion.domain.errors import ConfigError
from angarion.log import get_logger

_STATIC_DIR = Path(__file__).parent / 'static'
_LOCAL_HOSTS: Final = frozenset({'127.0.0.1', 'localhost', '::1'})
_log = get_logger('angarion.http')


def create_app(
    deps: AngarionDeps,
    *,
    routers: Sequence[APIRouter] = (),
    pages: Sequence[Page] = (),
    template_dirs: Sequence[Path] = (),
    title: str = 'angarion',
) -> FastAPI:
    """
    Собрать ASGI-приложение поверх портов (§12.5/§12.6/§12.7).

    ``routers`` — пользовательские JSON-роутеры (закрыты ``CurrentUser``);
    ``pages`` — UI-страницы ``Page`` (роутер монтируется, заголовок/путь
    появляются в навигации; ``public=True`` открывает страницу).
    ``template_dirs`` — каталоги пользовательских Jinja-шаблонов
    (``ChoiceLoader``). Каталоги, объявленные самими страницами в
    ``Page.template_dirs`` (T036), подмешиваются после явного аргумента —
    так entry-point-страница чистого CLI наследует ``base.html`` без
    лаунчера. webhook-роутеры адаптеров берутся из
    ``deps.webhook_routers`` и монтируются публично (платформа
    аутентифицирует их своим механизмом).

    fail-fast (§12.7, FR-0): ``auth="users"`` требует ``api.secret`` и
    user-store ``auth_sessionmaker``. При ``auth="none"`` и bind не на
    localhost — громкое предупреждение в лог.
    """
    api = deps.settings.api
    validate_auth_secret(deps.settings)
    if api.auth == 'users' and deps.auth_sessionmaker is None:
        msg = 'api.auth="users" требует user store (deps.auth_sessionmaker)'
        raise ConfigError(msg)
    if api.auth == 'none' and api.host not in _LOCAL_HOSTS:
        _log.warning(
            'auth_none_non_local_bind',
            host=api.host,
            detail='аутентификация выключена при bind не на localhost (§12.7)',
        )

    app = FastAPI(title=title)
    app.state.angarion_deps = deps
    app.state.started_at = datetime.now(UTC)
    page_template_dirs = [d for page in pages for d in page.template_dirs]
    app.state.templates = build_templates(
        template_dirs=[*template_dirs, *page_template_dirs], nav=build_nav(pages)
    )
    app.state.templates.env.globals['auth_enabled'] = api.auth == 'users'
    app.state.auth = AuthState(
        mode=api.auth,
        secret=api.secret,
        jwt_lifetime=api.jwt_lifetime,
        cookie_secure=api.cookie_secure,
        registration_enabled=api.registration_enabled,
        max_pending_registrations=api.max_pending_registrations,
        sessionmaker=deps.auth_sessionmaker,
    )

    protected = [Depends(require_user)]
    admin_only = [Depends(require_admin)]
    app.include_router(public_router)
    if api.auth == 'users':
        app.include_router(login_router, prefix='/api/v1/auth', tags=['auth'])
        app.include_router(register_router, prefix='/api/v1/auth', tags=['auth'])
        app.include_router(users_api_router, dependencies=admin_only)
        app.include_router(router_public)
        app.include_router(router_admin, dependencies=admin_only)
    app.include_router(api_v1_router, dependencies=protected)
    app.include_router(admin_api_router, dependencies=admin_only)
    # ручной триггер (T038): авторизация пер-роут через API-ключ
    # (``api.trigger_token``), не сессия — потому без admin_only/protected
    app.include_router(trigger_router)
    app.include_router(router_ops, dependencies=admin_only)
    app.include_router(ui_router, dependencies=protected)
    app.mount('/ui/static', StaticFiles(directory=_STATIC_DIR), name='static')
    for user_router in routers:
        app.include_router(user_router, dependencies=protected)
    for page in pages:
        app.include_router(page.router, dependencies=[] if page.public else protected)
    for webhook_router in deps.webhook_routers:
        app.include_router(cast('APIRouter', webhook_router))
    return app
