"""
SSR-страницы аутентификации и админки пользователей (§12.7, фаза 3).

Публичные ``/ui/login`` / ``/ui/logout`` / ``/ui/register`` (cookie-вход:
форма → JWT в HTTPOnly-cookie, тот же токен принимает ``require_user``) и
admin-страница ``/ui/users`` со списком и htmx-действиями (одобрить +
роль / деактивировать / удалить / создать) — действия зовут те же
сервисные функции, что и JSON-ручки (``admin``), и возвращают
HTML-фрагмент таблицы.

Без ``from __future__ import annotations``: DI fastapi и формы
вычисляются в runtime.
"""

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from angarion.adapters.http.auth.admin import (
    AdminUserCreate,
    UserUpdate,
    add_user,
    apply_update,
    fetch_users,
    remove_user,
)
from angarion.adapters.http.auth.deps import COOKIE_NAME, AdminUser
from angarion.adapters.http.auth.notify import notify_registration
from angarion.adapters.http.auth.users import (
    AngarionUserDatabase,
    UserRole,
    create_pending_user,
    get_user_db,
    jwt_strategy_for,
    password_helper,
)
from angarion.adapters.http.deps import get_deps

router_public = APIRouter(prefix='/ui', tags=['ui-auth'])
router_admin = APIRouter(prefix='/ui', tags=['ui-users'])

_ROLES = [role.value for role in UserRole]


def _templates(request: Request) -> Jinja2Templates:
    templates: Jinja2Templates = request.app.state.templates
    return templates


async def _users_fragment(
    request: Request, user_db: AngarionUserDatabase
) -> HTMLResponse:
    users = await fetch_users(user_db.session)
    return _templates(request).TemplateResponse(
        request,
        'angarion/fragments/users_table.html',
        {'users': users, 'roles': _ROLES},
    )


@router_public.get('/login', response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    """Форма входа (публичная)."""
    return _templates(request).TemplateResponse(request, 'angarion/login.html', {})


@router_public.post('/login')
async def login_submit(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> Response:
    """Проверить пароль, выдать JWT в HTTPOnly-cookie, увести на ``/ui``."""
    user = await user_db.get_by_email(username)
    verified = (
        user is not None
        and user.is_active
        and (password_helper.verify_and_update(password, user.hashed_password)[0])
    )
    if user is None or not verified:
        return _templates(request).TemplateResponse(
            request,
            'angarion/login.html',
            {'error': 'Invalid username/password or account not approved'},
            status_code=401,
        )
    auth = request.app.state.auth
    token = await jwt_strategy_for(auth).write_token(user)
    response = RedirectResponse('/ui', status_code=303)
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=auth.jwt_lifetime,
        httponly=True,
        secure=auth.cookie_secure,
        samesite='lax',
    )
    return response


@router_public.get('/logout')
async def logout() -> Response:
    """Сбросить cookie и увести на форму входа."""
    response = RedirectResponse('/ui/login', status_code=303)
    response.delete_cookie(COOKIE_NAME)
    return response


@router_public.get('/register', response_class=HTMLResponse)
async def register_page(request: Request) -> HTMLResponse:
    """Форма саморегистрации (публичная)."""
    return _templates(request).TemplateResponse(request, 'angarion/register.html', {})


@router_public.post('/register', response_class=HTMLResponse)
async def register_submit(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Создать заявку; страница сообщает «ждите одобрения админа»."""
    try:
        await create_pending_user(user_db, request.app.state.auth, username, password)
    except HTTPException as exc:
        return _templates(request).TemplateResponse(
            request,
            'angarion/register.html',
            {'error': str(exc.detail)},
            status_code=exc.status_code,
        )
    await notify_registration(get_deps(request), login=username)
    return _templates(request).TemplateResponse(
        request, 'angarion/register.html', {'submitted': True}
    )


@router_admin.get('/users', response_class=HTMLResponse)
async def users_page(
    request: Request,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Таблица пользователей и форма создания (admin)."""
    users = await fetch_users(user_db.session)
    return _templates(request).TemplateResponse(
        request, 'angarion/users.html', {'users': users, 'roles': _ROLES}
    )


@router_admin.post('/users/{user_id}/approve', response_class=HTMLResponse)
async def users_approve(
    request: Request,
    user_id: uuid.UUID,
    role: Annotated[str, Form()],
    admin: AdminUser,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Одобрить заявку с назначением роли (htmx, admin)."""
    await apply_update(
        user_db, user_id, UserUpdate(is_active=True, role=UserRole(role)), admin.login
    )
    return await _users_fragment(request, user_db)


@router_admin.post('/users/{user_id}/deactivate', response_class=HTMLResponse)
async def users_deactivate(
    request: Request,
    user_id: uuid.UUID,
    admin: AdminUser,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Деактивировать пользователя (htmx, admin)."""
    await apply_update(user_db, user_id, UserUpdate(is_active=False), admin.login)
    return await _users_fragment(request, user_db)


@router_admin.post('/users/{user_id}/delete', response_class=HTMLResponse)
async def users_delete(
    request: Request,
    user_id: uuid.UUID,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Удалить пользователя (htmx, admin — защита на уровне роутера)."""
    await remove_user(user_db, user_id)
    return await _users_fragment(request, user_db)


@router_admin.post('/users/create', response_class=HTMLResponse)
async def users_create(
    request: Request,
    login: Annotated[str, Form()],
    password: Annotated[str, Form()],
    role: Annotated[str, Form()],
    admin: AdminUser,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> HTMLResponse:
    """Создать пользователя вручную (htmx, admin)."""
    await add_user(
        user_db,
        AdminUserCreate(login=login, password=password, role=UserRole(role)),
        admin.login,
    )
    return await _users_fragment(request, user_db)
