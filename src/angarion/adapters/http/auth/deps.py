"""
Авторизация на уровне роутеров (§12.7): зависимости ``CurrentUser`` и
``AdminUser`` — публичный API для пользовательских ручек.

``require_user`` собрана вручную (а не через ``FastAPIUsers.current_user``),
потому что навешивается на **модульные** роутеры (встроенный ``/api/v1``,
``/ui``, пользовательские) через ``include_router(dependencies=...)``, а
конфиг аутентификации (секрет, sessionmaker, режим) живёт пер-приложение
в ``app.state.auth``. Зависимость читает его в момент запроса и
переиспользует JWT-стратегию + менеджер fastapi-users для разбора токена
(Bearer для ``/api/v1``, cookie для ``/ui``).

Режим ``auth="none"`` (§12.7) — резолвится синтетический локальный админ,
так что admin-страницы/операции доступны для dev/локали без логина.

Без ``from __future__ import annotations``: pydantic ``AuthState`` и
обобщения fastapi-users вычисляются в runtime.
"""

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from angarion.adapters.http.auth.users import (
    AngarionUserDatabase,
    UserManager,
    UserRole,
    jwt_strategy_for,
    password_helper,
)
from angarion.adapters.storage.orm import UserRow

COOKIE_NAME = 'angarionauth'
"""Имя HTTPOnly-cookie с JWT для ``/ui`` (выставляется на ``/ui/login``)."""

_SYNTHETIC_ADMIN_ID = uuid.UUID(int=0)
_UNAUTHENTICATED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail='Not authenticated',
    headers={'WWW-Authenticate': 'Bearer'},
)


def _synthetic_admin() -> UserRow:
    """Локальный админ режима ``auth="none"`` (не персистится)."""
    return UserRow(
        id=_SYNTHETIC_ADMIN_ID,
        login='__local__',
        hashed_password='',
        role=UserRole.ADMIN.value,
        is_active=True,
        registered_at=datetime.now(UTC),
    )


def _extract_token(request: Request) -> str | None:
    """JWT из заголовка ``Authorization: Bearer`` или cookie (UI)."""
    header = request.headers.get('Authorization', '')
    if header.lower().startswith('bearer '):
        return header[len('Bearer ') :]
    return request.cookies.get(COOKIE_NAME)


async def require_user(request: Request) -> UserRow:
    """
    Активный пользователь по токену; ``auth="none"`` → локальный админ.

    Кладёт пользователя в ``request.state.user`` — шаблоны рисуют по нему
    навигацию (пункт ``/ui/users`` и logout для админа).
    """
    auth = request.app.state.auth
    if auth.mode == 'none':
        admin = _synthetic_admin()
        request.state.user = admin
        return admin
    token = _extract_token(request)
    user: UserRow | None = None
    if token is not None:
        async with auth.sessionmaker() as session:
            user_db = AngarionUserDatabase(session, UserRow)
            manager = UserManager(user_db, password_helper)
            user = await jwt_strategy_for(auth).read_token(token, manager)
    if user is None or not user.is_active:
        raise _UNAUTHENTICATED
    request.state.user = user
    return user


CurrentUser = Annotated[UserRow, Depends(require_user)]


async def require_admin(user: CurrentUser) -> UserRow:
    """Пользователь с ролью ``admin`` — иначе 403 (§12.7, write-доступ)."""
    if user.role != UserRole.ADMIN.value:
        raise HTTPException(status.HTTP_403_FORBIDDEN, 'Admin only')
    return user


AdminUser = Annotated[UserRow, Depends(require_admin)]
