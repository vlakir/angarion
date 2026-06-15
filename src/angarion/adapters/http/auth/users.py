"""
Ядро аутентификации на fastapi-users (§12.7, M5/B T023, фаза 2).

fastapi-users даёт user store (SQLAlchemy), хэш паролей (argon2 через
pwdlib), JWT-стратегию и логин-роутер; наша модель отличается от
дефолтной (``login`` вместо email, роли ``admin``/``viewer``,
саморегистрация с одобрением), поэтому:

* ``AngarionUserDatabase`` переопределяет ``get_by_email`` на запрос по
  ``login`` — fastapi-users трактует ``login`` как идентификатор входа;
* регистрация — **свой** эндпоинт (заявка ``is_active=False``,
  ``role="viewer"``), а не дефолтный register-роутер, который завязан на
  email-схему и ``create_update_dict``.

Всё, специфичное для приложения (секрет JWT, срок, sessionmaker, режим),
DI-провайдеры читают из ``app.state.auth`` в момент запроса — поэтому
модульные синглтоны (backend, ``FastAPIUsers``) работают для любого
приложения, собранного ``create_app``.

Без ``from __future__ import annotations``: pydantic-схемы и обобщения
fastapi-users вычисляются в runtime.
"""

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin
from fastapi_users.authentication import (
    AuthenticationBackend,
    BearerTransport,
    JWTStrategy,
)
from fastapi_users.password import PasswordHelper
from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase
from pydantic import AwareDatetime, BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from angarion.adapters.http.auth.state import AuthState
from angarion.adapters.storage.orm import UserRow


class UserRole(StrEnum):
    """Роли v1 (§12.7): ``admin`` (write) и ``viewer`` (диагностика)."""

    ADMIN = 'admin'
    VIEWER = 'viewer'


class UserRead(BaseModel):
    """Проекция ``UserRow`` для API/UI (без ``hashed_password``)."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    login: str
    role: str
    is_active: bool
    registered_at: AwareDatetime
    approved_at: AwareDatetime | None = None
    approved_by: str | None = None


class UserCreate(BaseModel):
    """Тело заявки на регистрацию: логин + пароль (§12.7)."""

    login: str
    password: str


class AngarionUserDatabase(SQLAlchemyUserDatabase[UserRow, uuid.UUID]):
    """user store fastapi-users поверх ``UserRow``; вход — по ``login``."""

    async def get_by_email(self, email: str) -> UserRow | None:
        """fastapi-users ищет по «email» — у нас это ``login`` (§12.7)."""
        statement = select(self.user_table).where(UserRow.login == email)
        return await self._get_user(statement)


class UserManager(UUIDIDMixin, BaseUserManager[UserRow, uuid.UUID]):
    """
    Менеджер fastapi-users: ``authenticate`` (логин) + парс UUID-id.

    ``create``/reset/verify не используются (регистрация — свой эндпоинт,
    сброс/верификация вне v1), поэтому token-секреты не объявляются.
    """


password_helper = PasswordHelper()
"""Хэшер паролей (pwdlib argon2+bcrypt) — общий для логина и регистрации."""


async def get_async_session(request: Request) -> AsyncIterator[AsyncSession]:
    """Сессия user store из ``app.state.auth.sessionmaker`` (на запрос)."""
    async with request.app.state.auth.sessionmaker() as session:
        yield session


async def get_user_db(
    session: Annotated[AsyncSession, Depends(get_async_session)],
) -> AsyncIterator[AngarionUserDatabase]:
    """DI-провайдер user store fastapi-users."""
    yield AngarionUserDatabase(session, UserRow)


async def get_user_manager(
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> AsyncIterator[UserManager]:
    """DI-провайдер менеджера fastapi-users."""
    yield UserManager(user_db, password_helper)


def jwt_strategy_for(auth: AuthState) -> JWTStrategy[UserRow, uuid.UUID]:
    """JWT-стратегия (encode/decode токена) с секретом/сроком из ``AuthState``."""
    return JWTStrategy(secret=auth.secret, lifetime_seconds=auth.jwt_lifetime)


def get_jwt_strategy(request: Request) -> JWTStrategy[UserRow, uuid.UUID]:
    """JWT-стратегия из ``app.state.auth`` (на запрос; для backend логина)."""
    return jwt_strategy_for(request.app.state.auth)


bearer_transport = BearerTransport(tokenUrl='api/v1/auth/login')
jwt_backend = AuthenticationBackend(
    name='jwt', transport=bearer_transport, get_strategy=get_jwt_strategy
)
fastapi_users = FastAPIUsers[UserRow, uuid.UUID](get_user_manager, [jwt_backend])
login_router = fastapi_users.get_auth_router(jwt_backend)
"""Встроенный логин/логаут fastapi-users (JWT Bearer) для ``/api/v1/auth``."""


async def create_pending_user(
    user_db: AngarionUserDatabase, auth: AuthState, login: str, password: str
) -> UserRow:
    """
    Создать заявку на регистрацию (§12.7): ``is_active=False``,
    ``role="viewer"`` — вход только после одобрения админом.

    Защита от замусоривания: ``registration_enabled=false`` отключает
    регистрацию (403); при достижении ``max_pending_registrations`` —
    временно закрыто (429); занятый логин — 400. Общая логика JSON-ручки
    и формы ``/ui/register``.
    """
    if not auth.registration_enabled:
        raise HTTPException(status.HTTP_403_FORBIDDEN, 'registration disabled')
    pending = await user_db.session.scalar(
        select(func.count())
        .select_from(UserRow)
        .where(UserRow.__table__.c.is_active.is_(False))
    )
    if pending is not None and pending >= auth.max_pending_registrations:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS, 'too many pending registrations'
        )
    if await user_db.get_by_email(login) is not None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, 'login already exists')
    return await user_db.create(
        {
            'login': login,
            'hashed_password': password_helper.hash(password),
            'role': UserRole.VIEWER.value,
            'is_active': False,
            'registered_at': datetime.now(UTC),
        }
    )


register_router = APIRouter()


@register_router.post(
    '/register', response_model=UserRead, status_code=status.HTTP_201_CREATED
)
async def register(
    payload: UserCreate,
    request: Request,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> UserRow:
    """Саморегистрация-заявка (§12.7) — JSON-обёртка ``create_pending_user``."""
    return await create_pending_user(
        user_db, request.app.state.auth, payload.login, payload.password
    )
