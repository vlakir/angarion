"""
Управление пользователями (§12.7, M5/B T023, фаза 3): сервисные функции
(одобрение/роль/деактивация/удаление/ручное создание) и **JSON-ручки**
``/api/v1/users`` — единственное write-исключение из read-only встроенных
ручек, доступны только ``admin``.

Сервисные функции переиспользует htmx-страница ``/ui/users``
(``angarion.adapters.http.auth.ui``). Без ``from __future__ import
annotations``: pydantic-схемы и DI fastapi вычисляются в runtime.
"""

import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from angarion.adapters.http.auth.deps import AdminUser
from angarion.adapters.http.auth.users import (
    AngarionUserDatabase,
    UserRead,
    UserRole,
    get_async_session,
    get_user_db,
    password_helper,
)
from angarion.adapters.storage.orm import UserRow


class UserUpdate(BaseModel):
    """Изменение пользователя админом: одобрение/деактивация и/или роль."""

    is_active: bool | None = None
    role: UserRole | None = None


class AdminUserCreate(BaseModel):
    """Ручное создание пользователя админом (по умолчанию активный viewer)."""

    login: str
    password: str
    role: UserRole = UserRole.VIEWER
    is_active: bool = True


_NOT_FOUND = HTTPException(status.HTTP_404_NOT_FOUND, 'user not found')


async def fetch_users(session: AsyncSession) -> list[UserRow]:
    """Все пользователи, старые заявки первыми (для списка/таблицы)."""
    result = await session.scalars(select(UserRow).order_by(UserRow.registered_at))
    return list(result)


async def apply_update(
    user_db: AngarionUserDatabase,
    user_id: uuid.UUID,
    patch: UserUpdate,
    by: str,
) -> UserRow:
    """Одобрить/деактивировать и/или сменить роль; аудит одобрения."""
    user = await user_db.get(user_id)
    if user is None:
        raise _NOT_FOUND
    updates: dict[str, object] = {}
    if patch.role is not None:
        updates['role'] = patch.role.value
    if patch.is_active is not None:
        updates['is_active'] = patch.is_active
        if patch.is_active and user.approved_at is None:
            updates['approved_at'] = datetime.now(UTC)
            updates['approved_by'] = by
    return await user_db.update(user, updates)


async def remove_user(user_db: AngarionUserDatabase, user_id: uuid.UUID) -> None:
    """Удалить пользователя; отсутствующий — 404."""
    user = await user_db.get(user_id)
    if user is None:
        raise _NOT_FOUND
    await user_db.delete(user)


async def add_user(
    user_db: AngarionUserDatabase, payload: AdminUserCreate, by: str
) -> UserRow:
    """Создать пользователя вручную (занятый логин — 400)."""
    if await user_db.get_by_email(payload.login) is not None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, 'login already exists')
    now = datetime.now(UTC)
    return await user_db.create(
        {
            'login': payload.login,
            'hashed_password': password_helper.hash(payload.password),
            'role': payload.role.value,
            'is_active': payload.is_active,
            'registered_at': now,
            'approved_at': now if payload.is_active else None,
            'approved_by': by if payload.is_active else None,
        }
    )


router = APIRouter(prefix='/api/v1/users', tags=['users'])


@router.get('', response_model=list[UserRead])
async def list_users(
    session: Annotated[AsyncSession, Depends(get_async_session)],
) -> list[UserRow]:
    """Список пользователей (admin)."""
    return await fetch_users(session)


@router.post('', response_model=UserRead, status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: AdminUserCreate,
    admin: AdminUser,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> UserRow:
    """Создать пользователя вручную (admin)."""
    return await add_user(user_db, payload, admin.login)


@router.patch('/{user_id}', response_model=UserRead)
async def patch_user(
    user_id: uuid.UUID,
    payload: UserUpdate,
    admin: AdminUser,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> UserRow:
    """Одобрить/деактивировать/сменить роль (admin)."""
    return await apply_update(user_db, user_id, payload, admin.login)


@router.delete('/{user_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: uuid.UUID,
    user_db: Annotated[AngarionUserDatabase, Depends(get_user_db)],
) -> None:
    """Удалить пользователя (admin)."""
    await remove_user(user_db, user_id)
