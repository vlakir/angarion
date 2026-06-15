"""
Bootstrap-валидация аутентификации (§12.7, FR-0): fail-fast по секрету
и создание первого администратора из env на пустой таблице.

``validate_auth_secret`` зовётся в ``create_app`` (sync, без БД).
``ensure_admin`` — из composition root / lifespan (async, нужна сессия
user store): на пустой таблице создаёт админа из
``ANGARION_ADMIN_LOGIN``/``ANGARION_ADMIN_PASSWORD``; при ``auth="users"``
и отсутствии env — fail-fast (без SSH-ритуалов в эксплуатации, §12.7).

Без ``from __future__ import annotations``: единый стиль http-слоя.
"""

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from angarion.adapters.http.auth.users import (
    AngarionUserDatabase,
    UserRole,
    password_helper,
)
from angarion.adapters.storage.orm import UserRow
from angarion.config import AngarionSettings
from angarion.domain.errors import ConfigError


def validate_auth_secret(settings: AngarionSettings) -> None:
    """``auth="users"`` обязывает ``ANGARION_API__SECRET`` — иначе fail-fast."""
    if settings.api.auth == 'users' and not settings.api.secret:
        msg = (
            'api.auth="users" требует секрет JWT в env ANGARION_API__SECRET '
            '(§12.7); задайте секрет или используйте auth="none" локально'
        )
        raise ConfigError(msg)


async def ensure_admin(
    sessionmaker: async_sessionmaker[AsyncSession], settings: AngarionSettings
) -> None:
    """
    Создать первого админа на пустой таблице (§12.7, FR-0).

    Непустая таблица — no-op. Пустая: при заданных
    ``admin_login``/``admin_password`` создаёт активного админа; иначе
    при ``auth="users"`` — fail-fast, при ``auth="none"`` — no-op
    (синтетический локальный админ резолвится без записи).
    """
    async with sessionmaker() as session:
        count = await session.scalar(select(func.count()).select_from(UserRow))
        if count:
            return
        login, pw = settings.admin_login, settings.admin_password
        if not (login and pw):
            if settings.api.auth == 'users':
                msg = (
                    'пустая таблица пользователей и не заданы '
                    'ANGARION_ADMIN_LOGIN/ANGARION_ADMIN_PASSWORD — некем войти '
                    '(§12.7); задайте env bootstrap-админа'
                )
                raise ConfigError(msg)
            return
        now = datetime.now(UTC)
        await AngarionUserDatabase(session, UserRow).create(
            {
                'login': login,
                'hashed_password': password_helper.hash(pw),
                'role': UserRole.ADMIN.value,
                'is_active': True,
                'registered_at': now,
                'approved_at': now,
            }
        )
