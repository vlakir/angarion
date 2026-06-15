"""
Аутентификация и авторизация HTTP-адаптера (§12.7, M5/B T023) на
fastapi-users (JWT для ``/api/v1``, cookie для ``/ui``) поверх одного
user store (``UserRow``).

Публичный API для пользовательских ручек — зависимости ``CurrentUser`` и
``AdminUser`` (роутеры по умолчанию закрыты ``CurrentUser``). Прочее
(backend, роутеры логина/регистрации, bootstrap) ``create_app``
собирает сам.
"""

from angarion.adapters.http.auth.deps import (
    AdminUser,
    CurrentUser,
    require_admin,
    require_user,
)
from angarion.adapters.http.auth.state import AuthState
from angarion.adapters.http.auth.users import UserCreate, UserRead, UserRole

__all__ = [
    'AdminUser',
    'AuthState',
    'CurrentUser',
    'UserCreate',
    'UserRead',
    'UserRole',
    'require_admin',
    'require_user',
]
