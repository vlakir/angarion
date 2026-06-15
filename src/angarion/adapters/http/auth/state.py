"""
Конфиг аутентификации приложения ``AuthState`` (§12.7) — в отдельном
модуле, чтобы и ``deps`` (зависимости-стражи), и ``users`` (регистрация
с лимитом) типизировали его без циклического импорта.

Без ``from __future__ import annotations``: pydantic-поля вычисляются в
runtime.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict


class AuthState(BaseModel):
    """
    Конфиг аутентификации в ``app.state.auth`` (собран ``create_app`` из
    ``settings.api`` + sessionmaker user store).

    ``sessionmaker`` — ``async_sessionmaker`` user store (``None`` при
    ``auth="none"``). Композиция (не DTO): ``arbitrary_types_allowed``;
    ``sessionmaker`` оставлен ``Any`` (параметризованный generic как поле
    pydantic — источник проблем; здесь это просто хранимый колбэк).
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    mode: str
    secret: str
    jwt_lifetime: int
    cookie_secure: bool
    registration_enabled: bool
    max_pending_registrations: int
    sessionmaker: Any = None
