"""
Реальная обёртка над ``matrix-nio`` (граница nio, M7 B1).

Единственный nio-зависимый модуль фазы B1: парольный логин аккаунта
(``AsyncClient.login``) → ``MatrixSession`` (токен + ``device_id``).
Сетевой/интерактивный путь бритвенно-тонкий (как telethon-realclient,
W1 спеки T005): корректность проверяется юнит-тестом на подменённом
``AsyncClient`` и ручным прогоном на стенде (B4), сетевые вызовы в CI не
исполняются.

E2EE-стор (``store_path``/``AsyncClientConfig``) здесь ещё не
инициализируется — базовый ``matrix-nio`` без ``nio[e2e]`` (решение
2026-06-16); расшифровка и key-store — фаза B2. nio без ``py.typed``: под
``ignore_missing_imports`` его типы суть ``Any``.
"""

from __future__ import annotations

from nio import AsyncClient, LoginError

from angarion.adapters.matrix.session import MatrixSession
from angarion.domain.errors import ConfigError


async def password_login(
    homeserver: str, user_id: str, password: str, device_name: str
) -> str:
    """
    Парольный логин Matrix-аккаунта → строка ``MatrixSession`` (B1).

    ``AsyncClient.login`` возвращает ``LoginError`` на неуспех авторизации
    (неверный пароль и т.п.) — переводим в ``ConfigError`` с внятным
    текстом; сетевые сбои nio пробрасываются как есть (тонкий шов).
    """
    client = AsyncClient(homeserver, user_id)
    try:
        response = await client.login(password, device_name=device_name)
    finally:
        await client.close()
    if isinstance(response, LoginError):
        msg = f'Matrix login для {user_id!r} не удался: {response.message}'
        raise ConfigError(msg)
    session = MatrixSession(
        homeserver=homeserver,
        user_id=response.user_id,
        device_id=response.device_id,
        access_token=response.access_token,
    )
    return session.to_session_string()
