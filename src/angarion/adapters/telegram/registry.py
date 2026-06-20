"""
``ClientRegistry`` — общий пул Telethon-клиентов по аккаунтам (§12.1,
M3, фаза 5).

Один клиент = одна сессия = один процесс: listener и sender делят **один
и тот же** объект клиента на аккаунт (иначе MTProto-состояние раздваивается
и сессию могут отозвать). Реестр конструируется синхронно (клиенты ещё не
подключены), подключение — async в ``connect_all`` (зовётся из
``TelegramListener.start``), отключение — ``disconnect_all`` (из ``stop``).

Сетевой connect инъецируется (``connect``): дефолт — реальный Telethon из
``realclient.connect_client`` (единственный telethon-зависимый путь, W1),
тесты подставляют fake без сети. Нет сессии для аккаунта → ``ConfigError``
с подсказкой ``angarion login``.

Источник строки сессии (T042, ADR 2026-06-20): сначала ``env_sessions``
(``StringSession`` из конфига/env, dev/CI-путь, приоритетнее), затем
``session_store`` (расшифрованная из ``app.db``, prod-дефолт).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from angarion.adapters.telegram.client import TelegramClientPort
from angarion.adapters.telegram.realclient import connect_client
from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from angarion.domain.ports import SessionStorePort


@runtime_checkable
class ConnectedClient(TelegramClientPort, Protocol):
    """``TelegramClientPort`` с управляемым жизненным циклом соединения."""

    async def disconnect(self) -> None:
        """Закрыть соединение клиента (вызывается реестром на остановке)."""


ConnectFn = Callable[[int, str, str], Awaitable[ConnectedClient]]
"""Подключение клиента: ``(api_id, api_hash, session_string) -> client``."""


@runtime_checkable
class ClientPool(Protocol):
    """
    Пул клиентов с управляемым жизненным циклом, который ведёт listener
    (``connect_all`` на старте, ``disconnect_all`` на остановке). Реализуется
    ``ClientRegistry``; в юнит-тестах listener'а — лёгким fake.
    """

    @property
    def account_ids(self) -> tuple[str, ...]:
        """Сконфигурированные аккаунты (известны до подключения)."""

    @property
    def clients(self) -> Mapping[str, TelegramClientPort]:
        """Подключённые клиенты по ``account_id``."""

    async def connect_all(self) -> None:
        """Подключить все клиенты (идемпотентно)."""

    async def disconnect_all(self) -> None:
        """Отключить все клиенты."""


class ClientRegistry:
    """Пул подключённых Telethon-клиентов, общий для listener'а и sender'а."""

    def __init__(
        self,
        *,
        credentials: Mapping[str, tuple[int, str]],
        session_store: SessionStorePort,
        env_sessions: Mapping[str, str] | None = None,
        connect: ConnectFn = connect_client,
    ) -> None:
        self._credentials = dict(credentials)
        self._session_store = session_store
        # T042: авторизованные StringSession из конфига/env per account —
        # приоритетнее app.db, подключаются напрямую без ключа шифрования.
        self._env_sessions = dict(env_sessions or {})
        self._connect = connect
        self._clients: dict[str, ConnectedClient] = {}

    @property
    def account_ids(self) -> tuple[str, ...]:
        """Сконфигурированные аккаунты (известны до ``connect_all``)."""
        return tuple(self._credentials)

    @property
    def clients(self) -> Mapping[str, TelegramClientPort]:
        """Подключённые клиенты по ``account_id`` (пусто до ``connect_all``)."""
        return self._clients

    async def connect_all(self) -> None:
        """Загрузить сессии и подключить клиент на каждый аккаунт; идемпотентно."""
        if self._clients:
            return
        for account_id, (api_id, api_hash) in self._credentials.items():
            # T042: env-сессия приоритетнее сохранённой в app.db.
            session_string = self._env_sessions.get(account_id)
            if session_string is None:
                session_string = await self._session_store.load(account_id)
            if session_string is None:
                msg = (
                    f'нет сессии для аккаунта {account_id!r}: выполни '
                    f'`angarion login --account {account_id}`'
                )
                raise ConfigError(msg)
            self._clients[account_id] = await self._connect(
                api_id, api_hash, session_string
            )

    async def disconnect_all(self) -> None:
        """Отключить все клиенты и очистить пул; идемпотентно."""
        for client in self._clients.values():
            await client.disconnect()
        self._clients = {}
