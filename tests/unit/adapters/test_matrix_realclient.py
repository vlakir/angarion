"""
Тонкая nio-обёртка (W1, M7 B1): парольный логин на подменённом
``AsyncClient`` без сетевых вызовов. Корректность реального обмена
проверяется ручным прогоном на стенде (B4).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from nio import LoginError

from angarion.adapters.matrix import realclient
from angarion.adapters.matrix.realclient import password_login
from angarion.adapters.matrix.session import MatrixSession
from angarion.domain.errors import ConfigError


class _FakeClient:
    def __init__(self, response: object) -> None:
        self._response = response
        self.closed = False
        self.login_args: tuple[object, ...] = ()
        self.login_kwargs: dict[str, object] = {}

    async def login(self, password: str, *, device_name: str) -> object:
        self.login_args = (password,)
        self.login_kwargs = {'device_name': device_name}
        return self._response

    async def close(self) -> None:
        self.closed = True


def _patch_client(
    monkeypatch: pytest.MonkeyPatch, response: object
) -> list[_FakeClient]:
    built: list[_FakeClient] = []

    def factory(homeserver: str, user_id: str) -> _FakeClient:
        client = _FakeClient(response)
        client.homeserver = homeserver  # type: ignore[attr-defined]
        client.user_id = user_id  # type: ignore[attr-defined]
        built.append(client)
        return client

    monkeypatch.setattr(realclient, 'AsyncClient', factory)
    return built


async def test_password_login_returns_session_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = SimpleNamespace(
        user_id='@bot:matrix.example',
        device_id='DEVABC',
        access_token='tok-fake-secret',
    )
    built = _patch_client(monkeypatch, response)
    raw = await password_login(
        'https://matrix.example', '@bot:matrix.example', 's3cret', 'angarion'
    )
    session = MatrixSession.from_session_string(raw)
    assert session.homeserver == 'https://matrix.example'
    assert session.user_id == '@bot:matrix.example'
    assert session.device_id == 'DEVABC'
    assert session.access_token == 'tok-fake-secret'
    client = built[0]
    assert client.login_args == ('s3cret',)
    assert client.login_kwargs == {'device_name': 'angarion'}
    assert client.closed is True  # соединение закрыто всегда


async def test_password_login_closes_client_on_login_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    built = _patch_client(monkeypatch, LoginError(message='bad creds'))
    with pytest.raises(ConfigError, match='не удался: bad creds'):
        await password_login(
            'https://matrix.example', '@bot:matrix.example', 'wrong', 'angarion'
        )
    assert built[0].closed is True


async def test_password_login_closes_client_on_network_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сетевой сбой nio пробрасывается, но соединение всё равно закрыто."""
    built: list[_FakeClient] = []

    class _Boom(_FakeClient):
        async def login(self, password: str, *, device_name: str) -> object:
            msg = 'connection reset'
            raise OSError(msg)

    def factory(homeserver: str, user_id: str) -> _Boom:
        client = _Boom(None)
        built.append(client)
        return client

    monkeypatch.setattr(realclient, 'AsyncClient', factory)
    with pytest.raises(OSError, match='connection reset'):
        await password_login('https://m.ex', '@b:m.ex', 'p', 'angarion')
    assert built[0].closed is True
