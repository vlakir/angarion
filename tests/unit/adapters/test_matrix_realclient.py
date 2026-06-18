"""
Тонкая nio-обёртка (W1, M7 B1/B2): парольный логин + pure-трансляция
nio-событий + sync-loop ``MatrixClient`` на подменённом ``AsyncClient``
без сети. Корректность реального обмена — ручным прогоном на стенде (B4).
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from nio import (
    ErrorResponse,
    LoginError,
    MegolmEvent,
    RedactionEvent,
    RoomMessageImage,
    RoomMessageText,
    RoomResolveAliasError,
)

from angarion.adapters.matrix import realclient
from angarion.adapters.matrix.client import (
    MatrixRateLimitError,
    MatrixTransientError,
)
from angarion.adapters.matrix.realclient import (
    MatrixClient,
    _apply_relation,
    _raise_for_send_error,
    password_login,
    to_raw_message,
    to_raw_redaction,
    to_raw_undecryptable,
)
from angarion.adapters.matrix.session import MatrixSession
from angarion.adapters.memory.storage import MemorySessionStore
from angarion.domain.errors import ConfigError
from angarion.domain.models import EventKind


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


# --- pure-трансляция nio → RawMatrix* (на nio-фикстурах, §13.2.1) ---

TS = 1_700_000_000_000  # origin_server_ts (мс)


def _event(event_type: str, content: dict[str, Any], **top: Any) -> dict[str, Any]:
    return {
        'event_id': top.get('event_id', '$e1'),
        'sender': top.get('sender', '@alice:matrix.example'),
        'origin_server_ts': TS,
        'type': event_type,
        'content': content,
        **{k: v for k, v in top.items() if k not in {'event_id', 'sender'}},
    }


def _text(body: str = 'привет', **content: Any) -> Any:
    return RoomMessageText.from_dict(
        _event('m.room.message', {'msgtype': 'm.text', 'body': body, **content})
    )


class TestToRawMessage:
    def test_new_text(self) -> None:
        raw = to_raw_message('!r:s', _text('hi'), 'Алиса')
        assert raw.kind is EventKind.MESSAGE_NEW
        assert raw.room_id == '!r:s'
        assert raw.event_id == '$e1'
        assert raw.text == 'hi'
        assert raw.sender_id == '@alice:matrix.example'
        assert raw.sender_name == 'Алиса'
        assert raw.event_at.year == 2023

    def test_edit_uses_new_content_and_original_id(self) -> None:
        event = _text(
            '* new',
            **{
                'm.new_content': {'msgtype': 'm.text', 'body': 'new'},
                'm.relates_to': {'rel_type': 'm.replace', 'event_id': '$orig'},
            },
        )
        raw = to_raw_message('!r:s', event, None)
        assert raw.kind is EventKind.MESSAGE_EDITED
        assert raw.event_id == '$orig'
        assert raw.text == 'new'

    def test_thread_id(self) -> None:
        event = _text(
            'in thread',
            **{'m.relates_to': {'rel_type': 'm.thread', 'event_id': '$root'}},
        )
        raw = to_raw_message('!r:s', event, None)
        assert raw.thread_id == '$root'
        assert raw.reply_to_event_id is None  # thread-fallback не считаем reply

    def test_reply(self) -> None:
        event = _text(
            'reply',
            **{'m.relates_to': {'m.in_reply_to': {'event_id': '$parent'}}},
        )
        raw = to_raw_message('!r:s', event, None)
        assert raw.reply_to_event_id == '$parent'
        assert raw.thread_id is None

    def test_media_image(self) -> None:
        event = RoomMessageImage.from_dict(
            _event(
                'm.room.message',
                {
                    'msgtype': 'm.image',
                    'body': 'pic.jpg',
                    'url': 'mxc://matrix.example/abc',
                    'info': {
                        'mimetype': 'image/jpeg',
                        'size': 2048,
                        'w': 800,
                        'h': 600,
                    },
                },
            )
        )
        raw = to_raw_message('!r:s', event, None)
        assert raw.text is None
        assert len(raw.media) == 1
        media = raw.media[0]
        assert media.kind == 'photo'
        assert media.ref == 'mxc://matrix.example/abc'
        assert media.mime_type == 'image/jpeg'
        assert media.size == 2048
        assert media.width == 800
        assert media.height == 600
        assert media.file_name == 'pic.jpg'


def test_to_raw_redaction() -> None:
    event = RedactionEvent.from_dict(
        _event('m.room.redaction', {}, event_id='$r1', redacts='$gone')
    )
    raw = to_raw_redaction('!r:s', event)
    assert raw.room_id == '!r:s'
    assert raw.redacts_event_id == '$gone'


def test_to_raw_undecryptable() -> None:
    event = MegolmEvent.from_dict(
        _event(
            'm.room.encrypted',
            {
                'algorithm': 'm.megolm.v1.aes-sha2',
                'ciphertext': 'X',
                'sender_key': 'k',
                'session_id': 's',
                'device_id': 'D',
            },
            event_id='$m1',
            room_id='!r:s',
        )
    )
    raw = to_raw_undecryptable(event)
    assert raw.room_id == '!r:s'
    assert raw.event_id == '$m1'
    assert raw.sender_id == '@alice:matrix.example'


# --- MatrixClient: lifecycle + sync-loop на подменённом AsyncClient ---


class _FakeRoom:
    def __init__(self, room_id: str, names: dict[str, str] | None = None) -> None:
        self.room_id = room_id
        self._names = names or {}

    def user_name(self, user_id: str) -> str | None:
        return self._names.get(user_id)


class _FakeAsyncClient:
    def __init__(
        self,
        homeserver: str,
        user_id: str,
        *,
        device_id: str,
        store_path: str,
        config: object,
    ) -> None:
        self.homeserver = homeserver
        self.user_id = user_id
        self.device_id = device_id
        self.store_path = store_path
        self.config = config
        self.restore_kwargs: dict[str, object] = {}
        self.store_loaded = 0
        self.closed = 0
        self.callbacks: list[object] = []
        self.sync_calls: list[str | None] = []
        self._responses: list[object] = []
        self.alias_response: object = SimpleNamespace(room_id='!resolved:s')
        self.room_sends: list[dict[str, object]] = []
        self.send_response: object = SimpleNamespace(event_id='$sent-1')
        self.uploads: list[dict[str, object]] = []
        self.upload_response: object = SimpleNamespace(content_uri='mxc://up/1')
        self.messages_calls: list[tuple[str, int]] = []
        self.messages_response: object = SimpleNamespace(chunk=[])
        self.download_calls: list[str] = []
        self.download_response: object = SimpleNamespace(body=b'data', filename='f.bin')

    def restore_login(self, **kwargs: object) -> None:
        self.restore_kwargs = kwargs

    def load_store(self) -> None:
        self.store_loaded += 1

    def add_event_callback(self, callback: object, _types: object) -> None:
        self.callbacks.append(callback)

    async def room_resolve_alias(self, _alias: str) -> object:
        return self.alias_response

    async def sync(self, *, timeout: int, since: str | None, full_state: bool) -> object:
        assert timeout > 0
        assert full_state is False
        self.sync_calls.append(since)
        if self._responses:
            return self._responses.pop(0)
        await asyncio.Event().wait()  # блок до отмены гонкой stop
        msg = 'unreachable'
        raise AssertionError(msg)

    async def close(self) -> None:
        self.closed += 1

    async def room_send(
        self,
        room_id: str,
        *,
        message_type: str,
        content: dict[str, Any],
        ignore_unverified_devices: bool,
    ) -> object:
        self.room_sends.append(
            {
                'room_id': room_id,
                'message_type': message_type,
                'content': content,
                'ignore_unverified': ignore_unverified_devices,
            }
        )
        return self.send_response

    async def upload(
        self,
        data_provider: object,
        *,
        content_type: str,
        filename: str | None,
        filesize: int,
    ) -> tuple[object, None]:
        self.uploads.append({'content_type': content_type, 'filename': filename})
        return self.upload_response, None

    async def room_messages(self, room_id: str, *, start: str, limit: int) -> object:
        self.messages_calls.append((room_id, limit))
        return self.messages_response

    async def download(self, mxc: str) -> object:
        self.download_calls.append(mxc)
        return self.download_response


def _session() -> MatrixSession:
    return MatrixSession(
        homeserver='https://matrix.example',
        user_id='@bot:matrix.example',
        device_id='DEV1',
        access_token='tok-xyz',
    )


async def _client_with_session(store_dir: str) -> MatrixClient:
    store = MemorySessionStore()
    await store.save('main', _session().to_session_string())
    return MatrixClient(
        account_id='main', session_store=store, store_dir=store_dir
    )


def _patch_async_client(
    monkeypatch: pytest.MonkeyPatch,
) -> list[_FakeAsyncClient]:
    built: list[_FakeAsyncClient] = []

    def factory(homeserver: str, user_id: str, **kwargs: Any) -> _FakeAsyncClient:
        client = _FakeAsyncClient(homeserver, user_id, **kwargs)
        built.append(client)
        return client

    monkeypatch.setattr(realclient, 'AsyncClient', factory)
    monkeypatch.setattr(
        realclient, 'AsyncClientConfig', lambda **kw: SimpleNamespace(**kw)
    )
    return built


class TestMatrixClientRestore:
    async def test_restore_configures_e2ee_and_callbacks(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path / 'e2e'))
        await client.restore()
        fake = built[0]
        assert fake.config.encryption_enabled is True
        assert fake.restore_kwargs['access_token'] == 'tok-xyz'
        assert fake.restore_kwargs['device_id'] == 'DEV1'
        assert fake.store_loaded == 1
        assert len(fake.callbacks) == 3  # message / redaction / megolm
        assert (tmp_path / 'e2e').is_dir()

    async def test_restore_without_session_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        _patch_async_client(monkeypatch)
        client = MatrixClient(
            account_id='main',
            session_store=MemorySessionStore(),
            store_dir=str(tmp_path),
        )
        with pytest.raises(ConfigError, match='angarion login'):
            await client.restore()


class TestMatrixClientResolveRoom:
    async def test_room_id_passthrough(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        assert await client.resolve_room('!room:s') == '!room:s'

    async def test_alias_resolved(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0].alias_response = SimpleNamespace(room_id='!resolved:s')
        assert await client.resolve_room('#alias:s') == '!resolved:s'

    async def test_alias_error_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0].alias_response = RoomResolveAliasError(message='nope')
        with pytest.raises(ConfigError, match='nope'):
            await client.resolve_room('#bad:s')

    async def test_bad_form_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        with pytest.raises(ConfigError, match='!room_id или #alias'):
            await client.resolve_room('plain')


class TestMatrixClientCallbacks:
    async def test_message_cb_routes_new_and_edit(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        new: list[Any] = []
        edited: list[Any] = []
        client.on_new_message(lambda r: _collect(new, r))
        client.on_message_edited(lambda r: _collect(edited, r))
        room = _FakeRoom('!r:s', {'@alice:matrix.example': 'Алиса'})
        await client._message_cb(room, _text('hi'))
        edit_event = _text(
            '* x',
            **{
                'm.new_content': {'msgtype': 'm.text', 'body': 'x'},
                'm.relates_to': {'rel_type': 'm.replace', 'event_id': '$o'},
            },
        )
        await client._message_cb(room, edit_event)
        assert len(new) == 1
        assert new[0].sender_name == 'Алиса'
        assert len(edited) == 1
        assert edited[0].event_id == '$o'

    async def test_redaction_and_megolm_cb(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        deleted: list[Any] = []
        utd: list[Any] = []
        client.on_message_deleted(lambda r: _collect(deleted, r))
        client.on_undecryptable(lambda r: _collect(utd, r))
        room = _FakeRoom('!r:s')
        redaction = RedactionEvent.from_dict(
            _event('m.room.redaction', {}, redacts='$gone')
        )
        await client._redaction_cb(room, redaction)
        megolm = MegolmEvent.from_dict(
            _event(
                'm.room.encrypted',
                {
                    'algorithm': 'm.megolm.v1.aes-sha2',
                    'ciphertext': 'X',
                    'sender_key': 'k',
                    'session_id': 's',
                    'device_id': 'D',
                },
                room_id='!r:s',
            )
        )
        await client._megolm_cb(room, megolm)
        assert deleted[0].redacts_event_id == '$gone'
        assert utd[0].room_id == '!r:s'


async def _collect(sink: list[Any], raw: Any) -> None:
    sink.append(raw)


class TestMatrixClientSyncLoop:
    async def test_sync_loop_reports_next_batch_and_stops(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0]._responses = [SimpleNamespace(next_batch='s1')]
        tokens: list[str] = []
        client.on_sync(lambda t: _collect(tokens, t))
        task = asyncio.create_task(client.sync_forever(since='s0'))
        # дать первому sync вернуться, второму — заблокироваться
        for _ in range(5):
            await asyncio.sleep(0)
            if tokens:
                break
        assert tokens == ['s1']
        assert built[0].sync_calls[0] == 's0'  # стартовали с курсора
        await client.stop()
        await task
        assert built[0].closed == 1


class TestApplyRelation:
    def test_thread_only(self) -> None:
        content: dict[str, Any] = {'body': 'x'}
        _apply_relation(content, '$root', None)
        assert content['m.relates_to'] == {'rel_type': 'm.thread', 'event_id': '$root'}

    def test_thread_with_reply_fallback(self) -> None:
        content: dict[str, Any] = {}
        _apply_relation(content, '$root', '$parent')
        rel = content['m.relates_to']
        assert rel['rel_type'] == 'm.thread'
        assert rel['m.in_reply_to'] == {'event_id': '$parent'}
        assert rel['is_falling_back'] is True

    def test_reply_only(self) -> None:
        content: dict[str, Any] = {}
        _apply_relation(content, None, '$parent')
        assert content['m.relates_to'] == {'m.in_reply_to': {'event_id': '$parent'}}

    def test_none(self) -> None:
        content: dict[str, Any] = {'body': 'x'}
        _apply_relation(content, None, None)
        assert 'm.relates_to' not in content


class TestRaiseForSendError:
    def test_success_passes(self) -> None:
        _raise_for_send_error(SimpleNamespace(event_id='$s1'))  # не падает

    def test_rate_limit_by_status(self) -> None:
        err = ErrorResponse(message='slow down', status_code='M_LIMIT_EXCEEDED')
        err.retry_after_ms = 1500
        with pytest.raises(MatrixRateLimitError) as exc:
            _raise_for_send_error(err)
        assert exc.value.retry_after == 1.5

    def test_other_error_transient(self) -> None:
        err = ErrorResponse(message='boom', status_code='M_UNKNOWN')
        err.retry_after_ms = None
        with pytest.raises(MatrixTransientError):
            _raise_for_send_error(err)


class TestMatrixClientSend:
    async def test_send_text_builds_content(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        event_id = await client.send_text('!r:s', 'привет', thread_root='$root')
        assert event_id == '$sent-1'
        sent = built[0].room_sends[0]
        assert sent['content']['msgtype'] == 'm.text'
        assert sent['content']['body'] == 'привет'
        assert sent['content']['m.relates_to']['event_id'] == '$root'
        assert sent['ignore_unverified'] is True

    async def test_send_media_local_path_uploads(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        f = tmp_path / 'pic.jpg'
        f.write_bytes(b'JPEGDATA')
        await client.send_media(
            '!r:s', body='подпись', kind='photo', local_path=str(f),
            mime_type='image/jpeg', file_name='pic.jpg',
        )
        assert built[0].uploads[0]['content_type'] == 'image/jpeg'
        content = built[0].room_sends[0]['content']
        assert content['msgtype'] == 'm.image'
        assert content['url'] == 'mxc://up/1'
        assert content['body'] == 'подпись'

    async def test_send_media_mxc_ref_no_upload(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        await client.send_media('!r:s', body='c', kind='video', mxc_ref='mxc://s/v')
        assert built[0].uploads == []  # переотправка по ссылке без заливки
        assert built[0].room_sends[0]['content']['url'] == 'mxc://s/v'

    async def test_send_media_no_source_degrades_to_text(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        await client.send_media('!r:s', body='подпись', kind='photo')
        assert built[0].room_sends[0]['content']['msgtype'] == 'm.text'

    async def test_room_send_rate_limit_translated(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        err = ErrorResponse(message='slow', status_code='M_LIMIT_EXCEEDED')
        err.retry_after_ms = 2000
        built[0].send_response = err
        with pytest.raises(MatrixRateLimitError):
            await client.send_text('!r:s', 'x')


class TestMatrixClientHistory:
    async def test_fetch_history_splits_messages_and_redactions(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        msg_event = RoomMessageText.from_dict(
            _event('m.room.message', {'msgtype': 'm.text', 'body': 'hi'})
        )
        red_event = RedactionEvent.from_dict(
            _event('m.room.redaction', {}, event_id='$r1', redacts='$gone')
        )
        built[0].messages_response = SimpleNamespace(chunk=[msg_event, red_event])
        page = await client.fetch_history('!r:s', limit=50)
        assert len(page.messages) == 1
        assert page.messages[0].text == 'hi'
        assert len(page.redactions) == 1
        assert page.redactions[0].redacts_event_id == '$gone'

    async def test_fetch_history_error_raises_transient(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0].messages_response = ErrorResponse(message='nope')
        with pytest.raises(MatrixTransientError):
            await client.fetch_history('!r:s', limit=50)


class TestMatrixClientDownload:
    async def test_download_writes_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0].download_response = SimpleNamespace(body=b'BYTES', filename='a.bin')
        dest = tmp_path / 'media'
        path = await client.download_media(mxc='mxc://s/a', dest_dir=str(dest))
        assert path is not None
        assert (dest / 'a.bin').read_bytes() == b'BYTES'

    async def test_download_error_returns_none(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        built = _patch_async_client(monkeypatch)
        client = await _client_with_session(str(tmp_path))
        await client.restore()
        built[0].download_response = ErrorResponse(message='gone')
        assert await client.download_media(mxc='mxc://s/a', dest_dir=str(tmp_path)) is None
