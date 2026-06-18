"""
Реальная обёртка над ``matrix-nio`` (граница nio, M7 B1/B2, T010).

Два уровня (зеркало telegram ``realclient``, W1 спеки T005):

- **Pure-трансляция** ``to_raw_*``: nio-событие → нормализованный
  ``client.RawMatrix*``. Без сети, тестируется на nio-фикстурах
  (``Event.from_dict``), как Telethon-маппинг (§13.2.1). Вся логика
  раскрытия правок/медиа/тредов — здесь, поверх ``event.source``.
- **Тонкая обёртка** ``MatrixClient`` (``MatrixClientPort``): восстановление
  сессии + E2EE key-store (``AsyncClientConfig(encryption_enabled=True)``,
  ``store_path``), регистрация nio-колбэков, sync-loop с
  ``next_batch``-курсором, отзывчивый ``stop`` (гонка sync vs стоп-событие).
  Сетевой путь в CI не исполняется — корректность проверяется на стенде
  (B4). nio без ``py.typed``: его типы суть ``Any`` (``ignore_missing_imports``).

Парольный ``password_login`` (B1) — отдельный короткоживущий клиент без
store: обменивает пароль на токен+``device_id``, дальше работает
``MatrixClient`` над восстановленной сессией.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from nio import (
    AsyncClient,
    AsyncClientConfig,
    ErrorResponse,
    Event,
    LoginError,
    MatrixRoom,
    MegolmEvent,
    RedactionEvent,
    Response,
    RoomMessage,
    RoomMessageMedia,
    RoomMessageText,
    RoomResolveAliasError,
    SyncResponse,
)

from angarion.adapters.matrix.client import (
    MatrixHistoryPage,
    MatrixRateLimitError,
    MatrixTransientError,
    RawMatrixDeletion,
    RawMatrixMedia,
    RawMatrixMessage,
    RawMatrixUndecryptable,
)
from angarion.adapters.matrix.session import MatrixSession
from angarion.domain.errors import ConfigError
from angarion.domain.models import EventKind

if TYPE_CHECKING:
    from angarion.adapters.matrix.client import (
        RawDeletionHandler,
        RawMessageHandler,
        SyncHandler,
        UndecryptableHandler,
    )
    from angarion.domain.ports import SessionStorePort

_SYNC_TIMEOUT_MS: Final = 30_000
"""Long-poll таймаут одного sync (мс); ``stop`` прерывает гонкой не дожидаясь."""

_MSGTYPE_KIND: Final = {
    'm.image': 'photo',
    'm.video': 'video',
    'm.audio': 'audio',
    'm.file': 'document',
}
"""Matrix ``msgtype`` → доменный открытый ``kind`` (§5, открытый str)."""

_KIND_MSGTYPE: Final = {
    'photo': 'm.image',
    'video': 'm.video',
    'audio': 'm.audio',
    'voice': 'm.audio',
    'document': 'm.file',
}
"""Доменный ``kind`` → Matrix ``msgtype`` для исходящих медиа (B3)."""


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


def _event_at(event: Event) -> datetime:
    """``server_timestamp`` (мс от epoch) → aware UTC (§17.4)."""
    return datetime.fromtimestamp(event.server_timestamp / 1000, tz=UTC)


def _extract_media(content: dict[str, Any]) -> tuple[RawMatrixMedia, ...]:
    """Медиа-``content`` (``m.image``/``m.video``/``m.audio``/``m.file``) → ref."""
    kind = _MSGTYPE_KIND.get(content.get('msgtype', ''))
    if kind is None:
        return ()
    info = content.get('info') or {}
    return (
        RawMatrixMedia(
            kind=kind,
            ref=content.get('url'),
            mime_type=info.get('mimetype'),
            file_name=content.get('body'),
            size=info.get('size'),
            width=info.get('w'),
            height=info.get('h'),
            duration=info.get('duration'),
        ),
    )


def to_raw_message(
    room_id: str, event: RoomMessage, sender_name: str | None
) -> RawMatrixMessage:
    """
    Перевод ``RoomMessageText``/``RoomMessageMedia`` → ``RawMatrixMessage``.

    Правка (``m.relates_to.rel_type == "m.replace"``) раскрывается в
    ``kind=MESSAGE_EDITED`` с ``event_id`` оригинала и текстом из
    ``m.new_content`` — ``external_id`` совпадает с записью реестра.
    Тред (``m.thread``) → ``thread_id``; reply → ``reply_to_event_id``.
    Медиа-сообщение несёт ``mxc``-ссылку, текст у него ``None``.
    """
    content: dict[str, Any] = event.source.get('content') or {}
    relates: dict[str, Any] = content.get('m.relates_to') or {}
    is_edit = relates.get('rel_type') == 'm.replace'
    effective: dict[str, Any] = (
        (content.get('m.new_content') or {}) if is_edit else content
    )
    msgtype = effective.get('msgtype', 'm.text')
    text = effective.get('body') if msgtype == 'm.text' else None
    is_thread = relates.get('rel_type') == 'm.thread'
    reply = relates.get('m.in_reply_to') or {}
    return RawMatrixMessage(
        kind=EventKind.MESSAGE_EDITED if is_edit else EventKind.MESSAGE_NEW,
        room_id=room_id,
        event_id=relates['event_id'] if is_edit else event.event_id,
        thread_id=relates.get('event_id') if is_thread else None,
        text=text,
        sender_id=event.sender,
        sender_name=sender_name,
        reply_to_event_id=None if is_thread else reply.get('event_id'),
        media=_extract_media(effective),
        event_at=_event_at(event),
    )


def to_raw_redaction(room_id: str, event: RedactionEvent) -> RawMatrixDeletion:
    """Перевод ``RedactionEvent`` → ``RawMatrixDeletion`` (один ``redacts`` id)."""
    return RawMatrixDeletion(
        room_id=room_id,
        redacts_event_id=event.redacts,
        redacted_at=_event_at(event),
    )


def to_raw_undecryptable(event: MegolmEvent) -> RawMatrixUndecryptable:
    """Перевод ``MegolmEvent`` (нет ключа сессии, UTD) → ``RawMatrixUndecryptable``."""
    return RawMatrixUndecryptable(
        room_id=event.room_id,
        event_id=event.event_id,
        sender_id=event.sender,
        event_at=_event_at(event),
    )


def _apply_relation(
    content: dict[str, Any], thread_root: str | None, reply_to: str | None
) -> None:
    """Проставить ``m.relates_to`` (тред / reply) в исходящий ``content`` (B3)."""
    relates: dict[str, Any] = {}
    if thread_root is not None:
        relates['rel_type'] = 'm.thread'
        relates['event_id'] = thread_root
        if reply_to is not None:
            relates['m.in_reply_to'] = {'event_id': reply_to}
            relates['is_falling_back'] = True
    elif reply_to is not None:
        relates['m.in_reply_to'] = {'event_id': reply_to}
    if relates:
        content['m.relates_to'] = relates


def _raise_for_send_error(response: Response) -> None:
    """Ошибку отправки nio → port-исключение (rate-limit / transient)."""
    if not isinstance(response, ErrorResponse):
        return
    retry_ms = getattr(response, 'retry_after_ms', None)
    if response.status_code == 'M_LIMIT_EXCEEDED' or retry_ms is not None:
        raise MatrixRateLimitError((retry_ms or 1000) / 1000)
    raise MatrixTransientError(str(response))


class MatrixClient:
    """``MatrixClientPort`` над nio ``AsyncClient`` с E2EE (M7 B2)."""

    def __init__(
        self, *, account_id: str, session_store: SessionStorePort, store_dir: str
    ) -> None:
        self._account_id = account_id
        self._session_store = session_store
        self._store_dir = store_dir
        self._client: Any = None
        self._stopped = asyncio.Event()
        self._on_new: list[RawMessageHandler] = []
        self._on_edit: list[RawMessageHandler] = []
        self._on_delete: list[RawDeletionHandler] = []
        self._on_utd: list[UndecryptableHandler] = []
        self._on_sync: list[SyncHandler] = []

    async def restore(self) -> None:
        """Загрузить сессию из стора + поднять E2EE key-store и колбэки."""
        if self._client is not None:
            return  # идемпотентно: общий клиент listener+sender (B3)
        raw = await self._session_store.load(self._account_id)
        if raw is None:
            msg = (
                f'нет сессии Matrix для аккаунта {self._account_id!r} — '
                f'выполни `angarion login`'
            )
            raise ConfigError(msg)
        session = MatrixSession.from_session_string(raw)
        await asyncio.to_thread(
            Path(self._store_dir).mkdir, parents=True, exist_ok=True
        )
        config = AsyncClientConfig(encryption_enabled=True, store_sync_tokens=False)
        client = AsyncClient(
            session.homeserver,
            session.user_id,
            device_id=session.device_id,
            store_path=self._store_dir,
            config=config,
        )
        client.restore_login(
            user_id=session.user_id,
            device_id=session.device_id,
            access_token=session.access_token,
        )
        client.load_store()
        client.add_event_callback(self._message_cb, (RoomMessageText, RoomMessageMedia))
        client.add_event_callback(self._redaction_cb, RedactionEvent)
        client.add_event_callback(self._megolm_cb, MegolmEvent)
        self._client = client

    async def resolve_room(self, alias_or_id: str) -> str:
        """``#alias`` → ``room_id`` через homeserver; ``!id`` — passthrough."""
        if alias_or_id.startswith('!'):
            return alias_or_id
        if not alias_or_id.startswith('#'):
            msg = f'Matrix room {alias_or_id!r}: ожидается !room_id или #alias:server'
            raise ConfigError(msg)
        response = await self._client.room_resolve_alias(alias_or_id)
        if isinstance(response, RoomResolveAliasError):
            msg = f'не удалось резолвить {alias_or_id!r}: {response.message}'
            raise ConfigError(msg)
        room_id: str = response.room_id
        return room_id

    async def sync_forever(self, *, since: str | None) -> None:
        """Sync-loop: позиция ``since`` → колбэки → ``next_batch`` в on_sync."""
        self._stopped.clear()
        next_since = since
        while not self._stopped.is_set():
            response = await self._sync_once(next_since)
            if response is None:
                break
            next_since = response.next_batch
            for handler in self._on_sync:
                await handler(response.next_batch)

    async def _sync_once(self, since: str | None) -> SyncResponse | None:
        """Один sync в гонке со стоп-событием → ответ или ``None`` при стопе."""
        sync_task = asyncio.ensure_future(
            self._client.sync(timeout=_SYNC_TIMEOUT_MS, since=since, full_state=False)
        )
        stop_task = asyncio.ensure_future(self._stopped.wait())
        done, pending = await asyncio.wait(
            {sync_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if sync_task not in done:
            return None
        return sync_task.result()

    async def stop(self) -> None:
        """Прервать sync-loop и закрыть соединение."""
        self._stopped.set()
        if self._client is not None:
            await self._client.close()

    async def send_text(
        self,
        room_id: str,
        body: str,
        *,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        """Отправить текст (E2EE автоматически) → ``event_id``."""
        content: dict[str, Any] = {'msgtype': 'm.text', 'body': body}
        _apply_relation(content, thread_root, reply_to)
        return await self._room_send(room_id, content)

    async def send_media(
        self,
        room_id: str,
        *,
        body: str,
        kind: str,
        mxc_ref: str | None = None,
        local_path: str | None = None,
        mime_type: str | None = None,
        file_name: str | None = None,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        """Отправить вложение → ``event_id``; деградация до текста при отсутствии."""
        url = await self._resolve_outgoing_mxc(
            mxc_ref, local_path, mime_type, file_name
        )
        if url is None:
            return await self.send_text(
                room_id, body, thread_root=thread_root, reply_to=reply_to
            )
        content: dict[str, Any] = {
            'msgtype': _KIND_MSGTYPE.get(kind, 'm.file'),
            'body': body,
            'url': url,
        }
        if mime_type:
            content['info'] = {'mimetype': mime_type}
        _apply_relation(content, thread_root, reply_to)
        return await self._room_send(room_id, content)

    async def _resolve_outgoing_mxc(
        self,
        mxc_ref: str | None,
        local_path: str | None,
        mime_type: str | None,
        file_name: str | None,
    ) -> str | None:
        """Залить файл (``local_path``) → ``mxc`` или переиспользовать ``mxc_ref``."""
        if local_path is not None:
            data = await asyncio.to_thread(Path(local_path).read_bytes)
            response, _keys = await self._client.upload(
                lambda *_: data,
                content_type=mime_type or 'application/octet-stream',
                filename=file_name,
                filesize=len(data),
            )
            if isinstance(response, ErrorResponse):
                raise MatrixTransientError(str(response))
            content_uri: str = response.content_uri
            return content_uri
        return mxc_ref

    async def _room_send(self, room_id: str, content: dict[str, Any]) -> str:
        """``room_send`` с трансляцией ошибок nio в port-исключения."""
        response = await self._client.room_send(
            room_id,
            message_type='m.room.message',
            content=content,
            ignore_unverified_devices=True,
        )
        _raise_for_send_error(response)
        event_id: str = response.event_id
        return event_id

    async def fetch_history(self, room_id: str, *, limit: int) -> MatrixHistoryPage:
        """``/messages`` от новых к старым → сообщения + redaction чанка."""
        response = await self._client.room_messages(room_id, start='', limit=limit)
        if isinstance(response, ErrorResponse):
            raise MatrixTransientError(str(response))
        messages: list[RawMatrixMessage] = []
        redactions: list[RawMatrixDeletion] = []
        for event in response.chunk:
            if isinstance(event, RedactionEvent):
                redactions.append(to_raw_redaction(room_id, event))
            elif isinstance(event, RoomMessage):
                messages.append(to_raw_message(room_id, event, None))
        return MatrixHistoryPage(messages=tuple(messages), redactions=tuple(redactions))

    async def download_media(self, *, mxc: str, dest_dir: str) -> str | None:
        """Скачать ``mxc://`` в ``dest_dir`` → локальный путь или ``None``."""
        response = await self._client.download(mxc)
        if isinstance(response, ErrorResponse):
            return None
        await asyncio.to_thread(Path(dest_dir).mkdir, parents=True, exist_ok=True)
        name = response.filename or mxc.rsplit('/', 1)[-1]
        path = Path(dest_dir) / name
        await asyncio.to_thread(path.write_bytes, response.body)
        return str(path)

    def on_new_message(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        self._on_edit.append(handler)

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        self._on_delete.append(handler)

    def on_undecryptable(self, handler: UndecryptableHandler) -> None:
        self._on_utd.append(handler)

    def on_sync(self, handler: SyncHandler) -> None:
        self._on_sync.append(handler)

    async def _message_cb(self, room: MatrixRoom, event: RoomMessage) -> None:
        raw = to_raw_message(room.room_id, event, room.user_name(event.sender))
        edited = raw.kind is EventKind.MESSAGE_EDITED
        handlers = self._on_edit if edited else self._on_new
        for handler in handlers:
            await handler(raw)

    async def _redaction_cb(self, room: MatrixRoom, event: RedactionEvent) -> None:
        raw = to_raw_redaction(room.room_id, event)
        for handler in self._on_delete:
            await handler(raw)

    async def _megolm_cb(self, _room: MatrixRoom, event: MegolmEvent) -> None:
        raw = to_raw_undecryptable(event)
        for handler in self._on_utd:
            await handler(raw)
