"""
Оснастка интеграционного контура Matrix (§13.2, M7 B4, T010).

Поднимает **настоящий** конвейер через ``build_app`` на реальных
backend'ах (sqlite storage, persistqueue) и реальном ``MatrixClient``
(matrix-nio) против локального homeserver'а. Синтетический matrix-плагин
отдаёт боевые ``MatrixListener``/``MatrixSender`` поверх одного
test-owned ``MatrixClient`` (как боевой плагин делит пул listener+sender).

Драйв — через **live-sync**: в отличие от Telegram (self-send pts-gap),
Matrix надёжно отдаёт собственные события устройства через sync, поэтому
драйвер (``MatrixDriver`` — отдельный nio-клиент = второе устройство того
же аккаунта) публикует/правит/удаляет в источнике, а listener'ское
устройство получает события через sync → пайплайн → доставка в цель.

E2EE: драйвер и listener — оба с E2EE-стором; ключи megolm шарятся
device→device при отправке (``ignore_unverified_devices``), listener
расшифровывает входящие. Чтение цели — фоновый sync драйвера, копящий
``(room_id, body)`` (цель всегда незашифрованная — упрощает чтение).

Самоочистка: комнаты создаются на прогон и покидаются в ``aclose``;
homeserver — эфемерный стенд, между прогонами пересоздаётся.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nio import (
    AsyncClient,
    AsyncClientConfig,
    LoginError,
    RoomMessage,
)

from angarion.adapters.matrix.listener import MatrixListener
from angarion.adapters.matrix.plugin import MATRIX_CAPABILITIES, MatrixAccountConfig
from angarion.adapters.matrix.realclient import MatrixClient
from angarion.adapters.matrix.session import MatrixSession
from angarion.adapters.matrix.sender import MatrixSender
from angarion.adapters.memory.storage import MemorySessionStore
from angarion.adapters.queue.persistqueue_ import QUEUE_BACKEND
from angarion.adapters.storage.plugin import STORAGE_BACKEND
from angarion.bootstrap import LoadedPlugins, build_app
from angarion.config import AngarionSettings, EndpointConfig, PipelineConfig
from angarion.domain.models import RecordKind
from angarion.domain.plugin import AdapterPlugin
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping, Sequence
    from pathlib import Path

    from nio import MatrixRoom

ACCOUNT = 'bot'


async def wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float = 30.0,
    interval: float = 0.5,
) -> bool:
    """Поллинг (синхронного) условия с таймаутом; True — выполнилось."""
    elapsed = 0.0
    while elapsed < timeout:
        if predicate():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


async def _login(
    homeserver: str, user_id: str, password: str, store_path: str, *, encrypted: bool = True
) -> Any:
    """Залогинить nio-клиент (новое устройство); ``encrypted`` — поднимать E2EE-стор."""
    Path(store_path).mkdir(parents=True, exist_ok=True)
    config = AsyncClientConfig(encryption_enabled=encrypted, store_sync_tokens=False)
    client = AsyncClient(homeserver, user_id, store_path=store_path, config=config)
    response = await client.login(password)
    if isinstance(response, LoginError):
        await client.close()
        msg = f'login {user_id} не удался: {response.message}'
        raise RuntimeError(msg)
    client.load_store()
    return client


class MatrixDriver:
    """Второе устройство аккаунта: драйв источника + чтение цели (live sync)."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self.received: list[tuple[str, str]] = []
        self._sync_task: asyncio.Task[None] | None = None
        self._client.add_event_callback(self._collect, RoomMessage)

    async def _collect(self, room: MatrixRoom, event: RoomMessage) -> None:
        self.received.append((room.room_id, event.body))

    async def start_sync(self) -> None:
        """Первичный sync + фоновый цикл (читатель цели; без E2EE)."""
        await self._client.sync(timeout=3000, full_state=False)
        self._sync_task = asyncio.create_task(
            self._client.sync_forever(timeout=2000, full_state=False),
            name='matrix-reader-sync',
        )

    async def create_room(self, *, encrypted: bool = False) -> str:
        """Создать комнату (опц. зашифрованную) → room_id."""
        initial_state = (
            [
                {
                    'type': 'm.room.encryption',
                    'state_key': '',
                    'content': {'algorithm': 'm.megolm.v1.aes-sha2'},
                }
            ]
            if encrypted
            else []
        )
        response = await self._client.room_create(initial_state=initial_state)
        room_id: str = response.room_id
        return room_id

    def delivered(self, room_id: str, needle: str) -> bool:
        """True — среди собранных через sync сообщений комнаты есть с подстрокой."""
        return any(
            rid == room_id and needle in body for rid, body in self.received
        )

    async def aclose(self) -> None:
        """Остановить фоновый sync, разлогинить устройство и закрыть клиент."""
        if self._sync_task is not None:
            self._sync_task.cancel()
        with suppress(Exception):
            await self._client.logout()  # не копить устройства между прогонами
        await self._client.close()


async def logout_listener(client: MatrixClient) -> None:
    """Разлогинить listener-устройство (не копить устройства между прогонами)."""
    with suppress(Exception):
        await client._client.logout()  # noqa: SLF001 (test harness)


def source_poster(client: MatrixClient) -> Any:
    """
    nio-клиент listener-устройства — им же драйвим источник (M7 B4).

    Источник публикует **то же** устройство, что и listener: для E2EE это
    делает обмен ключами не нужным (устройство расшифровывает собственное
    megolm-событие, owns the session) — тестируем наш decrypt→map→deliver,
    а кросс-девайсный обмен ключами — забота nio, не адаптера. Доступ к
    приватному nio-клиенту — как у Telegram-контура (`pool.raw`).
    """
    return client._client  # noqa: SLF001 (test harness, см. telegram pool.raw)


async def post(client: Any, room_id: str, body: str) -> str:
    """Опубликовать текст в комнату → event_id."""
    response = await client.room_send(
        room_id,
        message_type='m.room.message',
        content={'msgtype': 'm.text', 'body': body},
        ignore_unverified_devices=True,
    )
    return str(response.event_id)


async def post_media(client: Any, room_id: str, file_name: str, data: bytes) -> str:
    """Залить файл и отправить как ``m.file`` в комнату → event_id."""
    upload, _keys = await client.upload(
        lambda *_: data,
        content_type='text/plain',
        filename=file_name,
        filesize=len(data),
    )
    response = await client.room_send(
        room_id,
        message_type='m.room.message',
        content={
            'msgtype': 'm.file',
            'body': file_name,
            'url': upload.content_uri,
            'info': {'mimetype': 'text/plain', 'size': len(data)},
        },
        ignore_unverified_devices=True,
    )
    return str(response.event_id)


async def edit(client: Any, room_id: str, target: str, body: str) -> None:
    """Отредактировать (m.replace) сообщение."""
    await client.room_send(
        room_id,
        message_type='m.room.message',
        content={
            'msgtype': 'm.text',
            'body': f'* {body}',
            'm.new_content': {'msgtype': 'm.text', 'body': body},
            'm.relates_to': {'rel_type': 'm.replace', 'event_id': target},
        },
        ignore_unverified_devices=True,
    )


async def redact(client: Any, room_id: str, event_id: str) -> None:
    """Удалить (redaction) сообщение."""
    await client.room_redact(room_id, event_id)


def build_matrix_plugin(client: MatrixClient) -> AdapterPlugin:
    """Синтетический matrix-плагин: боевые listener/sender поверх одного клиента."""
    clients: dict[str, Any] = {ACCOUNT: client}

    def make_listener(
        deps: Any,
        _accounts: Mapping[str, object],
        sources: Sequence[EndpointConfig],
    ) -> MatrixListener:
        catchup = deps.settings.catchup
        return MatrixListener(
            ingest=deps.ingest,
            clients=clients,
            sources=sources,
            cursors=deps.storage.cursors,
            analytics=deps.storage.analytics,
            log=get_logger('integration.matrix.listener'),
            media_policy=deps.settings.media,
            catchup_enabled=catchup.enabled,
            catchup_max_messages=catchup.max_messages_per_source,
            catchup_max_age_days=catchup.max_age_days,
        )

    def make_sender(deps: Any, _accounts: Mapping[str, object]) -> MatrixSender:
        return MatrixSender(
            clients=clients, log=get_logger('integration.matrix.sender')
        )

    return AdapterPlugin(
        name='matrix',
        capabilities=MATRIX_CAPABILITIES,
        account_config_model=MatrixAccountConfig,
        make_listener=make_listener,
        make_sender=make_sender,
    )


def build_plugins(client: MatrixClient) -> LoadedPlugins:
    """Реестр: синтетический matrix + реальные sqlite/persistqueue."""
    return LoadedPlugins(
        adapters={'matrix': build_matrix_plugin(client)},
        queues={'persistqueue': QUEUE_BACKEND},
        storages={'sqlite': STORAGE_BACKEND},
    )


def build_settings(
    *,
    homeserver: str,
    user_id: str,
    db_path: Path,
    queue_path: Path,
    pipelines: Mapping[str, PipelineConfig],
) -> AngarionSettings:
    """Конфиг контура: один matrix-аккаунт, sqlite + persistqueue."""
    payload: dict[str, Any] = {
        'accounts': {
            ACCOUNT: {
                'transport': 'matrix',
                'homeserver': homeserver,
                'user_id': user_id,
            }
        },
        'storage': {'backend': 'sqlite', 'path': str(db_path)},
        'queue': {'backend': 'persistqueue', 'path': str(queue_path)},
        'catchup': {'enabled': True},
        'worker': {'poll_interval': 0.2},
        'pipelines': dict(pipelines),
    }
    return AngarionSettings.model_validate(payload)


def mirror_pipeline(
    *,
    source_room: str,
    target_room: str,
    processor: str = 'integration_echo',
    events: frozenset[RecordKind] | None = None,
) -> PipelineConfig:
    """Пайплайн «зеркало»: source_room → target_room."""
    return PipelineConfig(
        processor=processor,
        events=events if events is not None else frozenset(RecordKind),
        sources=(EndpointConfig(account=ACCOUNT, address=source_room),),
        targets=(EndpointConfig(account=ACCOUNT, address=target_room),),
    )


async def make_matrix_client(
    *, homeserver: str, user_id: str, password: str, store_path: str
) -> MatrixClient:
    """Залогинить listener-устройство и собрать MatrixClient на засеянной сессии."""
    login_client = await _login(homeserver, user_id, password, store_path + '-login')
    try:
        session = MatrixSession(
            homeserver=homeserver,
            user_id=login_client.user_id,
            device_id=login_client.device_id,
            access_token=login_client.access_token,
        )
    finally:
        await login_client.close()
    store = MemorySessionStore()
    await store.save(ACCOUNT, session.to_session_string())
    return MatrixClient(
        account_id=ACCOUNT, session_store=store, store_dir=store_path
    )


async def make_driver(
    *, homeserver: str, user_id: str, password: str, store_path: str
) -> MatrixDriver:
    """
    Залогинить читатель-устройство (читает незашифрованную цель) и запустить sync.

    Без E2EE: цель всегда незашифрована, а источник драйвит listener-
    устройство (``source_poster``) — читателю ключи не нужны, что убирает
    дорогие key-запросы (и связанные rate-limit/UTD-шумы) на его стороне.
    """
    client = await _login(homeserver, user_id, password, store_path, encrypted=False)
    driver = MatrixDriver(client)
    await driver.start_sync()
    return driver
