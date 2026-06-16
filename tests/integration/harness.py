"""
Оснастка интеграционного контура §13.2 (T009 / M6, фаза 2).

Поднимает **настоящий** конвейер через ``build_app`` на реальных
backend'ах (sqlite storage, persistqueue) и реальном Telethon-клиенте,
которым **владеет сам тест**: один коннект кладётся в ``IntegrationPool``,
реальные ``TelegramListener``/``TelegramSender`` работают поверх него
(как в боевом плагине), а тест тем же клиентом драйвит сообщения в
источник (``post``/``edit``/``delete``) и читает доставки в цели
(``read_texts``). Один аккаунт, один коннект — апдейты гарантированно
доходят до listener'а (две параллельные сессии на одном auth-key
отвергнуты, см. spec Clarify).

Темп — с запасом под лимиты Telegram: sender держит дефолтный троттлинг
(≤ 1 msg/s на чат, ≤ 20/min на аккаунт), ожидание доставки — polling
истории цели с таймаутом. Самоочистка — по уникальному nonce в тексте:
``cleanup`` удаляет из групп только сообщения теста (и драйвера, и
доставленные пайплайном), не трогая постороннее.

Этот модуль — building blocks; сценарии живут в ``test_pipeline.py``.
Поскольку контур гоняется только на реальном аккаунте (маркер
``integration``, default-skip), здесь оптимизировано на верность боевому
пути, а не на прогон в CI.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from telethon import TelegramClient
from telethon.sessions import StringSession

from angarion.adapters.queue.persistqueue_ import QUEUE_BACKEND
from angarion.adapters.storage.plugin import STORAGE_BACKEND
from angarion.adapters.telegram.plugin import (
    TELEGRAM_CAPABILITIES,
    TelegramAccountConfig,
)
from angarion.adapters.telegram.realclient import TelethonClient, connect_client
from angarion.adapters.telegram.sender import TelegramSender
from angarion.adapters.telegram.listener import TelegramListener
from angarion.bootstrap import LoadedPlugins, build_app
from angarion.config import AngarionSettings, EndpointConfig, PipelineConfig
from angarion.domain.models import (
    EventKind,
    OutboundMessage,
    ProcessingResult,
    Verdict,
)
from angarion.domain.plugin import AdapterPlugin
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
    from pathlib import Path

    from angarion.adapters.telegram.client import TelegramClientPort
    from angarion.bootstrap import AdapterDeps, AngarionApp
    from angarion.domain.models import (
        InboundEvent,
        PipelineContextData,
        ProcessorServices,
    )

ECHO_PROCESSOR = 'integration_echo'
"""Имя echo-процессора контура (регистрируется фикстурой conftest)."""

_KIND_TAG = {
    EventKind.MESSAGE_NEW: 'NEW',
    EventKind.MESSAGE_EDITED: 'EDIT',
    EventKind.MESSAGE_DELETED: 'DEL',
}


class IntegrationPool:
    """
    ``ClientPool`` поверх одного реального Telethon-клиента (test-owned).

    Соединение создаётся в ``connect_all`` из ``StringSession`` и живёт до
    ``disconnect_all`` — один коннект на весь прогон. ``raw`` отдаёт сырой
    ``TelegramClient`` для драйва ``edit``/``delete``, которых нет в
    минимальном порту ``TelegramClientPort`` (бою они не нужны).
    """

    def __init__(
        self, account_id: str, api_id: int, api_hash: str, session_string: str
    ) -> None:
        self._account_id = account_id
        self._api_id = api_id
        self._api_hash = api_hash
        self._session_string = session_string
        self._raw: TelegramClient | None = None
        self._port: TelethonClient | None = None

    @property
    def account_ids(self) -> tuple[str, ...]:
        return (self._account_id,)

    @property
    def clients(self) -> Mapping[str, TelegramClientPort]:
        if self._port is None:
            return {}
        return {self._account_id: self._port}

    @property
    def raw(self) -> TelegramClient:
        if self._raw is None:
            msg = 'pool не подключён (вызови connect_all)'
            raise RuntimeError(msg)
        return self._raw

    @property
    def connected(self) -> bool:
        return self._raw is not None

    async def connect_all(self) -> None:
        if self._raw is not None:
            return
        # Боевой connect-путь (с фиксом T030: get_me + catch_up для приёма
        # live-апдейтов). Контур тем самым проверяет именно его. Сырой
        # клиент достаём для драйва edit/delete, которых нет в порту.
        port = await connect_client(self._api_id, self._api_hash, self._session_string)
        self._port = port
        self._raw = port._client  # тестовый доступ к сырому клиенту для драйва

    async def disconnect_all(self) -> None:
        if self._raw is not None:
            await self._raw.disconnect()
            self._raw = None
            self._port = None


async def echo_processor(
    event: InboundEvent,
    ctx: PipelineContextData,
    svc: ProcessorServices,
) -> ProcessingResult:
    """
    Зеркалит входящее в цели с тегом вида события: ``NEW|EDIT|DEL <text>``.

    Текст входящего несёт уникальный nonce теста (драйвер кладёт его в
    исходное сообщение, реестр восстанавливает для удалений) — значит и
    доставленное содержит nonce, и ``cleanup`` уберёт обе стороны.
    """
    tag = _KIND_TAG[event.kind]
    text = f'{tag} {event.text or ""}'
    outbound = [
        OutboundMessage(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=text,
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


def _telegram_plugin(pool: IntegrationPool) -> AdapterPlugin:
    """Синтетический telegram-плагин: боевые listener/sender поверх ``pool``."""

    def make_listener(
        deps: AdapterDeps,
        _accounts: Mapping[str, object],
        sources: Sequence[EndpointConfig],
    ) -> TelegramListener:
        catchup = deps.settings.catchup
        return TelegramListener(
            ingest=deps.ingest,
            pool=pool,
            sources=sources,
            registry=deps.storage.registry,
            cursors=deps.storage.cursors,
            analytics=deps.storage.analytics,
            log=get_logger('integration.telegram.listener'),
            catchup_enabled=catchup.enabled,
            catchup_max_messages=catchup.max_messages_per_source,
            catchup_max_age_days=catchup.max_age_days,
            catchup_interval=catchup.interval,
            buffer_soft_limit=deps.settings.telegram.live_buffer_soft_limit,
        )

    def make_sender(
        deps: AdapterDeps, _accounts: Mapping[str, object]
    ) -> TelegramSender:
        sender_cfg = deps.settings.telegram.sender
        return TelegramSender(
            pool=pool,
            log=get_logger('integration.telegram.sender'),
            chat_per_second=sender_cfg.chat_per_second,
            account_per_minute=sender_cfg.account_per_minute,
            runtime_config=deps.storage.runtime_config,
        )

    return AdapterPlugin(
        name='telegram',
        capabilities=TELEGRAM_CAPABILITIES,
        account_config_model=TelegramAccountConfig,
        make_listener=make_listener,
        make_sender=make_sender,
    )


def build_plugins(pool: IntegrationPool) -> LoadedPlugins:
    """Реестр плагинов: синтетический telegram + реальные sqlite/persistqueue."""
    return LoadedPlugins(
        adapters={'telegram': _telegram_plugin(pool)},
        queues={'persistqueue': QUEUE_BACKEND},
        storages={'sqlite': STORAGE_BACKEND},
    )


def build_settings(
    *,
    account_id: str,
    api_id: int,
    api_hash: str,
    db_path: Path,
    queue_path: Path,
    pipelines: Mapping[str, PipelineConfig],
    catchup_enabled: bool = False,
    sender_chat_per_second: float | None = None,
) -> AngarionSettings:
    """
    Конфиг контура: один telegram-аккаунт, sqlite + persistqueue, реальный
    троттлинг (лимиты Telegram). ``catchup_enabled`` включается точечно в
    catch-up-сценариях, чтобы live-тесты не поднимали историю.
    ``sender_chat_per_second`` (например, ~0) «замораживает» доставку для
    сценария рестарта с непустой очередью.
    """
    payload: dict[str, Any] = {
        'accounts': {
            account_id: {
                'messenger': 'telegram',
                'api_id': api_id,
                'api_hash': api_hash,
            }
        },
        'storage': {'backend': 'sqlite', 'path': str(db_path)},
        'queue': {'backend': 'persistqueue', 'path': str(queue_path)},
        'catchup': {'enabled': catchup_enabled},
        'worker': {'poll_interval': 0.2},
        'pipelines': dict(pipelines),
    }
    if sender_chat_per_second is not None:
        payload['telegram'] = {'sender': {'chat_per_second': sender_chat_per_second}}
    return AngarionSettings.model_validate(payload)


@asynccontextmanager
async def idle_client(
    api_id: int, api_hash: str, session_string: str
) -> AsyncIterator[TelegramClient]:
    """
    Короткоживущий Telethon-клиент для драйва в простое (app остановлен).

    Используется в catch-up-сценариях: пока listener выключен и pool
    отключён, правки/удаления/публикации в источник вносятся этим
    клиентом — он не конкурирует с listener'ом за апдейты (тот не
    подписан). Последовательность гарантируется тестом.
    """
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    await client.connect()
    try:
        yield client
    finally:
        await client.disconnect()


def mirror_pipeline(
    *,
    account_id: str,
    source_chat: str,
    target_chats: Sequence[str],
    processor: str = 'integration_echo',
    events: frozenset[Any] | None = None,
) -> PipelineConfig:
    """Пайплайн «зеркало»: один источник → одна-несколько целей."""
    kinds = events if events is not None else frozenset(EventKind)
    return PipelineConfig(
        processor=processor,
        events=kinds,
        sources=(EndpointConfig(account=account_id, chat_id=source_chat),),
        targets=tuple(
            EndpointConfig(account=account_id, chat_id=chat) for chat in target_chats
        ),
    )


async def post(client: TelegramClient, chat_id: int, text: str) -> int:
    """Опубликовать сообщение в чат (драйвер); вернуть message_id."""
    message = await client.send_message(chat_id, text)
    return int(message.id)


async def edit(
    client: TelegramClient, chat_id: int, message_id: int, text: str
) -> None:
    """Отредактировать сообщение драйвера."""
    await client.edit_message(chat_id, message_id, text)


async def delete(
    client: TelegramClient, chat_id: int, message_ids: int | list[int]
) -> None:
    """Удалить сообщение(я)."""
    await client.delete_messages(chat_id, message_ids)


async def read_texts(
    client: TelegramClient, chat_id: int, *, limit: int = 30
) -> list[str]:
    """Тексты последних сообщений чата (новые→старые)."""
    texts: list[str] = []
    async for message in client.iter_messages(chat_id, limit=limit):
        texts.append(message.text or '')
    return texts


async def wait_for(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout: float = 30.0,
    interval: float = 1.0,
) -> bool:
    """Поллинг условия с таймаутом; True — выполнилось, False — таймаут."""
    elapsed = 0.0
    while elapsed < timeout:
        if await predicate():
            return True
        await asyncio.sleep(interval)
        elapsed += interval
    return False


async def purge_recent(
    client: TelegramClient, chat_ids: Sequence[int], *, limit: int = 80
) -> None:
    """
    Удалить недавние сообщения в выделенных тестовых группах (чистый старт).

    Контур опирается на catch-up (``iter_messages``): на непустой группе
    первый прогон эмитил бы всю недавнюю историю как «новое». Чистка перед
    сценарием делает источник детерминированным — остаются только
    сообщения теста. Группы — выделенные (``angarion 1/2/3``), удаление
    всего недавнего безопасно.
    """
    for chat_id in chat_ids:
        ids = [message.id async for message in client.iter_messages(chat_id, limit=limit)]
        if ids:
            await client.delete_messages(chat_id, ids)


async def cleanup(client: TelegramClient, chat_ids: Sequence[int], nonce: str) -> None:
    """Удалить из групп все сообщения теста (по nonce в тексте), best-effort."""
    for chat_id in chat_ids:
        stale: list[int] = []
        async for message in client.iter_messages(chat_id, limit=100):
            if message.text and nonce in message.text:
                stale.append(message.id)
        if stale:
            await client.delete_messages(chat_id, stale)


async def delivered(client: TelegramClient, chat_id: int, needle: str) -> bool:
    """True — среди последних сообщений цели есть содержащее ``needle``."""
    texts = await read_texts(client, chat_id)
    return any(needle in text for text in texts)


def count_with(texts: Sequence[str], needle: str) -> int:
    """Сколько текстов содержат подстроку (для проверок «ровно N»)."""
    return sum(1 for text in texts if needle in text)


__all__ = [
    'ECHO_PROCESSOR',
    'IntegrationPool',
    'build_app',
    'build_plugins',
    'build_settings',
    'cleanup',
    'count_with',
    'delete',
    'delivered',
    'echo_processor',
    'edit',
    'idle_client',
    'mirror_pipeline',
    'post',
    'purge_recent',
    'read_texts',
    'wait_for',
]
