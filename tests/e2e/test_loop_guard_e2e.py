"""
Петлевой guard сквозь composition root (T009 / M6, фаза 1): пайплайн,
у которого цель совпадает с источником (``source == target``), не
зацикливается на собственных доставках.

Поднимает весь конвейер через ``build_app`` на реальных backend'ах
(sqlite storage, memory queue) и подменяет границу Telethon fake-клиентом,
который **эхом** возвращает каждую отправку как новое входящее сообщение —
так воспроизводится поведение Telegram, отдающего наше же доставленное
сообщение обратно в группу-источник. Без guard'а это даёт бесконечную
петлю; с guard'ом dedup-пометка произведённого ``external_id`` гасит
возврат как ``duplicate``, и доставка ровно одна.

Чаты сконфигурированы числовым id (``-100…``): guard сверяет идентичность
источника с целью по chat_id как в конфиге, а маппер входящих строит
ключ по ``str(raw.chat_id)`` — для числовых id они совпадают (см. ADR).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

from angarion.adapters.memory.plugin import QUEUE_BACKEND
from angarion.adapters.storage.plugin import STORAGE_BACKEND
from angarion.adapters.telegram.client import RawTelegramMessage
from angarion.adapters.telegram.listener import TelegramListener
from angarion.adapters.telegram.plugin import (
    TELEGRAM_CAPABILITIES,
    TelegramAccountConfig,
)
from angarion.adapters.telegram.sender import TelegramSender
from angarion.application import processors
from angarion.application.processors import FunctionProcessor
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
    from collections.abc import AsyncIterator, Mapping, Sequence
    from pathlib import Path

    from angarion.adapters.telegram.client import RawDeletionHandler, RawMessageHandler
    from angarion.bootstrap import AdapterDeps, AngarionApp
    from angarion.domain.models import (
        InboundEvent,
        PipelineContextData,
        ProcessorServices,
    )

ACCOUNT = 'main'
CHAT = '-100555'  # один чат: одновременно источник и цель
PEER = -100555
ALL_KINDS = frozenset({EventKind.MESSAGE_NEW, EventKind.MESSAGE_EDITED})


class _EchoingClient:
    """Fake ``TelegramClientPort``: отправка эхом возвращается как новое."""

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._on_new: list[RawMessageHandler] = []
        self._next_id = 5000
        self._echoes: list[asyncio.Task[None]] = []

    async def warm_entity_cache(self) -> None:
        return

    async def resolve_peer(self, peer: str) -> int:
        return PEER

    async def fetch_history(
        self,
        chat_id: int,
        *,
        limit: int,
        thread_id: int | None = None,
        min_id: int = 0,
    ) -> AsyncIterator[RawTelegramMessage]:
        return
        yield  # pragma: no cover — пустая история, catch-up здесь не цель

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to: int | None = None,
        parse_mode: str | None = None,
        silent: bool = False,
        link_preview: bool = True,
    ) -> int:
        self.sent.append(text)
        self._next_id += 1
        message_id = self._next_id
        # Telegram отдаёт нашу же доставку обратно в группу как новое
        # сообщение — но спустя сетевой RTT, уже после того как sender
        # вернул receipt и guard пометил dedup. Эмулируем эту задержку,
        # иначе тест проверял бы недостижимое в бою синхронное эхо.
        echo = RawTelegramMessage(
            kind=EventKind.MESSAGE_NEW,
            chat_id=PEER,
            message_id=message_id,
            text=text,
            sender_id=42,
            sender_name='self',
            event_at=datetime.now(UTC),
        )
        self._echoes.append(asyncio.create_task(self._deliver_echo(echo)))
        return message_id

    async def _deliver_echo(self, echo: RawTelegramMessage) -> None:
        await asyncio.sleep(0.05)
        for handler in self._on_new:
            await handler(echo)

    async def cancel_echoes(self) -> None:
        for task in self._echoes:
            task.cancel()

    def on_new_message(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        return

    async def fire_new(self, raw: RawTelegramMessage) -> None:
        for handler in self._on_new:
            await handler(raw)


class _Pool:
    def __init__(self, client: _EchoingClient) -> None:
        self._client = client

    @property
    def account_ids(self) -> tuple[str, ...]:
        return (ACCOUNT,)

    @property
    def clients(self) -> dict[str, _EchoingClient]:
        return {ACCOUNT: self._client}

    async def connect_all(self) -> None:
        return

    async def disconnect_all(self) -> None:
        await self._client.cancel_echoes()


async def _mirror(
    event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    """Доставляет текст входящего в цель (которая = источник)."""
    outbound = [
        OutboundMessage(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=event.text or '',
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


@pytest.fixture
def sandbox_processors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(processors, '_registry', dict(processors.registered()))
    processors.register(FunctionProcessor(name='mirror', fn=_mirror))


def _settings(db_path: Path) -> AngarionSettings:
    return AngarionSettings.model_validate(
        {
            'accounts': {
                ACCOUNT: {
                    'messenger': 'telegram',
                    'api_id': 12345,
                    'api_hash': 'deadbeefcafe',
                }
            },
            'storage': {'backend': 'sqlite', 'path': str(db_path)},
            'queue': {'backend': 'memory'},
            'worker': {
                'max_retries': 3,
                'backoff_base': 0.001,
                'backoff_cap': 0.002,
                'poll_interval': 0.01,
            },
            'pipelines': {
                'self_mirror': PipelineConfig(
                    processor='mirror',
                    events=ALL_KINDS,
                    sources=(EndpointConfig(account=ACCOUNT, chat_id=CHAT),),
                    targets=(EndpointConfig(account=ACCOUNT, chat_id=CHAT),),
                )
            },
        }
    )


def _plugin(pool: _Pool) -> AdapterPlugin:
    def make_listener(
        deps: AdapterDeps,
        _accounts: Mapping[str, object],
        sources: Sequence[EndpointConfig],
    ) -> TelegramListener:
        return TelegramListener(
            ingest=deps.ingest,
            pool=pool,
            sources=sources,
            registry=deps.storage.registry,
            cursors=deps.storage.cursors,
            analytics=deps.storage.analytics,
            log=get_logger('test.loopguard.listener'),
            catchup_enabled=False,
        )

    def make_sender(
        deps: AdapterDeps, _accounts: Mapping[str, object]
    ) -> TelegramSender:
        return TelegramSender(
            pool=pool,
            log=get_logger('test.loopguard.sender'),
            chat_per_second=1000.0,
            account_per_minute=100000.0,
        )

    return AdapterPlugin(
        name='telegram',
        capabilities=TELEGRAM_CAPABILITIES,
        account_config_model=TelegramAccountConfig,
        make_listener=make_listener,
        make_sender=make_sender,
    )


def _plugins(pool: _Pool) -> LoadedPlugins:
    return LoadedPlugins(
        adapters={'telegram': _plugin(pool)},
        queues={'memory': QUEUE_BACKEND},
        storages={'sqlite': STORAGE_BACKEND},
    )


async def _wait_until(condition: object, timeout: float = 2.0) -> None:
    for _ in range(int(timeout / 0.01)):
        if condition():  # type: ignore[operator]
            return
        await asyncio.sleep(0.01)


def _raw(message_id: int, text: str) -> RawTelegramMessage:
    return RawTelegramMessage(
        kind=EventKind.MESSAGE_NEW,
        chat_id=PEER,
        message_id=message_id,
        text=text,
        sender_id=777,
        sender_name='driver',
        event_at=datetime.now(UTC),
    )


async def test_source_equals_target_does_not_loop(
    sandbox_processors: None, tmp_path: Path
) -> None:
    """Одно входящее в совмещённой группе → ровно одна доставка, без петли."""
    client = _EchoingClient()
    pool = _Pool(client)
    settings = _settings(tmp_path / 'app.db')
    plugins = _plugins(pool)

    app: AngarionApp = build_app(settings, plugins=plugins)
    await app.start()
    try:
        await client.fire_new(_raw(10, 'привет'))
        await _wait_until(lambda: len(client.sent) >= 1)
        # дать петле шанс проявиться, будь guard сломан
        await asyncio.sleep(0.2)
    finally:
        await app.stop()

    assert client.sent == ['привет'], (
        f'ожидалась ровно одна доставка, получено: {client.sent}'
    )
