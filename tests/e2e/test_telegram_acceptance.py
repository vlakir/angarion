"""
Acceptance-сценарий M3 (T005, фаза 6; §16 M3 «простой → правки/удаления
→ рестарт»).

Поднимает **весь** конвейер через настоящий composition root
(``build_app``) на реальных driven-бэкендах — sqlite (storage) и memory
(queue) — и подменяет лишь границу Telethon: синтетический
telegram-плагин собирает настоящие ``TelegramListener``/``TelegramSender``
поверх in-memory fake-клиента (``TelegramClientPort``). Так проверяется
сам адаптер (резолв, live-подписки, catch-up §9.3, sender), а не его
заглушки.

Сценарий целиком:

1. **Live-работа.** Приходят два новых сообщения live → конвейер
   доставляет два ``NEW`` в цель.
2. **Простой.** ``app.stop()`` — процесс «выключен».
3. **Правки/удаления при простое.** На «сервере» (история fake-клиента)
   за это время: одно сообщение отредактировано, одно удалено, одно
   новое опубликовано.
4. **Рестарт.** Свежий ``build_app`` на **том же** sqlite-файле (реестр и
   курсоры переживают перезапуск): catch-up §9.3 сверяет историю с
   реестром и эмитит ``EDITED`` (с previous_text), ``NEW``
   и ``DELETED`` (с восстановленным из реестра текстом) → все три
   доходят до цели.

Суточный прогон (N4 спеки) — ручной, на личном тестовом аккаунте
(``tests/integration`` + ``scripts/tg_login.py``); автоматизации в CI не
подлежит и здесь не воспроизводится.
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
from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
)
from angarion.domain.models import (
    OutboundRecord,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Record,
    RecordKind,
    Verdict,
)
from angarion.domain.plugin import AdapterPlugin
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import (
        AsyncIterator,
        Callable,
        Mapping,
        Sequence,
    )
    from pathlib import Path

    from angarion.adapters.telegram.client import (
        RawDeletionHandler,
        RawMessageHandler,
        RawTelegramDeletion,
    )
    from angarion.bootstrap import AdapterDeps, AngarionApp

ACCOUNT = 'main'
SRC_CHAT = '-100123'  # источник; резолвится в одноимённый числовой peer
DST_CHAT = '-100222'  # цель доставки
SRC_PEER = -100123
ALL_KINDS = frozenset(
    {RecordKind.NEW, RecordKind.EDITED, RecordKind.DELETED}
)


class _FakeTelegramClient:
    """
    In-memory ``TelegramClientPort``: «сервер» (история) + live-fire +
    журнал отправок. Семантика ``fetch_history`` повторяет боевую обёртку
    (новые→старые, ``id > min_id``, до ``limit``).
    """

    def __init__(self, *, peer_ids: dict[str, int]) -> None:
        self._peer_ids = dict(peer_ids)
        self._history: dict[int, dict[int, RawTelegramMessage]] = {}
        self.sent: list[dict[str, object]] = []
        self._on_new: list[RawMessageHandler] = []
        self._on_edit: list[RawMessageHandler] = []
        self._on_delete: list[RawDeletionHandler] = []
        self._next_id = 5000

    async def warm_entity_cache(self) -> None:
        return

    async def resolve_peer(self, peer: str) -> int:
        return self._peer_ids[peer]

    async def fetch_history(
        self,
        chat_id: int,
        *,
        limit: int,
        thread_id: int | None = None,
        min_id: int = 0,
    ) -> AsyncIterator[RawTelegramMessage]:
        ordered = sorted(
            self._history.get(chat_id, {}).values(),
            key=lambda m: m.message_id,
            reverse=True,
        )
        yielded = 0
        for raw in ordered:
            if raw.message_id <= min_id:
                continue
            if thread_id is not None and raw.thread_id != thread_id:
                continue
            if yielded >= limit:
                break
            yielded += 1
            yield raw

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
        self.sent.append({'chat_id': chat_id, 'text': text})
        self._next_id += 1
        return self._next_id

    def on_new_message(self, handler: RawMessageHandler) -> None:
        self._on_new.append(handler)

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        self._on_edit.append(handler)

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        self._on_delete.append(handler)

    async def fire_new(self, raw: RawTelegramMessage) -> None:
        """Live-приход нового сообщения (events.NewMessage)."""
        for handler in self._on_new:
            await handler(raw)

    def server_upsert(self, raw: RawTelegramMessage) -> None:
        """Состояние «сервера» за время простоя: публикация/правка."""
        self._history.setdefault(raw.chat_id, {})[raw.message_id] = raw


class _FakePool:
    """``ClientPool`` поверх фиксированной мапы fake-клиентов."""

    def __init__(self, clients: dict[str, _FakeTelegramClient]) -> None:
        self._clients = dict(clients)

    @property
    def account_ids(self) -> tuple[str, ...]:
        return tuple(self._clients)

    @property
    def clients(self) -> dict[str, _FakeTelegramClient]:
        return self._clients

    async def connect_all(self) -> None:
        return

    async def disconnect_all(self) -> None:
        return


async def annotate(
    event: Record, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    """Свидетель обогащения: текст исходящего = kind|text|previous_text."""
    text = f'{event.kind}|{event.text}|{event.previous_text}'
    outbound = [
        OutboundRecord(
            idempotency_key=svc.make_idempotency_key(event, spec.target, n),
            target=spec.target,
            send_via=spec.send_via,
            text=text,
        )
        for n, spec in enumerate(ctx.targets)
    ]
    return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)


@pytest.fixture
def sandbox_processors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Изолированная копия реестра процессоров: тестовое имя не утекает."""
    monkeypatch.setattr(processors, '_registry', dict(processors.registered()))
    processors.register(FunctionProcessor(name='annotate', fn=annotate))


def _raw(message_id: int, text: str, *, kind: RecordKind = RecordKind.NEW) -> RawTelegramMessage:
    return RawTelegramMessage(
        kind=kind,
        chat_id=SRC_PEER,
        message_id=message_id,
        text=text,
        sender_id=777,
        sender_name='Алиса',
        event_at=datetime.now(UTC),
    )


def _telegram_plugin(pool: _FakePool) -> AdapterPlugin:
    """Синтетический telegram-плагин: настоящие listener/sender поверх ``pool``."""

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
            log=get_logger('test.telegram.listener'),
            catchup_enabled=catchup.enabled,
            catchup_max_messages=catchup.max_messages_per_source,
            catchup_max_age_days=catchup.max_age_days,
            catchup_interval=catchup.interval,
            buffer_soft_limit=deps.settings.telegram.live_buffer_soft_limit,
        )

    def make_sender(
        deps: AdapterDeps, _accounts: Mapping[str, object]
    ) -> TelegramSender:
        # Пороги троттлинга подняты, чтобы acceptance не упирался в паузы
        # token-bucket (поведение бакетов проверяют юнит-тесты sender'а).
        return TelegramSender(
            pool=pool,
            log=get_logger('test.telegram.sender'),
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


def _plugins(pool: _FakePool) -> LoadedPlugins:
    """Реестр плагинов: синтетический telegram + реальные sqlite/memory."""
    return LoadedPlugins(
        adapters={'telegram': _telegram_plugin(pool)},
        queues={'memory': QUEUE_BACKEND},
        storages={'sqlite': STORAGE_BACKEND},
    )


def _settings(db_path: Path) -> AngarionSettings:
    """Конфиг acceptance: telegram-аккаунт, sqlite-файл, мгновенные ретраи."""
    return AngarionSettings.model_validate(
        {
            'accounts': {
                ACCOUNT: {
                    'transport': 'telegram',
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
                'mirror': PipelineConfig(
                    processor='annotate',
                    events=ALL_KINDS,
                    sources=(EndpointConfig(account=ACCOUNT, address=SRC_CHAT),),
                    targets=(EndpointConfig(account=ACCOUNT, address=DST_CHAT),),
                )
            },
        }
    )


async def _wait_until(condition: Callable[[], bool], timeout: float = 5.0) -> None:
    """Дождаться условия с шагом 10 мс; таймаут — диагностический fail."""
    for _ in range(int(timeout / 0.01)):
        if condition():
            return
        await asyncio.sleep(0.01)
    pytest.fail('условие не выполнено за отведённый таймаут')


def _delivered_texts(client: _FakeTelegramClient) -> list[str]:
    return [str(sent['text']) for sent in client.sent]


class TestTelegramAcceptance:
    """§16 M3: простой → правки/удаления → рестарт, сквозь composition root."""

    async def test_idle_then_edits_deletes_then_restart(
        self, sandbox_processors: None, tmp_path: Path
    ) -> None:
        client = _FakeTelegramClient(peer_ids={SRC_CHAT: SRC_PEER})
        pool = _FakePool({ACCOUNT: client})
        db_path = tmp_path / 'app.db'
        settings = _settings(db_path)
        plugins = _plugins(pool)

        # --- 1. Live-работа: два новых сообщения доходят до цели ---
        app: AngarionApp = build_app(settings, plugins=plugins)
        await app.start()
        try:
            await client.fire_new(_raw(10, 'v1'))
            await client.fire_new(_raw(11, 'will-be-deleted'))
            await _wait_until(lambda: len(client.sent) >= 2)
        finally:
            await app.stop()  # 2. Простой

        live_texts = set(_delivered_texts(client))
        assert live_texts == {
            'new|v1|None',
            'new|will-be-deleted|None',
        }
        before_restart = len(client.sent)

        # --- 3. Правки/удаления при простое (состояние «сервера») ---
        client.server_upsert(_raw(10, 'v2'))  # сообщение 10 отредактировано
        client.server_upsert(_raw(12, 'fresh'))  # сообщение 12 опубликовано
        # сообщение 11 на сервер не возвращаем — оно удалено

        # --- 4. Рестарт: свежая сборка на том же app.db; catch-up §9.3 ---
        restarted: AngarionApp = build_app(settings, plugins=plugins)
        await restarted.start()
        try:
            await _wait_until(lambda: len(client.sent) - before_restart >= 3)
        finally:
            await restarted.stop()

        catchup_texts = set(_delivered_texts(client)[before_restart:])
        assert catchup_texts == {
            'edited|v2|v1',  # правка: previous_text из реестра
            'new|fresh|None',  # новое за время простоя
            'deleted|will-be-deleted|None',  # удаление: текст восстановлен
        }
