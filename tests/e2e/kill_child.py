"""
Дочерний процесс kill-теста (FR-12 спеки T003): конвейер на
персистентных адаптерах (persistqueue + sqlite) с синтетической
платформой «filesink».

Поведение:

- на КАЖДОМ старте listener ре-эмитит все события 0..N-1 (модель
  catch-up §9: источник повторяет, дедуп фильтрует);
- sink дописывает idempotency_key каждой доставки строкой в
  ``delivered.log`` (append + flush — переживает SIGKILL);
- инструментированные kill-точки (env ``KILL_POINT``) роняют процесс
  ``SIGKILL``-ом самому себе в детерминированных местах конвейера.

Env: ``KILL_DATA_DIR`` (каталог app.db / queue.db / delivered.log),
``KILL_POINT`` ('' | between_put_and_mark | before_ack | after_send),
``KILL_TARGET`` (external_id события-мишени), ``KILL_EVENT_COUNT``.
Имена без префикса ``ANGARION_`` — чтобы не пересекаться с
env-источником настроек.
"""

from __future__ import annotations

import asyncio
import os
import signal
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from angarion.adapters.queue.persistqueue_ import QUEUE_BACKEND as PQ_BACKEND
from angarion.adapters.storage.plugin import STORAGE_BACKEND as SQLITE_BACKEND
from angarion.bootstrap import LoadedPlugins, build_app
from angarion.config import AngarionSettings
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.errors import NotSupportedError
from angarion.domain.keys import make_dedup_key, make_source_key, normalize_and_hash
from angarion.domain.models import (
    AccountRef,
    Address,
    DeliveryReceipt,
    EventKind,
    InboundEvent,
    QueueEnvelope,
    QueueItem,
)
from angarion.domain.plugin import (
    AdapterPlugin,
    QueueBackend,
    StorageBackend,
    StorageBundle,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from pydantic import AwareDatetime

    from angarion.bootstrap import AdapterDeps
    from angarion.config import EndpointConfig, QueueConfig, StorageConfig
    from angarion.domain.models import OutboundMessage, QueueDepth
    from angarion.domain.ports import DedupStorePort, EventQueuePort

MESSENGER: Final = 'filesink'
ACCOUNT: Final = 'acc'
SRC_CHAT: Final = '-1'
DST_CHAT: Final = '-2'
PIPELINE: Final = 'relay'
EMIT_PACE_SECONDS: Final = 0.01
DELIVERED_LOG: Final = 'delivered.log'

POINT_BETWEEN_PUT_AND_MARK: Final = 'between_put_and_mark'
POINT_BEFORE_ACK: Final = 'before_ack'
POINT_AFTER_SEND: Final = 'after_send'


def expected_keys(count: int) -> set[str]:
    """Ключи идемпотентности всех ожидаемых доставок (паритет с worker)."""
    source_key = make_source_key(MESSENGER, ACCOUNT, SRC_CHAT)
    return {
        f'{make_dedup_key(EventKind.MESSAGE_NEW, source_key, str(i))}'
        f'->{PIPELINE}:{DST_CHAT}:0'
        for i in range(count)
    }


def target_key(target: str) -> str:
    """Ключ доставки события-мишени kill-точки."""
    source_key = make_source_key(MESSENGER, ACCOUNT, SRC_CHAT)
    dedup = make_dedup_key(EventKind.MESSAGE_NEW, source_key, target)
    return f'{dedup}->{PIPELINE}:{DST_CHAT}:0'


def _die() -> None:
    """SIGKILL самому себе: «питание выдернули» ровно в этой точке."""
    os.kill(os.getpid(), signal.SIGKILL)


def _build_events(count: int) -> list[InboundEvent]:
    source_key = make_source_key(MESSENGER, ACCOUNT, SRC_CHAT)
    now = datetime.now(UTC)
    return [
        InboundEvent(
            uid=uuid4(),
            kind=EventKind.MESSAGE_NEW,
            dedup_key=make_dedup_key(EventKind.MESSAGE_NEW, source_key, str(i)),
            origin='live',
            source=Address(messenger=MESSENGER, chat_id=SRC_CHAT),
            received_by=AccountRef(messenger=MESSENGER, account_id=ACCOUNT),
            external_id=str(i),
            text=f'msg-{i}',
            content_hash=normalize_and_hash(f'msg-{i}'),
            event_at=now,
            received_at=now,
        )
        for i in range(count)
    ]


class ReplayListener:
    """Listener: ре-эмит всех событий на каждом старте (модель catch-up)."""

    def __init__(self, ingest: object, events: list[InboundEvent]) -> None:
        self._ingest = ingest
        self._events = events
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._replay())

    async def _replay(self) -> None:
        for event in self._events:
            await self._ingest.ingest(event)  # type: ignore[attr-defined]
            await asyncio.sleep(EMIT_PACE_SECONDS)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()

    async def catchup(self, source_key: str) -> None:
        msg = f'история filesink недоступна: {source_key}'
        raise NotSupportedError(msg)


class FileSink:
    """Доставка = строка idempotency_key в delivered.log (append, flush)."""

    def __init__(self, path: Path, *, kill_key: str | None) -> None:
        self._path = path
        self._kill_key = kill_key

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        with self._path.open('a', encoding='utf-8') as log:
            log.write(msg.idempotency_key + '\n')
            log.flush()
        if self._kill_key is not None and msg.idempotency_key == self._kill_key:
            _die()  # send выполнен, mark_sent не записан — окно C-9
        return DeliveryReceipt(external_id=None, delivered_at=datetime.now(UTC))


class KillingDedup:
    """Обёртка dedup: SIGKILL перед отметкой мишени (окно put → mark, A-11)."""

    def __init__(self, inner: DedupStorePort, *, kill_key: str | None) -> None:
        self._inner = inner
        self._kill_key = kill_key

    async def seen(self, dedup_key: str) -> bool:
        return await self._inner.seen(dedup_key)

    async def mark_inbound(self, dedup_key: str) -> bool:
        if self._kill_key is not None and dedup_key == self._kill_key:
            _die()  # fan-out выполнен, отметка не записана
        return await self._inner.mark_inbound(dedup_key)

    async def prune(self, older_than: AwareDatetime) -> int:
        return await self._inner.prune(older_than)


class KillingQueue:
    """Обёртка очереди: SIGKILL перед ack envelope-мишени (после outbox)."""

    def __init__(self, inner: EventQueuePort, *, kill_id: str | None) -> None:
        self._inner = inner
        self._kill_id = kill_id

    async def put(self, item: QueueEnvelope) -> None:
        await self._inner.put(item)

    async def get(self) -> QueueItem:
        return await self._inner.get()

    async def ack(self, item: QueueItem) -> None:
        if (
            self._kill_id is not None
            and item.envelope.event.external_id == self._kill_id
        ):
            _die()  # обработка зафиксирована в outbox, ack не записан
        await self._inner.ack(item)

    async def nack(self, item: QueueItem) -> None:
        await self._inner.nack(item)

    async def recover(self) -> int:
        return await self._inner.recover()

    async def depth(self) -> QueueDepth:
        return await self._inner.depth()


class FilesinkAccountConfig(BaseModel):
    """Секция ``[accounts.*]`` платформы filesink: только ``messenger``."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    messenger: Literal['filesink']


def make_settings(data_dir: Path) -> AngarionSettings:
    return AngarionSettings.model_validate(
        {
            'accounts': {ACCOUNT: {'messenger': MESSENGER}},
            'storage': {'backend': 'sqlite', 'path': str(data_dir / 'app.db')},
            'queue': {
                'backend': 'persistqueue',
                'path': str(data_dir / 'queue.db'),
            },
            'worker': {'poll_interval': 0.05, 'backoff_base': 0.1},
            'catchup': {'enabled': False},
            'pipelines': {
                PIPELINE: {
                    'processor': 'passthrough',
                    'events': ['message_new'],
                    'sources': [{'account': ACCOUNT, 'chat_id': SRC_CHAT}],
                    'targets': [{'account': ACCOUNT, 'chat_id': DST_CHAT}],
                }
            },
        }
    )


def make_plugins(
    data_dir: Path, point: str, target: str, count: int
) -> LoadedPlugins:
    """Реестры с инструментированными kill-точками поверх боевых бэкендов."""
    source_key = make_source_key(MESSENGER, ACCOUNT, SRC_CHAT)
    target_dedup = make_dedup_key(EventKind.MESSAGE_NEW, source_key, target)
    events = _build_events(count)

    def make_listener(
        deps: AdapterDeps,
        _accounts: Mapping[str, BaseModel],
        _sources: Sequence[EndpointConfig],
    ) -> ReplayListener:
        return ReplayListener(deps.ingest, events)

    def make_sender(
        _deps: AdapterDeps, _accounts: Mapping[str, BaseModel]
    ) -> FileSink:
        kill_key = target_key(target) if point == POINT_AFTER_SEND else None
        return FileSink(data_dir / DELIVERED_LOG, kill_key=kill_key)

    def make_queue(config: QueueConfig) -> KillingQueue:
        kill_id = target if point == POINT_BEFORE_ACK else None
        return KillingQueue(PQ_BACKEND.make(config), kill_id=kill_id)

    def make_storage(config: StorageConfig) -> StorageBundle:
        bundle = SQLITE_BACKEND.make(config)
        kill_key = target_dedup if point == POINT_BETWEEN_PUT_AND_MARK else None
        return StorageBundle(
            dedup=KillingDedup(bundle.dedup, kill_key=kill_key),
            outbox=bundle.outbox,
            registry=bundle.registry,
            cursors=bundle.cursors,
            state=bundle.state,
            analytics=bundle.analytics,
            dead_letters=bundle.dead_letters,
            session=bundle.session,
            runtime_config=bundle.runtime_config,
            command_outbox=bundle.command_outbox,
        )

    plugin = AdapterPlugin(
        name=MESSENGER,
        capabilities=AdapterCapabilities(
            user_account=True,
            edit_events=True,
            delete_events=True,
            history_fetch=False,
            threads=False,
            push_transport='client',
        ),
        account_config_model=FilesinkAccountConfig,
        make_listener=make_listener,
        make_sender=make_sender,
    )
    return LoadedPlugins(
        adapters={MESSENGER: plugin},
        queues={'persistqueue': QueueBackend(name='persistqueue', make=make_queue)},
        storages={'sqlite': StorageBackend(name='sqlite', make=make_storage)},
    )


async def main() -> None:
    data_dir = Path(os.environ['KILL_DATA_DIR'])
    point = os.environ.get('KILL_POINT', '')
    target = os.environ.get('KILL_TARGET', '25')
    count = int(os.environ.get('KILL_EVENT_COUNT', '40'))
    data_dir.mkdir(parents=True, exist_ok=True)
    settings = make_settings(data_dir)
    app = build_app(settings, plugins=make_plugins(data_dir, point, target, count))
    await app.start()
    await asyncio.Event().wait()  # работаем до SIGKILL родителя


if __name__ == '__main__':
    asyncio.run(main())
