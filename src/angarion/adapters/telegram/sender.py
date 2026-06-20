"""
TelegramSender (FR «Sender», M3, фаза 4): ``SinkPort`` поверх
границы Telethon с троттлингом и устойчивой отправкой.

Перед отправкой — token-bucket троттлинг per (account, chat): бакет чата
(≤ 1 msg/s) и бакет аккаунта (≤ 20/min), пороги конфигурируемы, всё на
``asyncio.sleep`` (event loop не блокируется). Отправка устойчива к двум
видам ошибок границы (``client.py``):

- ``FloodWaitError`` — честно пережидаем ``seconds`` и **повторяем то же
  сообщение** (без пропуска, не как tgcf); число повторов ограничено
  (страховка от вечного цикла), при исчерпании — пробрасываем.
- ``TransientSendError`` (сеть/таймаут/5xx) — ретраи с экспоненциальным
  backoff (tenacity); при исчерпании — пробрасываем.

Проброшенное исключение ловит ``DeliveryWorker`` (§8): reschedule с
backoff, после ``max_retries`` — терминальный ``failed``; сообщение не
теряется. Telegram-специфику (``parse_mode``/``silent``/
``disable_preview``) sender читает из ``OutboundRecord.extra`` — ядро
поле не интерпретирует.

Часы/сон инъектируются (тесты — детерминированно, без реальных пауз).
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from angarion.adapters.telegram.client import (
    FloodWaitError,
    TransientSendError,
    as_peer,
)
from angarion.adapters.telegram.throttle import TokenBucket
from angarion.domain.models import DeliveryReceipt

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.telegram.client import TelegramClientPort
    from angarion.adapters.telegram.registry import ClientPool
    from angarion.domain.models import OutboundRecord
    from angarion.domain.ports import RuntimeConfigPort


class TelegramSendOptions(BaseModel):
    """
    Telegram-специфика из ``OutboundRecord.extra`` (ядро не трактует).

    ``extra='ignore'`` — незнакомые ключи extra не ломают отправку.
    """

    model_config = ConfigDict(frozen=True, extra='ignore')

    parse_mode: str | None = None
    silent: bool = False
    disable_preview: bool = False


class TelegramSender:
    """``SinkPort``: троттлинг + FloodWait-повтор + transient-ретраи."""

    def __init__(
        self,
        *,
        pool: ClientPool,
        log: FilteringBoundLogger,
        chat_per_second: float = 1.0,
        account_per_minute: float = 20.0,
        flood_max_retries: int = 5,
        transient_max_attempts: int = 3,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        runtime_config: RuntimeConfigPort | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not pool.account_ids:
            msg = 'нужен хотя бы один клиент Telegram'
            raise ValueError(msg)
        self._pool = pool
        self._log = log
        self._runtime_config = runtime_config
        # файловые пороги (TOML+env) — база, поверх которой ложится
        # динамический override §12.8; ``_chat_per_second`` — текущий
        # эффективный темп (override либо файл при отсутствии override'а)
        self._file_chat_per_second = chat_per_second
        self._file_account_per_minute = account_per_minute
        self._chat_per_second = chat_per_second
        self._account_per_minute = account_per_minute
        self._flood_max_retries = flood_max_retries
        self._transient_max_attempts = transient_max_attempts
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._clock = clock
        self._sleep = sleep
        # клиенты пула подключаются позже (listener.start, §12.1); бакеты
        # известны заранее по списку аккаунтов
        self._account_buckets = {
            account_id: TokenBucket(
                rate=account_per_minute / 60.0,
                capacity=max(account_per_minute, 1.0),
                clock=clock,
                sleep=sleep,
            )
            for account_id in pool.account_ids
        }
        self._chat_buckets: dict[tuple[str, str], TokenBucket] = {}

    async def send(self, record: OutboundRecord) -> DeliveryReceipt:
        """Троттлинг → устойчивая отправка → ``DeliveryReceipt`` (UTC)."""
        account_id = record.send_via.account_id
        client = self._pool.clients[account_id]
        opts = TelegramSendOptions.model_validate(record.extra)
        await self._apply_dynamic_limits()
        await self._throttle(account_id, record.target.address)
        reply_to = (
            int(record.target.thread_id)
            if record.target.thread_id is not None
            else None
        )
        peer = as_peer(record.target.address)
        do_send = self._send_op(
            client=client, peer=peer, record=record, reply_to=reply_to, opts=opts
        )
        message_id = await self._send_resilient(do_send=do_send, peer=peer)
        return DeliveryReceipt(
            external_id=str(message_id), delivered_at=datetime.now(UTC)
        )

    @staticmethod
    def _send_op(
        *,
        client: TelegramClientPort,
        peer: int | str,
        record: OutboundRecord,
        reply_to: int | None,
        opts: TelegramSendOptions,
    ) -> Callable[[], Awaitable[int]]:
        """
        Выбрать операцию отправки: медиа (есть ``media`` с ``local_path``
        или ``ref``) либо текст. ``local_path`` (скачано при ingest, A3) →
        заливка файла; иначе ``ref`` → refetch-fast-path (A2). Возвращает
        0-арный корутин-факторий для ``_send_resilient`` (FloodWait/transient-
        обёртка едина для обоих).
        """
        media = record.media[0] if record.media else None
        if media is not None and (
            media.local_path is not None or media.ref is not None
        ):
            source_ref = media.ref
            local_path = media.local_path

            async def do_send() -> int:
                return await client.send_media(
                    peer,
                    source_ref=source_ref,
                    local_path=local_path,
                    text=record.text,
                    reply_to=reply_to,
                    parse_mode=opts.parse_mode,
                    silent=opts.silent,
                )
        else:

            async def do_send() -> int:
                return await client.send_message(
                    peer,
                    record.text,
                    reply_to=reply_to,
                    parse_mode=opts.parse_mode,
                    silent=opts.silent,
                    link_preview=not opts.disable_preview,
                )

        return do_send

    async def _apply_dynamic_limits(self) -> None:
        """
        §12.8: применить динамические лимиты троттлинга «на лету».

        Эффективный темп = override (не-``None`` поле ``DynamicSettings``)
        либо файловый порог при его отсутствии — поэтому сброс override'а
        возвращает файловое значение. При изменении переконфигурируем уже
        созданные бакеты; лениво создаваемые позже подхватят новый темп
        сами (``_chat_bucket`` читает ``self._chat_per_second``).
        """
        if self._runtime_config is None:
            return
        settings = await self._runtime_config.load()
        chat_rate = (
            settings.sender_chat_per_second
            if settings.sender_chat_per_second is not None
            else self._file_chat_per_second
        )
        if chat_rate != self._chat_per_second:
            self._chat_per_second = chat_rate
            for bucket in self._chat_buckets.values():
                bucket.reconfigure(rate=chat_rate, capacity=max(chat_rate, 1.0))
        account_rate = (
            settings.sender_account_per_minute
            if settings.sender_account_per_minute is not None
            else self._file_account_per_minute
        )
        if account_rate != self._account_per_minute:
            self._account_per_minute = account_rate
            for bucket in self._account_buckets.values():
                bucket.reconfigure(
                    rate=account_rate / 60.0, capacity=max(account_rate, 1.0)
                )

    async def _throttle(self, account_id: str, chat_id: str) -> None:
        await self._chat_bucket(account_id, chat_id).acquire()
        await self._account_buckets[account_id].acquire()

    def _chat_bucket(self, account_id: str, chat_id: str) -> TokenBucket:
        key = (account_id, chat_id)
        bucket = self._chat_buckets.get(key)
        if bucket is None:
            bucket = TokenBucket(
                rate=self._chat_per_second,
                capacity=max(self._chat_per_second, 1.0),
                clock=self._clock,
                sleep=self._sleep,
            )
            self._chat_buckets[key] = bucket
        return bucket

    async def _send_resilient(
        self, *, do_send: Callable[[], Awaitable[int]], peer: int | str
    ) -> int:
        """FloodWait-повтор поверх transient-ретраев (tenacity)."""
        floods = 0
        while True:
            try:
                return await self._send_with_transient_retry(do_send)
            except FloodWaitError as exc:
                floods += 1
                if floods > self._flood_max_retries:
                    raise
                self._log.warning(
                    'flood_wait', seconds=exc.seconds, attempt=floods, peer=str(peer)
                )
                await self._sleep(exc.seconds)

    async def _send_with_transient_retry(
        self, do_send: Callable[[], Awaitable[int]]
    ) -> int:
        retryer = AsyncRetrying(
            retry=retry_if_exception_type(TransientSendError),
            stop=stop_after_attempt(self._transient_max_attempts),
            wait=wait_exponential(multiplier=self._backoff_base, max=self._backoff_cap),
            sleep=self._sleep,
            reraise=True,
        )
        return await retryer(do_send)
