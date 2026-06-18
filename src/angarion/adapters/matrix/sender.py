"""
MatrixSender (FR «Sender», M7 B3): доставка ``OutboundMessage`` в Matrix.

Реализует ``MessageSinkPort`` поверх ``MatrixClientPort`` (общий с
listener пул клиентов): резолвит целевую комнату, отправляет текст или
вложение (тред/reply), переживает rate-limit (``M_LIMIT_EXCEEDED``) и
повторяет transient-сбои — зеркало `TelegramSender` (§8), но без
token-bucket троттлинга (для второго адаптера достаточно реактивного
rate-limit-ожидания; вынести в общий механизм можно позже).

Деградация медиа (вложение недоступно / homeserver не принял reupload по
ссылке) живёт в границе nio (`MatrixClient.send_media` падает в текст) —
здесь, как и у Telegram, sender медиа-fallback не дублирует.

Время в ``DeliveryReceipt`` — UTC (§17.4); ``external_id`` — Matrix
``event_id`` отправленного события.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from angarion.adapters.matrix.client import MatrixRateLimitError, MatrixTransientError
from angarion.domain.errors import ConfigError
from angarion.domain.models import DeliveryReceipt

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.matrix.client import MatrixClientPort
    from angarion.domain.models import OutboundMessage


class MatrixSender:
    """Доставка исходящих в Matrix-комнаты (``MessageSinkPort``, M7 B3)."""

    def __init__(
        self,
        *,
        clients: Mapping[str, MatrixClientPort],
        log: FilteringBoundLogger,
        transient_max_attempts: int = 3,
        rate_limit_max_retries: int = 5,
        backoff_base: float = 1.0,
        backoff_cap: float = 60.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._clients = dict(clients)
        self._log = log
        self._transient_max_attempts = transient_max_attempts
        self._rate_limit_max_retries = rate_limit_max_retries
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._sleep = sleep

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        """Резолв комнаты → устойчивая отправка → ``DeliveryReceipt`` (UTC)."""
        client = self._clients.get(msg.send_via.account_id)
        if client is None:
            account = msg.send_via.account_id
            known = ', '.join(sorted(self._clients)) or '<нет>'
            msg_ = f'нет Matrix-клиента для аккаунта {account!r} (есть: {known})'
            raise ConfigError(msg_)
        await client.restore()  # идемпотентно (роль-сплит: процесс без listener'а)
        room_id = await client.resolve_room(msg.target.chat_id)
        event_id = await self._send_resilient(client, room_id, msg)
        return DeliveryReceipt(external_id=event_id, delivered_at=datetime.now(UTC))

    async def _send_op(
        self, client: MatrixClientPort, room_id: str, msg: OutboundMessage
    ) -> str:
        thread_root = msg.target.thread_id
        if msg.media:
            media = msg.media[0]
            body = msg.text or media.file_name or 'attachment'
            return await client.send_media(
                room_id,
                body=body,
                kind=media.kind,
                mxc_ref=media.ref,
                local_path=media.local_path,
                mime_type=media.mime_type,
                file_name=media.file_name,
                thread_root=thread_root,
            )
        return await client.send_text(room_id, msg.text, thread_root=thread_root)

    async def _send_resilient(
        self, client: MatrixClientPort, room_id: str, msg: OutboundMessage
    ) -> str:
        """Rate-limit-ожидание + transient-ретраи (тот же подход, что Telegram)."""
        rate_retries = 0
        attempts = 0
        while True:
            try:
                return await self._send_op(client, room_id, msg)
            except MatrixRateLimitError as exc:
                rate_retries += 1
                if rate_retries > self._rate_limit_max_retries:
                    raise
                self._log.warning(
                    'matrix_rate_limited',
                    room_id=room_id,
                    retry_after=exc.retry_after,
                )
                await self._sleep(exc.retry_after)
            except MatrixTransientError:
                attempts += 1
                if attempts >= self._transient_max_attempts:
                    raise
                grown = self._backoff_base * 2 ** (attempts - 1)
                await self._sleep(min(self._backoff_cap, grown))
