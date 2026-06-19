"""
Резолв сущностей с прогревом кэша (Q4, Q5 спеки T005, M3, фаза 2).

На старте, **per account**, греет кэш сущностей одним ``get_dialogs``
(Q5, gotcha T004 §3 — резолв/отправка по числовому id не падают
``Could not find the input entity`` на холодной сессии), затем резолвит
сконфигурированные источники в стабильный знаковый chat id.

Ошибка резолва **источника** (приватный/удалён/нет доступа) — не
fail-fast по всему старту, а управляемая деградация (Q4): источник
пропускается, громкий warning + событие ``source_unavailable`` в
аналитику; остальные источники/пайплайны продолжают работу (§12.10).

Возвращает богатые ``ResolvedSource`` (числовой chat id + готовый
source_key): live-фильтрация и catch-up §9.3 (фаза 3) опираются на них.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from angarion.adapters.telegram.client import MESSENGER
from angarion.domain.keys import make_source_key
from angarion.domain.models import AnalyticsEvent

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from structlog.typing import FilteringBoundLogger

    from angarion.adapters.telegram.client import TelegramClientPort
    from angarion.config import EndpointConfig
    from angarion.domain.ports import AnalyticsPort


class ResolvedSource(BaseModel):
    """Резолвленный источник: аккаунт + числовой chat id + source_key."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    account_id: str
    chat_id: int
    thread_id: str | None
    source_key: str
    recent_poll: bool = False
    """Включён ли для источника лёгкий поллинг недавнего окна (T032)."""


async def resolve_sources(
    *,
    clients: Mapping[str, TelegramClientPort],
    sources: Sequence[EndpointConfig],
    analytics: AnalyticsPort,
    log: FilteringBoundLogger,
    recent_poll_endpoints: frozenset[EndpointConfig] = frozenset(),
) -> list[ResolvedSource]:
    """
    Прогреть кэш каждого клиента и резолвить источники в знаковые chat
    id. Возвращает список успешно резолвленных источников; недоступные
    в результат не попадают (управляемая деградация, Q4).

    ``recent_poll_endpoints`` (T032) — подмножество ``sources``, для
    которых включён лёгкий поллинг недавнего окна; флаг переносится на
    соответствующий ``ResolvedSource``.
    """
    for client in clients.values():
        await client.warm_entity_cache()
    resolved: list[ResolvedSource] = []
    for ep in sources:
        source_client = clients.get(ep.account)
        if source_client is None:
            continue
        try:
            chat_id = await source_client.resolve_peer(ep.chat_id)
        except Exception as exc:
            log.warning(
                'source_unavailable',
                account=ep.account,
                chat_id=ep.chat_id,
                error=str(exc),
            )
            await analytics.record(
                AnalyticsEvent(
                    uid=uuid4(),
                    kind='source_unavailable',
                    payload={
                        'account': ep.account,
                        'chat_id': ep.chat_id,
                        'error': str(exc),
                    },
                    at=datetime.now(UTC),
                )
            )
            continue
        resolved.append(
            ResolvedSource(
                account_id=ep.account,
                chat_id=chat_id,
                thread_id=ep.thread_id,
                source_key=make_source_key(
                    MESSENGER, ep.account, str(chat_id), ep.thread_id
                ),
                recent_poll=ep in recent_poll_endpoints,
            )
        )
    return resolved
