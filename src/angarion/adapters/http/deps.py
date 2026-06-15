"""
DI-контейнер и типизированные FastAPI-зависимости HTTP-адаптера (§12.5).

``AngarionDeps`` — контейнер driven-портов из composition root, который
фабрика ``create_app`` кладёт в ``app.state``. Поверх него опубликованы
типизированные зависимости (``AnalyticsDep`` и пр.) — это **публичный
API** для пользовательских ручек: пользователь дотягивается до системы
только через порты, не зная про ORM/сессии/Telethon.

Без ``from __future__ import annotations``: аннотации pydantic-модели
``AngarionDeps`` вычисляются в runtime (как у ``StorageBundle``).
"""

from typing import Annotated, Any, cast

from fastapi import Depends, Request
from pydantic import BaseModel, ConfigDict

from angarion.config import AngarionSettings
from angarion.domain.ports import (
    AnalyticsPort,
    CursorStorePort,
    EventQueuePort,
    MessageRegistryPort,
    StateStorePort,
)


class AngarionDeps(BaseModel):
    """
    Контейнер портов composition root в ``app.state`` (§12.5, spec §5).

    Объём фазы 2 (T022) — read-порты встроенного ``/api/v1`` и DI для
    пользовательских ручек; ``runtime_config``/``outbox`` придут
    аддитивно в группе C (§12.8/§12.9). ``webhook_routers`` — собранные
    composition root'ом роутеры адаптеров (``push_transport="webhook"``,
    §12.11), которые ``create_app`` монтирует поверх встроенных.
    ``auth_sessionmaker`` (T023) — ``async_sessionmaker`` user store
    fastapi-users (§12.7); ``None`` при ``auth="none"``.

    Конструкция композиции (A-2): JSON-контракт DTO не действует; frozen
    с ``arbitrary_types_allowed`` — как у ``StorageBundle``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    queue: EventQueuePort
    analytics: AnalyticsPort
    registry: MessageRegistryPort
    state: StateStorePort
    cursors: CursorStorePort
    settings: AngarionSettings
    webhook_routers: tuple[Any, ...] = ()
    auth_sessionmaker: Any = None


def get_deps(request: Request) -> AngarionDeps:
    """Достать контейнер портов из ``app.state`` (база DI-провайдеров)."""
    return cast('AngarionDeps', request.app.state.angarion_deps)


def get_analytics(request: Request) -> AnalyticsPort:
    """DI-провайдер ``AnalyticsPort`` (read-сторона аналитики)."""
    return get_deps(request).analytics


def get_registry(request: Request) -> MessageRegistryPort:
    """DI-провайдер ``MessageRegistryPort`` (реестр сообщений)."""
    return get_deps(request).registry


def get_state(request: Request) -> StateStorePort:
    """DI-провайдер ``StateStorePort`` (KV stateful-процессоров)."""
    return get_deps(request).state


def get_queue(request: Request) -> EventQueuePort:
    """DI-провайдер ``EventQueuePort`` (очередь событий)."""
    return get_deps(request).queue


def get_cursors(request: Request) -> CursorStorePort:
    """DI-провайдер ``CursorStorePort`` (курсоры catch-up)."""
    return get_deps(request).cursors


AnalyticsDep = Annotated[AnalyticsPort, Depends(get_analytics)]
RegistryDep = Annotated[MessageRegistryPort, Depends(get_registry)]
StateDep = Annotated[StateStorePort, Depends(get_state)]
QueueDep = Annotated[EventQueuePort, Depends(get_queue)]
CursorsDep = Annotated[CursorStorePort, Depends(get_cursors)]
