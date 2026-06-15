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

from angarion.application.settings import SettingsNotifier
from angarion.config import AngarionSettings
from angarion.domain.ports import (
    AnalyticsPort,
    CommandOutboxPort,
    CursorStorePort,
    DeadLetterPort,
    EventQueuePort,
    MessageRegistryPort,
    RuntimeConfigPort,
    StateStorePort,
)


class AngarionDeps(BaseModel):
    """
    Контейнер портов composition root в ``app.state`` (§12.5, spec §5).

    Read-порты встроенного ``/api/v1`` и DI для пользовательских ручек
    (T022) + динамика/команды/DLQ группы C (§12.8/§12.9, T024):
    ``runtime_config`` (динамические настройки), ``command_outbox``
    (мост api→pipeline для restart/catchup/notify), ``dead_letters``
    (requeue DLQ). ``notifier`` — in-process ``SettingsNotifier`` (§12.8):
    composition root подписывает на него применение ``log_level``, а
    save-обработчик динамики зовёт ``notify`` после сохранения; ``None``
    в тестах/режимах без подписчиков.

    ``webhook_routers`` — собранные composition root'ом роутеры адаптеров
    (``push_transport="webhook"``, §12.11), которые ``create_app``
    монтирует поверх встроенных. ``auth_sessionmaker`` (T023) —
    ``async_sessionmaker`` user store fastapi-users (§12.7); ``None`` при
    ``auth="none"``.

    Конструкция композиции (A-2): JSON-контракт DTO не действует; frozen
    с ``arbitrary_types_allowed`` — как у ``StorageBundle``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    queue: EventQueuePort
    analytics: AnalyticsPort
    registry: MessageRegistryPort
    state: StateStorePort
    cursors: CursorStorePort
    runtime_config: RuntimeConfigPort
    command_outbox: CommandOutboxPort
    dead_letters: DeadLetterPort
    settings: AngarionSettings
    notifier: Any = None
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


def get_runtime_config(request: Request) -> RuntimeConfigPort:
    """DI-провайдер ``RuntimeConfigPort`` (динамические настройки §12.8)."""
    return get_deps(request).runtime_config


def get_command_outbox(request: Request) -> CommandOutboxPort:
    """DI-провайдер ``CommandOutboxPort`` (мост api→pipeline §12.9)."""
    return get_deps(request).command_outbox


def get_dead_letters(request: Request) -> DeadLetterPort:
    """DI-провайдер ``DeadLetterPort`` (DLQ; requeue §12.8)."""
    return get_deps(request).dead_letters


def get_notifier(request: Request) -> SettingsNotifier | None:
    """DI-провайдер ``SettingsNotifier`` (in-process событие §12.8); ``None`` без."""
    return cast('SettingsNotifier | None', get_deps(request).notifier)


AnalyticsDep = Annotated[AnalyticsPort, Depends(get_analytics)]
RegistryDep = Annotated[MessageRegistryPort, Depends(get_registry)]
StateDep = Annotated[StateStorePort, Depends(get_state)]
QueueDep = Annotated[EventQueuePort, Depends(get_queue)]
CursorsDep = Annotated[CursorStorePort, Depends(get_cursors)]
RuntimeConfigDep = Annotated[RuntimeConfigPort, Depends(get_runtime_config)]
CommandOutboxDep = Annotated[CommandOutboxPort, Depends(get_command_outbox)]
DeadLettersDep = Annotated[DeadLetterPort, Depends(get_dead_letters)]
NotifierDep = Annotated['SettingsNotifier | None', Depends(get_notifier)]
