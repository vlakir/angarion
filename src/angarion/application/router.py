"""
Router (§6.2 ТЗ, FR-8): таблица ``(Address, EventKind) → set[pipeline]``.

Multicast — базовое требование: источник входит в произвольное число
пайплайнов. Фильтры-предикаты пайплайна (``only_replies``)
применяются здесь же (§6.1, шаг 3).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from angarion.domain.models import Address, EventKind, InboundEvent


class RouteSpec(BaseModel):
    """
    Декларация маршрута пайплайна (секция ``[pipelines.*]`` §11):
    подписка на виды событий + источники + фильтры-предикаты.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    pipeline: str
    events: frozenset[EventKind]
    sources: tuple[Address, ...]
    only_replies: bool = False


def _source_matches(declared: Address, source: Address) -> bool:
    """
    Идентичность адреса для маршрутизации: messenger + chat_id +
    thread_id; ``title`` декоративен. Маршрут без ``thread_id``
    покрывает весь чат (включая треды), с ``thread_id`` — только тред.
    """
    return (
        declared.messenger == source.messenger
        and declared.chat_id == source.chat_id
        and declared.thread_id in (None, source.thread_id)
    )


class Router:
    """Маршрутизация события в пайплайны (multicast)."""

    def __init__(self, routes: Sequence[RouteSpec]) -> None:
        self._routes = tuple(routes)

    def resolve(
        self, source: Address, kind: EventKind, event: InboundEvent
    ) -> set[str]:
        """Пайплайны, подписанные на (source, kind) и прошедшие фильтры."""
        return {
            route.pipeline
            for route in self._routes
            if kind in route.events
            and any(_source_matches(declared, source) for declared in route.sources)
            and (not route.only_replies or event.reply_to_external_id is not None)
        }
