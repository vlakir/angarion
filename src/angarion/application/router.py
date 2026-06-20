"""
Router (§6.2 ТЗ, FR-8): таблица ``(Endpoint, RecordKind) → set[pipeline]``.

Multicast — базовое требование: источник входит в произвольное число
пайплайнов. Фильтры-предикаты пайплайна (``only_replies``)
применяются здесь же (§6.1, шаг 3).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from angarion.domain.models import Endpoint, Record, RecordKind


class RouteSpec(BaseModel):
    """
    Декларация маршрута пайплайна (секция ``[pipelines.*]`` §11):
    подписка на виды записей + источники + фильтры-предикаты.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    pipeline: str
    events: frozenset[RecordKind]
    sources: tuple[Endpoint, ...]
    only_replies: bool = False


def _source_matches(declared: Endpoint, source: Endpoint) -> bool:
    """
    Идентичность адреса для маршрутизации: transport + address +
    thread_id; ``title`` декоративен. Маршрут без ``thread_id``
    покрывает весь адрес (включая треды), с ``thread_id`` — только тред.
    """
    return (
        declared.transport == source.transport
        and declared.address == source.address
        and declared.thread_id in (None, source.thread_id)
    )


class Router:
    """Маршрутизация записи в пайплайны (multicast)."""

    def __init__(self, routes: Sequence[RouteSpec]) -> None:
        self._routes = tuple(routes)

    def resolve(self, source: Endpoint, kind: RecordKind, record: Record) -> set[str]:
        """Пайплайны, подписанные на (source, kind) и прошедшие фильтры."""
        return {
            route.pipeline
            for route in self._routes
            if kind in route.events
            and any(_source_matches(declared, source) for declared in route.sources)
            and (not route.only_replies or record.reply_to_external_id is not None)
        }
