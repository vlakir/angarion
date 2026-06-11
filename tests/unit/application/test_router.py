"""Router (§6.2 ТЗ, FR-8): multicast, подписка на виды, фильтры-предикаты."""

from __future__ import annotations

from app_factories import make_address, make_event

from angarion.application.router import Router, RouteSpec
from angarion.domain.models import EventKind

ALL_KINDS = set(EventKind)


def make_route(**overrides: object) -> RouteSpec:
    fields: dict[str, object] = {
        'pipeline': 'digest',
        'events': {EventKind.MESSAGE_NEW},
        'sources': [make_address()],
    }
    fields.update(overrides)
    return RouteSpec.model_validate(fields)


class TestResolve:
    def test_matches_source_and_kind(self) -> None:
        router = Router([make_route()])
        resolved = router.resolve(make_address(), EventKind.MESSAGE_NEW, make_event())
        assert resolved == {'digest'}

    def test_kind_not_subscribed(self) -> None:
        router = Router([make_route()])
        event = make_event(kind=EventKind.MESSAGE_DELETED, text=None)
        resolved = router.resolve(make_address(), EventKind.MESSAGE_DELETED, event)
        assert resolved == set()

    def test_unknown_source(self) -> None:
        router = Router([make_route()])
        other = make_address(chat_id='-100777')
        event = make_event(source=other)
        assert router.resolve(other, EventKind.MESSAGE_NEW, event) == set()

    def test_multicast_same_source(self) -> None:
        """Источник входит в произвольное число пайплайнов (§6.2)."""
        router = Router([make_route(), make_route(pipeline='audit')])
        resolved = router.resolve(make_address(), EventKind.MESSAGE_NEW, make_event())
        assert resolved == {'digest', 'audit'}

    def test_title_ignored_in_matching(self) -> None:
        """title — декоративное поле, в идентичность адреса не входит."""
        router = Router([make_route(sources=[make_address(title='Из конфига')])])
        source = make_address(title='Из события')
        event = make_event(source=source)
        assert router.resolve(source, EventKind.MESSAGE_NEW, event) == {'digest'}

    def test_no_routes(self) -> None:
        router = Router([])
        assert router.resolve(make_address(), EventKind.MESSAGE_NEW, make_event()) == set()


class TestThreadMatching:
    def test_route_without_thread_matches_any_thread(self) -> None:
        """Маршрут без thread_id покрывает весь чат, включая треды."""
        router = Router([make_route()])
        source = make_address(thread_id='7')
        event = make_event(source=source)
        assert router.resolve(source, EventKind.MESSAGE_NEW, event) == {'digest'}

    def test_route_with_thread_requires_same_thread(self) -> None:
        router = Router([make_route(sources=[make_address(thread_id='7')])])
        same = make_address(thread_id='7')
        other = make_address(thread_id='8')
        assert router.resolve(
            same, EventKind.MESSAGE_NEW, make_event(source=same)
        ) == {'digest'}
        assert (
            router.resolve(other, EventKind.MESSAGE_NEW, make_event(source=other))
            == set()
        )

    def test_route_with_thread_rejects_chat_level_event(self) -> None:
        router = Router([make_route(sources=[make_address(thread_id='7')])])
        source = make_address()
        event = make_event(source=source)
        assert router.resolve(source, EventKind.MESSAGE_NEW, event) == set()


class TestOnlyReplies:
    def test_filters_non_replies(self) -> None:
        router = Router([make_route(only_replies=True)])
        event = make_event()
        assert router.resolve(make_address(), EventKind.MESSAGE_NEW, event) == set()

    def test_passes_replies(self) -> None:
        router = Router([make_route(only_replies=True)])
        event = make_event(reply_to_external_id='10')
        resolved = router.resolve(make_address(), EventKind.MESSAGE_NEW, event)
        assert resolved == {'digest'}

    def test_filter_is_per_pipeline(self) -> None:
        """Фильтр одного пайплайна не задевает другие (multicast)."""
        router = Router([make_route(only_replies=True), make_route(pipeline='audit')])
        event = make_event()
        resolved = router.resolve(make_address(), EventKind.MESSAGE_NEW, event)
        assert resolved == {'audit'}
