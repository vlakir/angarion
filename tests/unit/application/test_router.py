"""Router (§6.2 ТЗ, FR-8): multicast, подписка на виды, фильтры-предикаты."""

from __future__ import annotations

from app_factories import make_endpoint, make_record

from angarion.application.router import Router, RouteSpec
from angarion.domain.models import RecordKind

ALL_KINDS = set(RecordKind)


def make_route(**overrides: object) -> RouteSpec:
    fields: dict[str, object] = {
        'pipeline': 'digest',
        'events': {RecordKind.NEW},
        'sources': [make_endpoint()],
    }
    fields.update(overrides)
    return RouteSpec.model_validate(fields)


class TestResolve:
    def test_matches_source_and_kind(self) -> None:
        router = Router([make_route()])
        resolved = router.resolve(make_endpoint(), RecordKind.NEW, make_record())
        assert resolved == {'digest'}

    def test_kind_not_subscribed(self) -> None:
        router = Router([make_route()])
        event = make_record(kind=RecordKind.DELETED, text=None)
        resolved = router.resolve(make_endpoint(), RecordKind.DELETED, event)
        assert resolved == set()

    def test_unknown_source(self) -> None:
        router = Router([make_route()])
        other = make_endpoint(address='-100777')
        event = make_record(source=other)
        assert router.resolve(other, RecordKind.NEW, event) == set()

    def test_multicast_same_source(self) -> None:
        """Источник входит в произвольное число пайплайнов (§6.2)."""
        router = Router([make_route(), make_route(pipeline='audit')])
        resolved = router.resolve(make_endpoint(), RecordKind.NEW, make_record())
        assert resolved == {'digest', 'audit'}

    def test_title_ignored_in_matching(self) -> None:
        """Title — декоративное поле, в идентичность адреса не входит."""
        router = Router([make_route(sources=[make_endpoint(title='Из конфига')])])
        source = make_endpoint(title='Из события')
        event = make_record(source=source)
        assert router.resolve(source, RecordKind.NEW, event) == {'digest'}

    def test_no_routes(self) -> None:
        router = Router([])
        assert router.resolve(make_endpoint(), RecordKind.NEW, make_record()) == set()


class TestThreadMatching:
    def test_route_without_thread_matches_any_thread(self) -> None:
        """Маршрут без thread_id покрывает весь чат, включая треды."""
        router = Router([make_route()])
        source = make_endpoint(thread_id='7')
        event = make_record(source=source)
        assert router.resolve(source, RecordKind.NEW, event) == {'digest'}

    def test_route_with_thread_requires_same_thread(self) -> None:
        router = Router([make_route(sources=[make_endpoint(thread_id='7')])])
        same = make_endpoint(thread_id='7')
        other = make_endpoint(thread_id='8')
        assert router.resolve(same, RecordKind.NEW, make_record(source=same)) == {
            'digest'
        }
        assert router.resolve(other, RecordKind.NEW, make_record(source=other)) == set()

    def test_route_with_thread_rejects_chat_level_event(self) -> None:
        router = Router([make_route(sources=[make_endpoint(thread_id='7')])])
        source = make_endpoint()
        event = make_record(source=source)
        assert router.resolve(source, RecordKind.NEW, event) == set()


class TestOnlyReplies:
    def test_filters_non_replies(self) -> None:
        router = Router([make_route(only_replies=True)])
        event = make_record()
        assert router.resolve(make_endpoint(), RecordKind.NEW, event) == set()

    def test_passes_replies(self) -> None:
        router = Router([make_route(only_replies=True)])
        event = make_record(reply_to_external_id='10')
        resolved = router.resolve(make_endpoint(), RecordKind.NEW, event)
        assert resolved == {'digest'}

    def test_filter_is_per_pipeline(self) -> None:
        """Фильтр одного пайплайна не задевает другие (multicast)."""
        router = Router([make_route(only_replies=True), make_route(pipeline='audit')])
        event = make_record()
        resolved = router.resolve(make_endpoint(), RecordKind.NEW, event)
        assert resolved == {'audit'}
