"""Реестр процессоров, @processor и passthrough (§10.1–10.2 ТЗ, FR-17)."""

from __future__ import annotations

import pytest
from app_factories import make_context, make_event, make_services, make_target

from angarion.application import processors
from angarion.application.processors import (
    FunctionProcessor,
    TemplateProcessorConfig,
    get_processor,
    processor,
    register,
    registered,
)
from angarion.domain.errors import ConfigError
from angarion.domain.keys import make_idempotency_key
from angarion.domain.models import (
    EventKind,
    InboundEvent,
    MediaRef,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Verdict,
)
from angarion.domain.ports import ProcessorPort


@pytest.fixture(autouse=True)
def _registry_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Изолировать мутации реестра: каждый тест работает с копией."""
    monkeypatch.setattr(processors, '_registry', dict(processors._registry))


async def echo(
    event: InboundEvent, ctx: PipelineContextData, svc: ProcessorServices
) -> ProcessingResult:
    return ProcessingResult(verdict=Verdict.DROP, note=event.text)


class TestRegistry:
    def test_decorator_registers_function(self) -> None:
        decorated = processor('my_echo')(echo)
        assert decorated is echo  # функция возвращается как есть
        proc = get_processor('my_echo')
        assert isinstance(proc, ProcessorPort)
        assert proc.name == 'my_echo'

    async def test_function_processor_delegates(self) -> None:
        proc = FunctionProcessor(name='my_echo', fn=echo)
        result = await proc.process(make_event(), make_context(), make_services())
        assert result.note == 'hello'

    def test_duplicate_name_rejected(self) -> None:
        register(FunctionProcessor(name='dup', fn=echo))
        with pytest.raises(ConfigError, match='dup'):
            register(FunctionProcessor(name='dup', fn=echo))

    def test_unknown_name_lists_known(self) -> None:
        with pytest.raises(ConfigError, match='passthrough'):
            get_processor('no_such_processor')

    def test_registered_returns_snapshot(self) -> None:
        snapshot = registered()
        assert 'passthrough' in snapshot
        snapshot.clear()
        assert 'passthrough' in registered()


class TestPassthrough:
    async def test_delivers_to_all_targets(self) -> None:
        """§10.2: ретрансляция текста как есть, по ключу на цель."""
        proc = get_processor('passthrough')
        event = make_event()
        targets = [make_target('-100111'), make_target('-100222')]
        ctx = make_context(targets=targets)
        result = await proc.process(event, ctx, make_services())
        assert result.verdict is Verdict.DELIVER
        assert [msg.text for msg in result.outbound] == ['hello', 'hello']
        assert [msg.target.chat_id for msg in result.outbound] == [
            '-100111',
            '-100222',
        ]
        expected_keys = [
            make_idempotency_key('digest', event, spec.target, n)
            for n, spec in enumerate(targets)
        ]
        assert [msg.idempotency_key for msg in result.outbound] == expected_keys
        assert [msg.send_via for msg in result.outbound] == [
            spec.send_via for spec in targets
        ]

    async def test_event_without_text_dropped(self) -> None:
        """§10.1: процессор обязан переживать text=None (DELETED без реестра)."""
        proc = get_processor('passthrough')
        event = make_event(kind=EventKind.MESSAGE_DELETED, text=None)
        result = await proc.process(event, make_context(), make_services())
        assert result.verdict is Verdict.DROP
        assert result.outbound == []
        assert result.note is not None

    async def test_media_forwarded_to_all_targets(self) -> None:
        """A1 (M7): вложения события транзитом переносятся в каждый
        OutboundMessage (sender научится их слать в A2)."""
        proc = get_processor('passthrough')
        media = [MediaRef(kind='photo', ref='file42')]
        event = make_event(media=media)
        targets = [make_target('-100111'), make_target('-100222')]
        ctx = make_context(targets=targets)
        result = await proc.process(event, ctx, make_services())
        assert result.verdict is Verdict.DELIVER
        assert [msg.media for msg in result.outbound] == [media, media]

    async def test_no_media_yields_empty_outbound_media(self) -> None:
        """Без вложений OutboundMessage.media пуст (а не None)."""
        proc = get_processor('passthrough')
        result = await proc.process(make_event(), make_context(), make_services())
        assert all(msg.media == [] for msg in result.outbound)


class TestTemplate:
    """Встроенный процессор ``template`` (§10.2, FR §3 спеки T007).

    Каждый тест использует своё имя пайплайна: процессор — синглтон с
    кэшем конфига по пайплайну (W1), один пайплайн = один конфиг.
    """

    def test_registered_as_builtin(self) -> None:
        proc = get_processor('template')
        assert isinstance(proc, ProcessorPort)
        assert proc.name == 'template'

    async def test_delivers_rendered_text_to_all_targets(self) -> None:
        """§10.2: рендер базового шаблона → OutboundMessage по ключу на цель."""
        proc = get_processor('template')
        event = make_event(text='мир')
        targets = [make_target('-100111'), make_target('-100222')]
        ctx = make_context(
            pipeline='tmpl_deliver',
            targets=targets,
            settings={'template': 'Эхо: {{ text }}'},
        )
        result = await proc.process(event, ctx, make_services('tmpl_deliver'))
        assert result.verdict is Verdict.DELIVER
        assert [msg.text for msg in result.outbound] == ['Эхо: мир', 'Эхо: мир']
        expected_keys = [
            make_idempotency_key('tmpl_deliver', event, spec.target, n)
            for n, spec in enumerate(targets)
        ]
        assert [msg.idempotency_key for msg in result.outbound] == expected_keys

    async def test_edited_uses_edited_template(self) -> None:
        proc = get_processor('template')
        event = make_event(
            kind=EventKind.MESSAGE_EDITED, text='новый', previous_text='старый'
        )
        ctx = make_context(
            pipeline='tmpl_edited',
            settings={
                'template': 'NEW {{ text }}',
                'edited': '{{ previous_text }} → {{ text }}',
            },
        )
        result = await proc.process(event, ctx, make_services('tmpl_edited'))
        assert result.outbound[0].text == 'старый → новый'

    async def test_edited_falls_back_to_base_template(self) -> None:
        proc = get_processor('template')
        event = make_event(kind=EventKind.MESSAGE_EDITED, text='t')
        ctx = make_context(
            pipeline='tmpl_fallback', settings={'template': 'base {{ text }}'}
        )
        result = await proc.process(event, ctx, make_services('tmpl_fallback'))
        assert result.outbound[0].text == 'base t'

    async def test_deleted_uses_deleted_template(self) -> None:
        """N1: для DELETED рендерится свой шаблон, если задан."""
        proc = get_processor('template')
        event = make_event(
            kind=EventKind.MESSAGE_DELETED, text=None, external_id='7'
        )
        ctx = make_context(
            pipeline='tmpl_deleted',
            settings={'template': '{{ text }}', 'deleted': 'удалено #{{ external_id }}'},
        )
        result = await proc.process(event, ctx, make_services('tmpl_deleted'))
        assert result.verdict is Verdict.DELIVER
        assert result.outbound[0].text == 'удалено #7'

    async def test_empty_render_dropped(self) -> None:
        """N1: пустой рендер (DELETED без deleted-шаблона) → DROP."""
        proc = get_processor('template')
        event = make_event(kind=EventKind.MESSAGE_DELETED, text=None)
        ctx = make_context(pipeline='tmpl_empty', settings={'template': '{{ text }}'})
        result = await proc.process(event, ctx, make_services('tmpl_empty'))
        assert result.verdict is Verdict.DROP
        assert result.outbound == []
        assert result.note is not None

    async def test_invalid_config_raises_config_error(self) -> None:
        """W1: некорректный processor_config → ConfigError на первом событии."""
        proc = get_processor('template')
        ctx = make_context(pipeline='tmpl_invalid', settings={})
        with pytest.raises(ConfigError, match='processor_config'):
            await proc.process(make_event(), ctx, make_services('tmpl_invalid'))

    async def test_bad_template_syntax_raises_config_error(self) -> None:
        """Битый синтаксис Jinja2 в конфиге → ConfigError (не TemplateSyntaxError)."""
        proc = get_processor('template')
        ctx = make_context(pipeline='tmpl_syntax', settings={'template': '{{ text '})
        with pytest.raises(ConfigError, match='processor_config'):
            await proc.process(make_event(), ctx, make_services('tmpl_syntax'))

    async def test_config_cached_per_pipeline(self) -> None:
        """W1: повторное событие пайплайна берёт конфиг из кэша."""
        proc = get_processor('template')
        ctx = make_context(pipeline='tmpl_cache', settings={'template': '{{ text }}'})
        svc = make_services('tmpl_cache')
        first = await proc.process(make_event(text='a'), ctx, svc)
        second = await proc.process(make_event(text='b'), ctx, svc)
        assert first.outbound[0].text == 'a'
        assert second.outbound[0].text == 'b'

    def test_config_model_returns_schema(self) -> None:
        """FR-0 (T021): хук отдаёт модель для валидации на старте."""
        assert get_processor('template').config_model() is TemplateProcessorConfig


class TestConfigModelHook:
    """FR-0 (T021): хук ``config_model`` контракта ``ProcessorPort``."""

    def test_passthrough_has_no_config_model(self) -> None:
        assert get_processor('passthrough').config_model() is None

    def test_function_processor_has_no_config_model(self) -> None:
        proc = FunctionProcessor(name='noop', fn=echo)
        assert proc.config_model() is None
