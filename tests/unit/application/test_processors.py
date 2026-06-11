"""Реестр процессоров, @processor и passthrough (§10.1–10.2 ТЗ, FR-17)."""

from __future__ import annotations

import pytest
from app_factories import make_context, make_event, make_services, make_target

from angarion.application import processors
from angarion.application.processors import (
    FunctionProcessor,
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
