"""
Unit-тесты примера stateful-процессора «дайджест» (``examples/digest``).

Проверяем накопление в ``svc.state``, сброс по ``n``/``max_age_s``,
LLM-саммари батча через fake-границу (без сети) и — главное —
идемпотентность под at-least-once: повторная доставка того же события
ни внутри батча, ни после сброса не задваивает накопитель и не шлёт
дайджест дважды (§10.3, FR «Тестируемость»).

Каталог примера и общие фабрики подключены через ``conftest.py``.
Файл вне ``--cov=src`` (пример не в ``src/``), но гоняется в CI.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import pytest
from app_factories import make_context, make_event, make_services, make_target
from processor import DIGEST, DigestProcessor, DigestState

from angarion.application.llm import LlmHttpResult, LlmTransportError
from angarion.domain.errors import ConfigError, ProcessingError
from angarion.domain.models import Verdict
from angarion.domain.ports import ProcessorPort

if TYPE_CHECKING:
    from angarion.domain.models import ProcessorServices

BASE_SETTINGS: dict[str, Any] = {
    'n': 3,
    'base_url': 'http://localhost:11434/v1',
    'model': 'qwen2.5:3b',
    'system_prompt': 'Ты — составитель сводок.',
    'user_prompt': 'Сделай дайджест:',
}


def ok(content: str | None) -> LlmHttpResult:
    """Успешный (200) ответ с content в choices[0].message."""
    return LlmHttpResult(
        status_code=200, body={'choices': [{'message': {'content': content}}]}
    )


class FakeLlmHttp:
    """Граница HTTP LLM, отдающая заданную последовательность исходов."""

    def __init__(self, outcomes: list[LlmHttpResult | Exception]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    async def post_chat(
        self,
        *,
        base_url: str,
        payload: dict[str, Any],
        api_key: str | None,
        timeout_s: float,
    ) -> LlmHttpResult:
        self.calls.append(
            {
                'base_url': base_url,
                'payload': payload,
                'api_key': api_key,
                'timeout_s': timeout_s,
            }
        )
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class Clock:
    """Мутабельные настенные часы для детерминированного теста ``max_age_s``."""

    def __init__(self, start: datetime) -> None:
        self.t = start

    def __call__(self) -> datetime:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


def make_proc(
    outcomes: list[LlmHttpResult | Exception] | None = None,
    now: Clock | None = None,
) -> tuple[DigestProcessor, FakeLlmHttp]:
    """Процессор с fake-границей LLM и (опц.) инъецированными часами."""
    fake = FakeLlmHttp(outcomes or [])
    kwargs: dict[str, Any] = {'http': fake}
    if now is not None:
        kwargs['now'] = now
    return DigestProcessor(**kwargs), fake


async def feed(
    proc: DigestProcessor,
    svc: ProcessorServices,
    *,
    count: int,
    settings: dict[str, Any],
    start: int = 0,
) -> None:
    """Скормить процессору ``count`` различных событий (разные external_id)."""
    for i in range(start, start + count):
        ctx = make_context(settings=settings)
        await proc.process(make_event(external_id=str(i), text=f'msg{i}'), ctx, svc)


class TestDigestProcessor:
    def test_registered_object_is_processor(self) -> None:
        assert isinstance(DIGEST, ProcessorPort)
        assert DIGEST.name == 'digest'

    async def test_accumulates_below_threshold_drops(self) -> None:
        proc, fake = make_proc()
        svc = make_services()
        ctx = make_context(settings=BASE_SETTINGS)
        result = await proc.process(make_event(external_id='1', text='a'), ctx, svc)
        assert result.verdict is Verdict.DROP
        assert result.outbound == []
        assert fake.calls == []  # LLM не дёргается до сброса
        state = DigestState.model_validate_json(await svc.state.get('state'))
        assert [it.text for it in state.items] == ['a']

    async def test_flush_at_threshold_delivers_summary_to_all_targets(self) -> None:
        proc, fake = make_proc([ok('СВОДКА')])
        svc = make_services()
        targets = [make_target('-100111'), make_target('-100222')]
        await feed(proc, svc, count=2, settings=BASE_SETTINGS)
        ctx = make_context(targets=targets, settings=BASE_SETTINGS)
        result = await proc.process(make_event(external_id='2', text='c'), ctx, svc)
        assert result.verdict is Verdict.DELIVER
        assert [m.text for m in result.outbound] == ['СВОДКА', 'СВОДКА']
        assert len(fake.calls) == 1
        # батч из трёх сообщений ушёл в user-промпт
        user_msg = fake.calls[0]['payload']['messages'][1]['content']
        assert 'msg0' in user_msg
        assert 'msg1' in user_msg
        assert 'c' in user_msg
        # после сброса накопитель пуст
        state = DigestState.model_validate_json(await svc.state.get('state'))
        assert state.items == []

    async def test_redelivery_in_batch_not_double_counted(self) -> None:
        """At-least-once: тот же event до сброса не задваивает накопитель."""
        proc, _ = make_proc()
        svc = make_services()
        ctx = make_context(settings=BASE_SETTINGS)
        event = make_event(external_id='1', text='a')
        first = await proc.process(event, ctx, svc)
        second = await proc.process(event, ctx, svc)  # повторная доставка
        assert first.verdict is Verdict.DROP
        assert second.verdict is Verdict.DROP
        state = DigestState.model_validate_json(await svc.state.get('state'))
        assert len(state.items) == 1  # не два

    async def test_redelivery_after_flush_does_not_reflush(self) -> None:
        """At-least-once: повторная доставка триггера после сброса → DROP."""
        proc, fake = make_proc([ok('СВОДКА')])
        svc = make_services()
        await feed(proc, svc, count=2, settings=BASE_SETTINGS)
        ctx = make_context(settings=BASE_SETTINGS)
        trigger = make_event(external_id='2', text='c')
        flushed = await proc.process(trigger, ctx, svc)
        again = await proc.process(trigger, ctx, svc)  # повтор триггера
        assert flushed.verdict is Verdict.DELIVER
        assert again.verdict is Verdict.DROP
        assert again.outbound == []
        assert len(fake.calls) == 1  # второго вызова LLM нет

    async def test_flush_by_max_age(self) -> None:
        """max_age_s сбрасывает накопитель ниже порога n по возрасту батча."""
        clock = Clock(datetime(2026, 6, 14, 12, 0, tzinfo=UTC))
        proc, fake = make_proc([ok('СВОДКА')], now=clock)
        svc = make_services()
        settings = {**BASE_SETTINGS, 'n': 100, 'max_age_s': 60.0}
        ctx = make_context(settings=settings)
        first = await proc.process(
            make_event(external_id='1', text='a'), ctx, svc
        )
        assert first.verdict is Verdict.DROP  # батч молод — копим
        clock.advance(120)  # батчу теперь 120с > max_age
        result = await proc.process(make_event(external_id='2', text='b'), ctx, svc)
        assert result.verdict is Verdict.DELIVER
        assert len(fake.calls) == 1

    async def test_recent_tail_capped(self) -> None:
        """Хвост сброшенных ключей ограничен recent_cap."""
        proc, _ = make_proc([ok('s1'), ok('s2')])
        svc = make_services()
        settings = {**BASE_SETTINGS, 'n': 2, 'recent_cap': 2}
        # два сброса по 2 ключа = 4 сброшенных, хвост обрезан до 2
        await feed(proc, svc, count=4, settings=settings)
        state = DigestState.model_validate_json(await svc.state.get('state'))
        assert len(state.recent) == 2

    async def test_text_none_dropped_not_accumulated(self) -> None:
        proc, fake = make_proc()
        svc = make_services()
        ctx = make_context(settings=BASE_SETTINGS)
        result = await proc.process(
            make_event(external_id='1', text=None), ctx, svc
        )
        assert result.verdict is Verdict.DROP
        assert fake.calls == []
        assert await svc.state.get('state') is None  # ничего не сохранили

    async def test_empty_llm_content_falls_back_to_raw_body(self) -> None:
        proc, _ = make_proc([ok('')])
        svc = make_services()
        await feed(proc, svc, count=2, settings=BASE_SETTINGS)
        ctx = make_context(settings=BASE_SETTINGS)
        result = await proc.process(make_event(external_id='2', text='c'), ctx, svc)
        assert result.verdict is Verdict.DELIVER
        assert 'msg0' in result.outbound[0].text  # сырой батч как fallback

    async def test_transport_error_raises_and_keeps_accumulation(self) -> None:
        """Сбой LLM → ProcessingError (re-enqueue); накопитель сохранён."""
        proc, _ = make_proc([LlmTransportError('boom')])
        svc = make_services()
        await feed(proc, svc, count=2, settings=BASE_SETTINGS)
        ctx = make_context(settings=BASE_SETTINGS)
        with pytest.raises(ProcessingError):
            await proc.process(make_event(external_id='2', text='c'), ctx, svc)
        # накопление уже персистировано — на повторе не задвоится
        state = DigestState.model_validate_json(await svc.state.get('state'))
        assert len(state.items) == 3

    async def test_bad_status_raises_processing_error(self) -> None:
        proc, _ = make_proc([LlmHttpResult(status_code=500)])
        svc = make_services()
        await feed(proc, svc, count=2, settings=BASE_SETTINGS)
        ctx = make_context(settings=BASE_SETTINGS)
        with pytest.raises(ProcessingError):
            await proc.process(make_event(external_id='2', text='c'), ctx, svc)

    async def test_invalid_config_raises_config_error(self) -> None:
        proc, _ = make_proc()
        ctx = make_context(settings={'n': 3})  # нет base_url/model/промптов
        with pytest.raises(ConfigError, match='processor_config'):
            await proc.process(make_event(), ctx, make_services())

    async def test_api_key_env_missing_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv('ANGARION_TEST_DIGEST_KEY', raising=False)
        proc, _ = make_proc([ok('s')])
        svc = make_services()
        settings = {**BASE_SETTINGS, 'n': 1, 'api_key_env': 'ANGARION_TEST_DIGEST_KEY'}
        ctx = make_context(settings=settings)
        with pytest.raises(ConfigError, match='api_key_env'):
            await proc.process(make_event(external_id='1', text='a'), ctx, svc)

    async def test_api_key_env_read_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('ANGARION_TEST_DIGEST_KEY', 'sk-secret')
        proc, fake = make_proc([ok('s')])
        svc = make_services()
        settings = {**BASE_SETTINGS, 'n': 1, 'api_key_env': 'ANGARION_TEST_DIGEST_KEY'}
        ctx = make_context(settings=settings)
        await proc.process(make_event(external_id='1', text='a'), ctx, svc)
        assert fake.calls[0]['api_key'] == 'sk-secret'
