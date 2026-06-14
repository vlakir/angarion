"""Встроенный процессор ``llm`` (§10.2, FR §3 спеки T007).

Процессор тестируется на fake-границе HTTP (``FakeLlmHttp``), реальная
обёртка ``HttpxLlmClient`` — на ``httpx.MockTransport`` без сети.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from app_factories import make_context, make_event, make_services, make_target

from angarion.application.llm import (
    LLM,
    ChatCompletionResponse,
    HttpxLlmClient,
    LlmHttpResult,
    LlmProcessor,
    LlmProcessorConfig,
    LlmTransportError,
)
from angarion.domain.errors import ConfigError, ProcessingError
from angarion.domain.keys import make_idempotency_key
from angarion.domain.models import EventKind, Verdict
from angarion.domain.ports import ProcessorPort

LLM_SETTINGS: dict[str, object] = {
    'base_url': 'http://localhost:11434/v1',
    'model': 'qwen2.5:3b',
    'system_prompt': 'Ты — суммаризатор.',
    'user_prompt': 'Сократи: {{ text }}',
}


def ok(content: str | None) -> LlmHttpResult:
    """Успешный (200) ответ с заданным content в choices[0].message."""
    return LlmHttpResult(
        status_code=200, body={'choices': [{'message': {'content': content}}]}
    )


def status(code: int, *, retry_after: float | None = None) -> LlmHttpResult:
    """HTTP-результат с произвольным статусом (тело не важно)."""
    return LlmHttpResult(status_code=code, retry_after=retry_after)


class FakeLlmHttp:
    """Граница HTTP, отдающая заранее заданную последовательность исходов."""

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


def make_proc(
    outcomes: list[LlmHttpResult | Exception],
    slept: list[float] | None = None,
) -> tuple[LlmProcessor, FakeLlmHttp]:
    """Процессор с fake-границей и пишущим в ``slept`` no-op sleep."""
    fake = FakeLlmHttp(outcomes)

    async def sleep(seconds: float) -> None:
        if slept is not None:
            slept.append(seconds)

    return LlmProcessor(http=fake, sleep=sleep, backoff_base=0.01, backoff_cap=0.01), fake


class TestLlmProcessor:
    def test_registered_object_is_processor(self) -> None:
        assert isinstance(LLM, ProcessorPort)
        assert LLM.name == 'llm'

    def test_config_model_returns_schema(self) -> None:
        """FR-0 (T021): хук отдаёт ``LlmProcessorConfig`` для валидации на старте."""
        assert LLM.config_model() is LlmProcessorConfig

    async def test_delivers_to_all_targets(self) -> None:
        proc, fake = make_proc([ok('сводка')])
        event = make_event(text='длинный текст')
        targets = [make_target('-100111'), make_target('-100222')]
        ctx = make_context(targets=targets, settings=LLM_SETTINGS)
        result = await proc.process(event, ctx, make_services())
        assert result.verdict is Verdict.DELIVER
        assert [m.text for m in result.outbound] == ['сводка', 'сводка']
        expected = [
            make_idempotency_key('digest', event, spec.target, n)
            for n, spec in enumerate(targets)
        ]
        assert [m.idempotency_key for m in result.outbound] == expected
        assert len(fake.calls) == 1

    async def test_text_none_dropped_without_calling_model(self) -> None:
        """Q7: text=None (DELETED без восстановления) → DROP, без вызова LLM."""
        proc, fake = make_proc([])
        event = make_event(kind=EventKind.MESSAGE_DELETED, text=None)
        result = await proc.process(event, make_context(settings=LLM_SETTINGS), make_services())
        assert result.verdict is Verdict.DROP
        assert fake.calls == []

    async def test_empty_content_dropped(self) -> None:
        proc, _ = make_proc([ok('')])
        result = await proc.process(
            make_event(), make_context(settings=LLM_SETTINGS), make_services()
        )
        assert result.verdict is Verdict.DROP
        assert result.outbound == []

    async def test_no_choices_dropped(self) -> None:
        proc, _ = make_proc([LlmHttpResult(status_code=200, body={'choices': []})])
        result = await proc.process(
            make_event(), make_context(settings=LLM_SETTINGS), make_services()
        )
        assert result.verdict is Verdict.DROP

    async def test_429_then_success_honors_retry_after(self) -> None:
        slept: list[float] = []
        proc, fake = make_proc([status(429, retry_after=2.0), ok('ок')], slept)
        result = await proc.process(
            make_event(), make_context(settings=LLM_SETTINGS), make_services()
        )
        assert result.verdict is Verdict.DELIVER
        assert result.outbound[0].text == 'ок'
        assert len(fake.calls) == 2
        assert slept == [2.0]  # уважён Retry-After (потолок timeout_s=60)

    async def test_retry_after_capped_at_timeout(self) -> None:
        """W5: ожидание Retry-After ограничено сверху timeout_s."""
        slept: list[float] = []
        proc, _ = make_proc([status(429, retry_after=99.0), ok('ок')], slept)
        ctx = make_context(settings={**LLM_SETTINGS, 'timeout_s': 1.5})
        result = await proc.process(make_event(), ctx, make_services())
        assert result.verdict is Verdict.DELIVER
        assert slept == [1.5]

    async def test_5xx_exhausts_retries_then_processing_error(self) -> None:
        proc, fake = make_proc([status(500), status(503), status(500)])
        with pytest.raises(ProcessingError, match='исчерпаны попытки'):
            await proc.process(
                make_event(), make_context(settings=LLM_SETTINGS), make_services()
            )
        assert len(fake.calls) == 3  # max_attempts по умолчанию

    async def test_transport_error_retried_then_success(self) -> None:
        proc, fake = make_proc([LlmTransportError('boom'), ok('готово')])
        result = await proc.process(
            make_event(), make_context(settings=LLM_SETTINGS), make_services()
        )
        assert result.outbound[0].text == 'готово'
        assert len(fake.calls) == 2

    async def test_transport_error_exhausted(self) -> None:
        proc, fake = make_proc([LlmTransportError('x')] * 3)
        with pytest.raises(ProcessingError):
            await proc.process(
                make_event(), make_context(settings=LLM_SETTINGS), make_services()
            )
        assert len(fake.calls) == 3

    @pytest.mark.parametrize('code', [400, 401, 404, 422])
    async def test_4xx_not_retried(self, code: int) -> None:
        """4xx≠429 — без ретраев, сразу ProcessingError (Q6)."""
        proc, fake = make_proc([status(code)])
        with pytest.raises(ProcessingError, match='отклонён'):
            await proc.process(
                make_event(), make_context(settings=LLM_SETTINGS), make_services()
            )
        assert len(fake.calls) == 1

    async def test_prompts_rendered_via_jinja(self) -> None:
        proc, fake = make_proc([ok('r')])
        event = make_event(text='привет мир')
        await proc.process(event, make_context(settings=LLM_SETTINGS), make_services())
        messages = fake.calls[0]['payload']['messages']
        assert messages[0] == {'role': 'system', 'content': 'Ты — суммаризатор.'}
        assert messages[1] == {'role': 'user', 'content': 'Сократи: привет мир'}

    async def test_per_kind_user_prompt_for_edited(self) -> None:
        proc, fake = make_proc([ok('r')])
        event = make_event(
            kind=EventKind.MESSAGE_EDITED, text='новый', previous_text='старый'
        )
        ctx = make_context(
            settings={**LLM_SETTINGS, 'user_prompt_edited': '{{ previous_text }} => {{ text }}'}
        )
        await proc.process(event, ctx, make_services())
        assert fake.calls[0]['payload']['messages'][1]['content'] == 'старый => новый'

    async def test_per_kind_user_prompt_for_deleted(self) -> None:
        proc, fake = make_proc([ok('r')])
        # DELETED с восстановленным реестром текстом (§4.2) → промпт строится
        event = make_event(kind=EventKind.MESSAGE_DELETED, text='был текст')
        ctx = make_context(
            settings={**LLM_SETTINGS, 'user_prompt_deleted': 'удалено: {{ text }}'}
        )
        await proc.process(event, ctx, make_services())
        assert fake.calls[0]['payload']['messages'][1]['content'] == 'удалено: был текст'

    async def test_generation_params_included_when_set(self) -> None:
        proc, fake = make_proc([ok('r')])
        ctx = make_context(settings={**LLM_SETTINGS, 'temperature': 0.2, 'max_tokens': 256})
        await proc.process(make_event(), ctx, make_services())
        payload = fake.calls[0]['payload']
        assert payload['temperature'] == 0.2
        assert payload['max_tokens'] == 256

    async def test_generation_params_omitted_when_unset(self) -> None:
        proc, fake = make_proc([ok('r')])
        await proc.process(
            make_event(), make_context(settings=LLM_SETTINGS), make_services()
        )
        payload = fake.calls[0]['payload']
        assert 'temperature' not in payload
        assert 'max_tokens' not in payload

    async def test_invalid_config_raises_config_error(self) -> None:
        proc, _ = make_proc([])
        ctx = make_context(settings={'model': 'x'})  # нет base_url/промптов
        with pytest.raises(ConfigError, match='processor_config'):
            await proc.process(make_event(), ctx, make_services())

    async def test_bad_prompt_syntax_raises_config_error(self) -> None:
        """Битый синтаксис Jinja2 в промпте → ConfigError, без вызова модели."""
        proc, fake = make_proc([])
        ctx = make_context(settings={**LLM_SETTINGS, 'user_prompt': '{{ text '})
        with pytest.raises(ConfigError, match='processor_config'):
            await proc.process(make_event(), ctx, make_services())
        assert fake.calls == []

    async def test_config_cached_per_pipeline(self) -> None:
        proc, fake = make_proc([ok('a'), ok('b')])
        ctx = make_context(settings=LLM_SETTINGS)
        first = await proc.process(make_event(text='1'), ctx, make_services())
        second = await proc.process(make_event(text='2'), ctx, make_services())
        assert first.outbound[0].text == 'a'
        assert second.outbound[0].text == 'b'
        assert len(fake.calls) == 2

    async def test_api_key_env_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv('ANGARION_TEST_LLM_KEY', raising=False)
        proc, _ = make_proc([])
        ctx = make_context(settings={**LLM_SETTINGS, 'api_key_env': 'ANGARION_TEST_LLM_KEY'})
        with pytest.raises(ConfigError, match='api_key_env'):
            await proc.process(make_event(), ctx, make_services())

    async def test_api_key_env_read_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv('ANGARION_TEST_LLM_KEY', 'sk-secret')
        proc, fake = make_proc([ok('r')])
        ctx = make_context(settings={**LLM_SETTINGS, 'api_key_env': 'ANGARION_TEST_LLM_KEY'})
        await proc.process(make_event(), ctx, make_services())
        assert fake.calls[0]['api_key'] == 'sk-secret'


class TestHttpxLlmClient:
    async def test_success_returns_parsed_result(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={'choices': [{'message': {'content': 'hi'}}]})

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        result = await client.post_chat(
            base_url='http://x/v1', payload={'model': 'm'}, api_key=None, timeout_s=5
        )
        assert result.status_code == 200
        assert ChatCompletionResponse.model_validate(result.body).content == 'hi'

    async def test_builds_chat_completions_url(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen['url'] = str(request.url)
            return httpx.Response(200, json={})

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        await client.post_chat(
            base_url='http://x/v1/', payload={}, api_key=None, timeout_s=5
        )
        assert seen['url'] == 'http://x/v1/chat/completions'

    async def test_authorization_header_set_with_key(self) -> None:
        seen: dict[str, str | None] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen['auth'] = request.headers.get('authorization')
            return httpx.Response(200, json={})

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        await client.post_chat(
            base_url='http://x/v1', payload={}, api_key='sk-1', timeout_s=5
        )
        assert seen['auth'] == 'Bearer sk-1'

    async def test_no_authorization_header_without_key(self) -> None:
        seen: dict[str, str | None] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen['auth'] = request.headers.get('authorization')
            return httpx.Response(200, json={})

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        await client.post_chat(
            base_url='http://x/v1', payload={}, api_key=None, timeout_s=5
        )
        assert seen['auth'] is None

    async def test_network_error_becomes_transport_error(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            msg = 'connection refused'
            raise httpx.ConnectError(msg)

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        with pytest.raises(LlmTransportError):
            await client.post_chat(
                base_url='http://x/v1', payload={}, api_key=None, timeout_s=5
            )

    async def test_retry_after_header_parsed(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(429, headers={'Retry-After': '7'})

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        result = await client.post_chat(
            base_url='http://x/v1', payload={}, api_key=None, timeout_s=5
        )
        assert result.status_code == 429
        assert result.retry_after == 7.0

    async def test_retry_after_http_date_ignored(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                429, headers={'Retry-After': 'Wed, 21 Oct 2026 07:28:00 GMT'}
            )

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        result = await client.post_chat(
            base_url='http://x/v1', payload={}, api_key=None, timeout_s=5
        )
        assert result.retry_after is None

    async def test_non_json_body_tolerated(self) -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text='<html>error</html>')

        client = HttpxLlmClient(transport=httpx.MockTransport(handler))
        result = await client.post_chat(
            base_url='http://x/v1', payload={}, api_key=None, timeout_s=5
        )
        assert result.status_code == 500
        assert result.body == {}
