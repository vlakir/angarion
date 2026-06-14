"""Общий Jinja2-хелпер рендеринга по полям события (FR §3 спеки T007)."""

from __future__ import annotations

import pytest
from app_factories import make_event
from jinja2 import TemplateSyntaxError

from angarion.application.templating import (
    compile_event_template,
    render_event_template,
)
from angarion.domain.models import EventKind


class TestRenderEventTemplate:
    def test_renders_event_fields(self) -> None:
        """Контекст — поля события: text, sender_name и т.п."""
        event = make_event(text='привет', sender_name='Алиса')
        out = render_event_template('{{ sender_name }}: {{ text }}', event)
        assert out == 'Алиса: привет'

    def test_none_field_renders_empty_not_none(self) -> None:
        """None-поле рендерится пустой строкой, не строкой 'None' (FR, finalize)."""
        event = make_event(text=None, kind=EventKind.MESSAGE_DELETED)
        assert render_event_template('[{{ text }}]', event) == '[]'

    def test_missing_attribute_renders_empty(self) -> None:
        """Нет StrictUndefined: несуществующее поле → пусто, не ошибка (W4)."""
        assert render_event_template('[{{ no_such_field }}]', make_event()) == '[]'

    def test_no_html_escaping(self) -> None:
        """Plain text: спецсимволы не экранируются (W4)."""
        event = make_event(text='a < b & c > d')
        assert render_event_template('{{ text }}', event) == 'a < b & c > d'

    def test_nested_address_field(self) -> None:
        """Вложенные поля события доступны как source.chat_id."""
        event = make_event()
        assert render_event_template('{{ source.chat_id }}', event) == '-100123'

    def test_origin_available(self) -> None:
        """N2: origin (live/catchup) доступен автору шаблона."""
        event = make_event(origin='catchup')
        assert render_event_template('{{ origin }}', event) == 'catchup'

    def test_kind_rendered_as_value(self) -> None:
        """kind рендерится строковым значением StrEnum."""
        event = make_event(kind=EventKind.MESSAGE_NEW)
        assert render_event_template('{{ kind }}', event) == 'message_new'

    def test_compile_bad_syntax_raises(self) -> None:
        """Битый синтаксис → TemplateSyntaxError (процессоры → ConfigError)."""
        with pytest.raises(TemplateSyntaxError):
            compile_event_template('{{ text ')
