"""Общий Jinja2-хелпер рендеринга по полям события (FR §3 спеки T007)."""

from __future__ import annotations

import pytest
from app_factories import make_record
from jinja2 import TemplateSyntaxError

from angarion.application.templating import (
    compile_record_template,
    render_record_template,
)
from angarion.domain.models import RecordKind


class TestRenderEventTemplate:
    def test_renders_event_fields(self) -> None:
        """Контекст — поля события: text, sender_name и т.п."""
        event = make_record(text='привет', sender_name='Алиса')
        out = render_record_template('{{ sender_name }}: {{ text }}', event)
        assert out == 'Алиса: привет'

    def test_none_field_renders_empty_not_none(self) -> None:
        """None-поле рендерится пустой строкой, не строкой 'None' (FR, finalize)."""
        event = make_record(text=None, kind=RecordKind.DELETED)
        assert render_record_template('[{{ text }}]', event) == '[]'

    def test_missing_attribute_renders_empty(self) -> None:
        """Нет StrictUndefined: несуществующее поле → пусто, не ошибка (W4)."""
        assert render_record_template('[{{ no_such_field }}]', make_record()) == '[]'

    def test_no_html_escaping(self) -> None:
        """Plain text: спецсимволы не экранируются (W4)."""
        event = make_record(text='a < b & c > d')
        assert render_record_template('{{ text }}', event) == 'a < b & c > d'

    def test_nested_address_field(self) -> None:
        """Вложенные поля события доступны как source.address."""
        event = make_record()
        assert render_record_template('{{ source.address }}', event) == '-100123'

    def test_origin_available(self) -> None:
        """N2: origin (live/catchup) доступен автору шаблона."""
        event = make_record(origin='catchup')
        assert render_record_template('{{ origin }}', event) == 'catchup'

    def test_kind_rendered_as_value(self) -> None:
        """Kind рендерится строковым значением StrEnum."""
        event = make_record(kind=RecordKind.NEW)
        assert render_record_template('{{ kind }}', event) == 'new'

    def test_compile_bad_syntax_raises(self) -> None:
        """Битый синтаксис → TemplateSyntaxError (процессоры → ConfigError)."""
        with pytest.raises(TemplateSyntaxError):
            compile_record_template('{{ text ')
