"""
Общий Jinja2-движок рендеринга по полям ``Record`` для встроенных
процессоров ``template`` и ``llm`` (FR §3 спеки T007).

Окружение настроено под plain text (W4 спеки):

- ``autoescape=select_autoescape(default_for_string=False)`` — для
  безымянных string-шаблонов (``from_string``) экранирования нет: текст
  сообщений plain, не HTML, и HTML-escaping исказил бы спецсимволы
  (``<``, ``&``). Литеральный ``autoescape=False`` ловится линтером
  (S701); ``select_autoescape`` — bandit-одобренная форма того же
  поведения для наших шаблонов.
- ``finalize`` рендерит ``None`` пустой строкой, а не ``"None"`` (FR).
- ``Undefined`` (а не ``StrictUndefined``): отсутствующий атрибут
  рендерится пусто — чтобы, например, ``previous_text`` у NEW не валил
  рендер (W4).

Контекст рендеринга — поля записи в JSON-форме
(``model_dump(mode='json')``): ``text``, ``previous_text``, ``kind``,
``origin``, ``sender_name``, ``sender_id``, ``external_id``,
``event_at`` и вложенные ``source``/``received_by`` (доступны как
``source.address`` и т.п.).
"""

from jinja2 import Environment, Template, select_autoescape

from angarion.domain.models import Record


def _finalize(value: object) -> object:
    """``None`` → пустая строка, иначе значение как есть (FR §3)."""
    return '' if value is None else value


_ENV = Environment(
    autoescape=select_autoescape(default_for_string=False),
    finalize=_finalize,
)


def compile_record_template(source: str) -> Template:
    """
    Скомпилировать Jinja2-шаблон (``jinja2.TemplateSyntaxError`` при битом
    синтаксисе) — процессоры компилируют один раз при разборе конфига и
    кэшируют, а ошибку синтаксиса оборачивают в ``ConfigError``.
    """
    return _ENV.from_string(source)


def render_compiled(template: Template, record: Record) -> str:
    """Отрендерить скомпилированный шаблон по полям записи (FR §3)."""
    return template.render(record.model_dump(mode='json'))


def render_record_template(source: str, record: Record) -> str:
    """Скомпилировать и отрендерить шаблон-строку (one-shot; компиляция на вызов)."""
    return render_compiled(compile_record_template(source), record)
