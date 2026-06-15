"""
SSR-каркас Web UI (§12.6): Jinja2-environment с ``ChoiceLoader``,
дескриптор пользовательской страницы ``Page`` и сборка навигации.

Контракт расширения: пользовательский шаблон начинает с
``{% extends "angarion/base.html" %}`` и получает навигацию, стили и
htmx-автообновление бесплатно. Каталоги пользователя ищутся раньше
встроенного (``ChoiceLoader``), так что любой встроенный шаблон можно
переопределить. Без ``from __future__ import annotations``: поле-роутер
``Page`` — runtime-аннотация pydantic (как у ``AngarionDeps``).
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel, ConfigDict

_BUILTIN_TEMPLATES = Path(__file__).parent / 'templates'


def _user_context(request: Request) -> dict[str, Any]:
    """
    Текущий пользователь в каждый шаблон (``None`` на публичных страницах).

    ``require_user`` кладёт пользователя в ``request.state.user`` — навигация
    рисует ``/ui/users`` и logout по роли (§12.7); здесь — безопасное чтение.
    """
    return {'current_user': getattr(request.state, 'user', None)}


class Page(BaseModel):
    """
    Дескриптор пользовательской UI-страницы (§12.6, spec §5).

    ``router`` — ``APIRouter`` со страничными ручками (HTML-ответы);
    ``title``/``path`` — пункт навигации, автоматически появляющийся в
    шапке дашборда. Передаётся в ``create_app(pages=[Page(...)])``.

    ``public`` (§12.7) — по умолчанию страница закрыта ``CurrentUser``;
    ``public=True`` — осознанно открытая (например, своя страница входа).

    Композиция (не DTO): ``frozen`` с ``arbitrary_types_allowed`` —
    ``APIRouter`` не сериализуется (как у ``AngarionDeps``).
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    title: str
    path: str
    router: APIRouter
    public: bool = False


class NavItem(BaseModel):
    """Пункт навигации шапки: подпись + путь (рендерится в ``base.html``)."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    title: str
    path: str


_BUILTIN_NAV: tuple[NavItem, ...] = (
    NavItem(title='Dashboard', path='/ui'),
    NavItem(title='Events', path='/ui/events'),
)


def build_nav(pages: Sequence[Page]) -> list[NavItem]:
    """Встроенные пункты навигации + пользовательские страницы (в порядке)."""
    return [*_BUILTIN_NAV, *(NavItem(title=p.title, path=p.path) for p in pages)]


def build_templates(
    *, template_dirs: Sequence[Path] = (), nav: Sequence[NavItem] = ()
) -> Jinja2Templates:
    """
    Собрать ``Jinja2Templates`` с ``ChoiceLoader`` (пользователь → пакет).

    Навигация кладётся в ``env.globals['nav']`` — статична на время жизни
    приложения (страницы известны при сборке ``create_app``).
    """
    loaders = [FileSystemLoader(str(d)) for d in template_dirs]
    loaders.append(FileSystemLoader(str(_BUILTIN_TEMPLATES)))
    env = Environment(
        loader=ChoiceLoader(loaders),
        autoescape=select_autoescape(['html', 'xml']),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.globals['nav'] = list(nav)
    return Jinja2Templates(env=env, context_processors=[_user_context])
