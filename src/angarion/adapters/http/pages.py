"""
Загрузка пользовательских UI-страниц из entry points (§12.6, T029):
группа ``angarion.pages`` — шов для чистого CLI (``angarion run
--with-api``), симметричный ``angarion.processors`` / ``angarion.adapters``.

Раннер web-ролей (``server._make_server``) резолвит зарегистрированные
``Page`` и передаёт их в ``create_app(pages=...)``: установленный пакет с
entry point ``angarion.pages`` отдаёт страницу в навигацию дашборда **без
собственного лаунчера** (раньше — только через свой composition root, как
``examples/web/run.py``).

Загрузчик живёт в http-адаптере, а не в ядровом fastapi-free
``bootstrap`` (§14.9): ``Page`` несёт ``APIRouter``, тянущий FastAPI.

Без ``from __future__ import annotations``: единый стиль http-слоя.
"""

from importlib.metadata import entry_points
from typing import Final

from angarion.adapters.http.templating import Page
from angarion.domain.errors import ConfigError
from angarion.log import get_logger

PAGES_GROUP: Final = 'angarion.pages'

_log = get_logger('angarion.http.pages')


def load_pages() -> list[Page]:
    """
    Резолвить страницы группы ``angarion.pages`` (§12.6, T029).

    Каждый entry point обязан загружаться в ``Page``; иначе — ``ConfigError``
    (симметрично проверке типа в ``load_processors`` / ``_load_group``).
    Entry point с неустановленной зависимостью (опциональный extra, C-1
    T003) пропускается с предупреждением, а не роняет сборку приложения.
    Порядок навигации детерминирован сортировкой по ``path`` (порядок
    entry points между окружениями не гарантирован).
    """
    pages: list[Page] = []
    for ep in entry_points(group=PAGES_GROUP):
        try:
            obj = ep.load()
        except ModuleNotFoundError as exc:
            _log.warning(
                'entry_point_unavailable',
                group=PAGES_GROUP,
                name=ep.name,
                error=str(exc),
            )
            continue
        if not isinstance(obj, Page):
            msg = (
                f'entry point {ep.name!r} группы {PAGES_GROUP!r} '
                f'должен быть Page, получен {type(obj).__name__}'
            )
            raise ConfigError(msg)
        pages.append(obj)
    return sorted(pages, key=lambda page: page.path)
