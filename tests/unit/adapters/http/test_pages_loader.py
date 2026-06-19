"""
Загрузка пользовательских UI-страниц из entry points (§12.6, T029):
группа ``angarion.pages``. Контракт загрузки (резолв / сортировка /
пропуск отсутствующего extra / проверка типа) + сквозная провязка:
зарегистрированная страница попадает в навигацию и монтируется как
``create_app(pages=load_pages())``, в т.ч. через раннер ``_make_server``
(путь ``angarion run --with-api``) — без кастомного лаунчера.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from angarion.adapters.http import Page, create_app, server
from angarion.adapters.http import pages as pages_mod
from angarion.adapters.http.pages import load_pages
from angarion.domain.errors import ConfigError

from conftest import asgi_client

if TYPE_CHECKING:
    from angarion.adapters.http import AngarionDeps


class FakeEntryPoint:
    """Duck-type entry point: загрузчику нужны ``name`` и ``load()``."""

    def __init__(
        self, name: str, obj: object, error: Exception | None = None
    ) -> None:
        self.name = name
        self._obj = obj
        self._error = error

    def load(self) -> object:
        if self._error is not None:
            raise self._error
        return self._obj


def _patch_entry_points(
    monkeypatch: pytest.MonkeyPatch, eps: list[FakeEntryPoint]
) -> None:
    def fake_entry_points(*, group: str) -> list[FakeEntryPoint]:
        return eps if group == pages_mod.PAGES_GROUP else []

    monkeypatch.setattr(pages_mod, 'entry_points', fake_entry_points)


def _page(title: str, path: str) -> Page:
    router = APIRouter()

    @router.get(path, response_class=HTMLResponse)
    async def _handler() -> HTMLResponse:
        return HTMLResponse(f'<p>{title} body</p>')

    return Page(title=title, path=path, router=router, public=True)


def test_load_pages_resolves_and_sorts_by_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_entry_points(
        monkeypatch,
        [
            FakeEntryPoint('beta', _page('Beta', '/ui/beta')),
            FakeEntryPoint('alpha', _page('Alpha', '/ui/alpha')),
        ],
    )
    assert [page.path for page in load_pages()] == ['/ui/alpha', '/ui/beta']


def test_load_pages_skips_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Entry point с неустановленной зависимостью пропускается, не роняя сборку."""
    _patch_entry_points(
        monkeypatch,
        [
            FakeEntryPoint('broken', None, error=ModuleNotFoundError('no mod')),
            FakeEntryPoint('ok', _page('Ok', '/ui/ok')),
        ],
    )
    assert [page.title for page in load_pages()] == ['Ok']


def test_load_pages_wrong_type_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_entry_points(monkeypatch, [FakeEntryPoint('bad', object())])
    with pytest.raises(ConfigError, match='Page'):
        load_pages()


def test_load_pages_empty_without_entry_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_entry_points(monkeypatch, [])
    assert load_pages() == []


@pytest.mark.asyncio
async def test_entry_point_page_appears_in_nav_and_mounts(
    monkeypatch: pytest.MonkeyPatch, deps: AngarionDeps
) -> None:
    _patch_entry_points(monkeypatch, [FakeEntryPoint('ext', _page('Ext', '/ui/ext'))])
    app = create_app(deps, pages=load_pages())
    async with asgi_client(app) as client:
        home = (await client.get('/ui')).text
        assert 'Ext' in home
        assert 'href="/ui/ext"' in home
        page = await client.get('/ui/ext')
        assert page.status_code == 200
        assert 'Ext body' in page.text


def test_make_server_passes_entry_point_pages_to_create_app(
    monkeypatch: pytest.MonkeyPatch, deps: AngarionDeps
) -> None:
    """Раннер (``angarion run --with-api``) передаёт страницы в ``create_app``."""
    _patch_entry_points(monkeypatch, [FakeEntryPoint('ext', _page('Ext', '/ui/ext'))])
    captured: dict[str, object] = {}
    real_create_app = server.create_app

    def spy_create_app(deps_arg: AngarionDeps, **kwargs: object) -> object:
        captured['pages'] = kwargs.get('pages', ())
        return real_create_app(deps_arg, **kwargs)

    monkeypatch.setattr(server, 'create_app', spy_create_app)
    server._make_server(deps)
    captured_pages = captured['pages']
    assert isinstance(captured_pages, list)
    assert [page.path for page in captured_pages] == ['/ui/ext']
