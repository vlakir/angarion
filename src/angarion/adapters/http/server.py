"""
ASGI-раннер web-ролей (§12.9, FR-5; T024, M5/C, фаза 4): uvicorn поверх
фабрики ``create_app``.

Два режима запуска (§12.9):

- **api** (``--role api``) — отдельный web-процесс: producer командного
  outbox (admin-операции, notify-заявки) + дашборд. Дотягивается до
  хранилища и очереди (``build_storage``/``build_queue``) без построения
  конвейера и подключения Telethon-клиентов; pipeline-процесс исполняет
  команды consumer'ом.
- **combined** (``run --with-api``) — uvicorn как asyncio-задача рядом с
  конвейером в одном процессе: producer и consumer outbox в нём же.
  ``restart_pipeline`` (или сигнал) гасит весь процесс — by design (§3.2),
  супервизор поднимает.

Раннер живёт в http-адаптере (extra ``web``): uvicorn/FastAPI тянет
только он, ядро остаётся fastapi-free (§14.9). Импорт uvicorn — в шапке
модуля; модуль импортируется лишь на web-ветке запуска.

Без ``from __future__ import annotations``: единый стиль http-слоя.
"""

import asyncio
import contextlib

import uvicorn

from angarion.adapters.http.app import create_app
from angarion.adapters.http.auth.bootstrap import ensure_admin
from angarion.adapters.http.composition import (
    build_settings_notifier,
    build_web_deps,
)
from angarion.adapters.http.deps import AngarionDeps
from angarion.adapters.http.pages import load_pages
from angarion.bootstrap import (
    AngarionApp,
    build_app,
    build_queue,
    build_storage,
)
from angarion.config import AngarionSettings
from angarion.domain.plugin import StorageBundle
from angarion.log import get_logger

_log = get_logger('angarion.http.server')


def _make_server(deps: AngarionDeps) -> uvicorn.Server:
    """Собрать ``uvicorn.Server`` поверх ``create_app(deps)`` по ``[api]``."""
    api = deps.settings.api
    # Пользовательские страницы из entry points (§12.6, T029): чистый CLI
    # (run --with-api / --role api) монтирует их без своего лаунчера.
    app = create_app(deps, pages=load_pages())
    config = uvicorn.Config(
        app, host=api.host, port=api.port, log_level='info', access_log=False
    )
    return uvicorn.Server(config)


async def _bootstrap_admin(deps: AngarionDeps) -> None:
    """Создать bootstrap-админа на пустой таблице (FR-0); ``auth="none"`` — пропуск."""
    if deps.auth_sessionmaker is not None:
        await ensure_admin(deps.auth_sessionmaker, deps.settings)


async def _dispose(storage: StorageBundle) -> None:
    """Закрыть ресурсы хранилища, если бэкенд их держит (sqlite — пул движка)."""
    dispose = getattr(storage, 'dispose', None)
    if callable(dispose):
        await dispose()


async def _wait_any(*events: asyncio.Event) -> None:
    """Ждать срабатывания любого из событий (стоп-сигнал / restart)."""
    waiters = [asyncio.ensure_future(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()
        for waiter in waiters:
            with contextlib.suppress(asyncio.CancelledError):
                await waiter


async def serve_api(settings: AngarionSettings, stop: asyncio.Event) -> None:
    """
    api-роль (§12.9): web-процесс поверх хранилища и очереди, без
    конвейера. uvicorn держится до стоп-сигнала, затем закрывает БД.
    """
    storage = build_storage(settings)
    queue = build_queue(settings)
    deps = build_web_deps(settings, storage, queue, notifier=build_settings_notifier())
    await _bootstrap_admin(deps)
    server = _make_server(deps)
    serve_task = asyncio.create_task(server.serve(), name='angarion-uvicorn')
    _log.info('api_started', host=settings.api.host, port=settings.api.port)
    try:
        await stop.wait()
    finally:
        server.should_exit = True
        await serve_task
        await _dispose(storage)
    _log.info('api_stopped')


async def serve_combined(settings: AngarionSettings, stop: asyncio.Event) -> None:
    """
    combined-роль (``run --with-api``): конвейер + uvicorn в одном
    процессе. Останавливается по стоп-сигналу ИЛИ команде
    ``restart_pipeline`` (через ``app.restart_event``) — гасит весь
    процесс, супервизор поднимает (§3.2).
    """
    app: AngarionApp = build_app(settings)
    notifier = build_settings_notifier()
    deps = build_web_deps(settings, app.storage, app.queue, notifier=notifier)
    await _bootstrap_admin(deps)
    server = _make_server(deps)
    await app.start()
    serve_task = asyncio.create_task(server.serve(), name='angarion-uvicorn')
    _log.info('combined_started', host=settings.api.host, port=settings.api.port)
    try:
        await _wait_any(stop, app.restart_event)
    finally:
        server.should_exit = True
        await serve_task
        await app.stop()
    _log.info('combined_stopped')
