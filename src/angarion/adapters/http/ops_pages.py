"""
SSR-страницы админ-операций (§12.8, FR-4; T024, фаза 4): ``/ui/settings``
и ``/ui/dlq`` — admin-only (роутер закрыт ``require_admin`` в
``create_app``).

``/ui/settings`` — зона «Конфигурация» (read-only, секреты маскированы) +
зона «Управление» (формы динамики с пометкой источника файл/override) +
пауза/возобновление пайплайнов + restart/catchup. ``/ui/dlq`` — список
DLQ с кнопкой requeue. Формы зовут те же сервисные функции, что и
JSON-ручки ``/api/v1/admin`` (``ops``), и редиректят обратно на страницу
(POST→redirect→GET).

Без ``from __future__ import annotations``: DI fastapi и формы
вычисляются в runtime.
"""

import uuid
from collections.abc import Callable
from typing import Annotated, Any

from fastapi import APIRouter, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from angarion.adapters.http._data import source_keys
from angarion.adapters.http.deps import get_deps
from angarion.adapters.http.ops import (
    request_catchup,
    request_restart,
    requeue_dead_letter,
    reset_setting,
    save_settings,
    set_pause,
)
from angarion.adapters.http.ui import render_pipelines_fragment
from angarion.config import AngarionSettings
from angarion.domain.models import DynamicSettings

router_ops = APIRouter(prefix='/ui', tags=['ui-ops'])

_MASK = '••••'
"""Маска секрета в read-only зоне «Конфигурация» (§12.8/§17.7)."""


def _templates(request: Request) -> Jinja2Templates:
    templates: Jinja2Templates = request.app.state.templates
    return templates


def _static_view(settings: AngarionSettings) -> list[dict[str, str]]:
    """Read-only снимок статической конфигурации с маскированием секретов."""
    api = settings.api
    sender = settings.telegram.sender
    catchup = settings.catchup
    secret = _MASK if api.secret else ''
    return [
        {'key': 'api.host', 'value': str(api.host)},
        {'key': 'api.port', 'value': str(api.port)},
        {'key': 'api.auth', 'value': str(api.auth)},
        {'key': 'api.secret', 'value': secret},
        {'key': 'api.registration_enabled', 'value': str(api.registration_enabled)},
        {
            'key': 'api.max_pending_registrations',
            'value': str(api.max_pending_registrations),
        },
        {
            'key': 'telegram.sender.chat_per_second',
            'value': str(sender.chat_per_second),
        },
        {
            'key': 'telegram.sender.account_per_minute',
            'value': str(sender.account_per_minute),
        },
        {
            'key': 'catchup.max_messages_per_source',
            'value': str(catchup.max_messages_per_source),
        },
        {'key': 'catchup.max_age_days', 'value': str(catchup.max_age_days)},
    ]


def _override_view(overrides: DynamicSettings) -> list[dict[str, Any]]:
    """Поля динамики с текущим override'ом и пометкой источника файл/override."""
    data = overrides.model_dump(mode='json')
    rows: list[dict[str, Any]] = []
    for key, value in data.items():
        if key == 'paused_pipelines':
            continue
        rows.append(
            {
                'key': key,
                'value': '' if value is None else value,
                'source': 'override' if value is not None else 'file',
            }
        )
    return rows


async def _settings_context(request: Request) -> dict[str, Any]:
    """Контекст ``/ui/settings``: статика, override'ы, пайплайны, источники."""
    deps = get_deps(request)
    overrides = await deps.runtime_config.load()
    paused = overrides.paused_pipelines or frozenset()
    pipelines = [
        {'name': name, 'paused': name in paused}
        for name in sorted(deps.settings.pipelines)
    ]
    return {
        'static_rows': _static_view(deps.settings),
        'override_rows': _override_view(overrides),
        'pipelines': pipelines,
        'source_keys': source_keys(deps.settings),
    }


@router_ops.get('/settings', response_class=HTMLResponse)
async def settings_page(request: Request) -> HTMLResponse:
    """Конфигурация (read-only) + управление динамикой (admin)."""
    context = await _settings_context(request)
    return _templates(request).TemplateResponse(
        request, 'angarion/settings.html', context
    )


def _current_login(request: Request) -> str:
    """Логин текущего админа (``require_admin`` положил его в ``request.state``)."""
    user = request.state.user
    return str(user.login)


def _parse_optional[T](raw: str, cast: Callable[[str], T]) -> T | None:
    """Пустая строка формы → ``None`` (нет правки); иначе привести типом."""
    stripped = raw.strip()
    return cast(stripped) if stripped else None


@router_ops.post('/settings')
async def settings_save(
    request: Request,
    log_level: Annotated[str, Form()] = '',
    sender_chat_per_second: Annotated[str, Form()] = '',
    sender_account_per_minute: Annotated[str, Form()] = '',
    catchup_max_messages_per_source: Annotated[str, Form()] = '',
    catchup_max_age_days: Annotated[str, Form()] = '',
) -> Response:
    """Применить непустые поля формы как override'ы динамики (admin)."""
    deps = get_deps(request)
    patch = DynamicSettings(
        log_level=_parse_optional(log_level, str),
        sender_chat_per_second=_parse_optional(sender_chat_per_second, float),
        sender_account_per_minute=_parse_optional(sender_account_per_minute, float),
        catchup_max_messages_per_source=_parse_optional(
            catchup_max_messages_per_source, int
        ),
        catchup_max_age_days=_parse_optional(catchup_max_age_days, int),
    )
    await save_settings(
        deps.runtime_config,
        deps.analytics,
        deps.notifier,
        patch=patch,
        by=_current_login(request),
    )
    return RedirectResponse('/ui/settings', status_code=303)


@router_ops.post('/settings/{key}/reset')
async def settings_reset(request: Request, key: str) -> Response:
    """Снять override поля (возврат к файлу; admin)."""
    deps = get_deps(request)
    await reset_setting(
        deps.runtime_config,
        deps.analytics,
        deps.notifier,
        key=key,
        by=_current_login(request),
    )
    return RedirectResponse('/ui/settings', status_code=303)


async def _set_pause_respond(
    request: Request, pipeline: str, *, paused: bool
) -> Response:
    """
    Применить паузу и ответить по источнику запроса.

    htmx-клик по узлу графа (``/ui/pipelines``, заголовок ``HX-Request``)
    → обновлённый фрагмент графа на месте; обычная форма ``/ui/settings``
    → редирект назад (POST→redirect→GET, T024).
    """
    deps = get_deps(request)
    await set_pause(
        deps.runtime_config,
        deps.analytics,
        deps.notifier,
        pipeline=pipeline,
        paused=paused,
        by=_current_login(request),
    )
    if request.headers.get('HX-Request'):
        return await render_pipelines_fragment(request, _templates(request))
    return RedirectResponse('/ui/settings', status_code=303)


@router_ops.post('/pipelines/{pipeline}/pause')
async def pipeline_pause(request: Request, pipeline: str) -> Response:
    """Поставить пайплайн на паузу (admin)."""
    return await _set_pause_respond(request, pipeline, paused=True)


@router_ops.post('/pipelines/{pipeline}/resume')
async def pipeline_resume(request: Request, pipeline: str) -> Response:
    """Снять пайплайн с паузы (admin)."""
    return await _set_pause_respond(request, pipeline, paused=False)


@router_ops.post('/restart')
async def restart_action(request: Request) -> Response:
    """Заявить graceful-перезапуск pipeline-процесса через outbox (admin)."""
    deps = get_deps(request)
    await request_restart(
        deps.command_outbox, deps.analytics, by=_current_login(request)
    )
    return RedirectResponse('/ui/settings', status_code=303)


@router_ops.post('/catchup')
async def catchup_action(
    request: Request, source_key: Annotated[str, Form()]
) -> Response:
    """Заявить ручной catch-up источника через outbox (admin)."""
    deps = get_deps(request)
    await request_catchup(
        deps.command_outbox,
        deps.analytics,
        source_key=source_key,
        by=_current_login(request),
    )
    return RedirectResponse('/ui/settings', status_code=303)


@router_ops.get('/dlq', response_class=HTMLResponse)
async def dlq_page(request: Request) -> HTMLResponse:
    """Список DLQ с кнопкой requeue (admin)."""
    deps = get_deps(request)
    letters = await deps.dead_letters.list(limit=200)
    return _templates(request).TemplateResponse(
        request, 'angarion/dlq.html', {'letters': letters}
    )


@router_ops.post('/dlq/{uid}/requeue')
async def dlq_requeue(request: Request, uid: uuid.UUID) -> Response:
    """Вернуть запись DLQ в очередь с ``attempt=0`` (admin)."""
    deps = get_deps(request)
    await requeue_dead_letter(
        deps.dead_letters,
        deps.queue,
        deps.analytics,
        uid=uid,
        by=_current_login(request),
    )
    return RedirectResponse('/ui/dlq', status_code=303)
