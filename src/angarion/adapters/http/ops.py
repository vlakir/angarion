"""
Админ-операции и динамические настройки (§12.8, FR-4; T024, M5/C, фаза 4).

Сервисные функции (пауза/возобновление пайплайна, сохранение/сброс
динамики, requeue из DLQ, restart/catchup через командный outbox) +
**JSON-ручки** ``/api/v1/admin`` — единственное write-исключение из
read-only встроенного API, доступны только ``admin`` (роутер закрыт
``require_admin``; viewer → 403). Те же сервисные функции переиспользуют
htmx-страницы ``/ui/settings`` и ``/ui/dlq`` (``ops_pages``).

Каждая операция и каждое изменение динамики пишет аудит — событие
``admin_op`` в аналитику (пользователь, операция, старое/новое), видно в
``/ui/events`` (§12.8). ``restart``/``catchup`` исполняет не api-процесс:
они ставятся в командный outbox и исполняются consumer'ом pipeline-
процесса (§12.9) — в комбинированном режиме тем же процессом.

Без ``from __future__ import annotations``: pydantic-схемы и DI fastapi
вычисляются в runtime.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from angarion.adapters.http.auth.deps import AdminUser
from angarion.adapters.http.deps import (
    AnalyticsDep,
    CommandOutboxDep,
    DeadLettersDep,
    NotifierDep,
    QueueDep,
    RuntimeConfigDep,
)
from angarion.application.settings import SettingsNotifier
from angarion.domain.models import (
    AnalyticsEvent,
    CommandKind,
    DeadLetter,
    DynamicSettings,
    OutboxCommand,
    Record,
)
from angarion.domain.ports import (
    AnalyticsPort,
    CommandOutboxPort,
    DeadLetterPort,
    EventQueuePort,
    RuntimeConfigPort,
)

ADMIN_OP = 'admin_op'
"""Вид события аудита админ-операций (§12.8)."""


async def record_admin_op(
    analytics: AnalyticsPort,
    *,
    operation: str,
    by: str,
    pipeline: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Записать аудит ``admin_op`` (пользователь, операция, детали; §12.8)."""
    await analytics.record(
        AnalyticsEvent(
            uid=uuid.uuid4(),
            kind=ADMIN_OP,
            pipeline=pipeline,
            payload={'operation': operation, 'by': by, **(details or {})},
            at=datetime.now(UTC),
        )
    )


async def _notify(notifier: SettingsNotifier | None, settings: DynamicSettings) -> None:
    """Оповестить in-process подписчиков (уровень лога), если notifier задан."""
    if notifier is not None:
        await notifier.notify(settings)


async def set_pause(
    runtime_config: RuntimeConfigPort,
    analytics: AnalyticsPort,
    notifier: SettingsNotifier | None,
    *,
    pipeline: str,
    paused: bool,
    by: str,
) -> DynamicSettings:
    """
    Поставить/снять пайплайн с паузы (§12.8): правит динамический
    ``paused_pipelines`` и оповещает подписчиков. Пауза ≠ потеря —
    worker откладывает envelope'ы паузнутого пайплайна в хвост (FR-4).
    """
    current = (await runtime_config.load()).paused_pipelines or frozenset()
    new = current | {pipeline} if paused else current - {pipeline}
    settings = await runtime_config.save(
        DynamicSettings(paused_pipelines=new), updated_by=by
    )
    await _notify(notifier, settings)
    await record_admin_op(
        analytics,
        operation='pause' if paused else 'resume',
        by=by,
        pipeline=pipeline,
    )
    return settings


async def save_settings(
    runtime_config: RuntimeConfigPort,
    analytics: AnalyticsPort,
    notifier: SettingsNotifier | None,
    *,
    patch: DynamicSettings,
    by: str,
) -> DynamicSettings:
    """Применить override'ы динамики (sparse), оповестить, записать аудит."""
    before = await runtime_config.load()
    settings = await runtime_config.save(patch, updated_by=by)
    await _notify(notifier, settings)
    await record_admin_op(
        analytics,
        operation='settings_update',
        by=by,
        details={
            'changed': sorted(patch.model_dump(exclude_none=True)),
            'old': before.model_dump(mode='json', exclude_none=True),
            'new': settings.model_dump(mode='json', exclude_none=True),
        },
    )
    return settings


async def reset_setting(
    runtime_config: RuntimeConfigPort,
    analytics: AnalyticsPort,
    notifier: SettingsNotifier | None,
    *,
    key: str,
    by: str,
) -> DynamicSettings:
    """Снять override поля (возврат к файлу), оповестить, записать аудит."""
    settings = await runtime_config.reset(key)
    await _notify(notifier, settings)
    await record_admin_op(
        analytics, operation='settings_reset', by=by, details={'key': key}
    )
    return settings


async def requeue_dead_letter(
    dead_letters: DeadLetterPort,
    queue: EventQueuePort,
    analytics: AnalyticsPort,
    *,
    uid: uuid.UUID,
    by: str,
) -> DeadLetter:
    """
    Вернуть запись DLQ в очередь с ``attempt=0`` (§12.8): изъять из DLQ,
    поставить envelope заново (счётчик попыток обнулён, отложка снята),
    записать аудит с ``requeued_at``. Неизвестный uid → 404.
    """
    letter = await dead_letters.take(uid)
    if letter is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, 'dead letter not found')
    requeued = letter.envelope.model_copy(update={'attempt': 0, 'not_before': None})
    await queue.put(requeued)
    now = datetime.now(UTC)
    await record_admin_op(
        analytics,
        operation='requeue',
        by=by,
        pipeline=letter.envelope.pipeline,
        details={'uid': str(uid), 'requeued_at': now.isoformat()},
    )
    return letter


async def request_restart(
    command_outbox: CommandOutboxPort, analytics: AnalyticsPort, *, by: str
) -> OutboxCommand:
    """Поставить команду graceful-перезапуска pipeline-процесса (§12.9)."""
    command = await command_outbox.put(CommandKind.RESTART_PIPELINE)
    await record_admin_op(
        analytics, operation='restart', by=by, details={'command_uid': str(command.uid)}
    )
    return command


async def request_catchup(
    command_outbox: CommandOutboxPort,
    analytics: AnalyticsPort,
    *,
    source_key: str,
    by: str,
) -> OutboxCommand:
    """Поставить команду ручного catch-up источника (§12.9)."""
    command = await command_outbox.put(
        CommandKind.CATCHUP, payload={'source_key': source_key}
    )
    await record_admin_op(
        analytics,
        operation='catchup',
        by=by,
        details={'source_key': source_key, 'command_uid': str(command.uid)},
    )
    return command


async def request_inject(
    command_outbox: CommandOutboxPort,
    analytics: AnalyticsPort,
    *,
    record: Record,
    by: str,
) -> OutboxCommand:
    """
    Поставить команду ручного впрыска события (T038, split event-мост).

    ``Record`` сериализуется в payload команды; consumer pipeline-процесса
    десериализует и проводит через ``IngestService.ingest`` (router/dedup/
    реестр/fan-out). Используется только в split (``--role api``): в combined
    ручка зовёт ``ingest`` напрямую, минуя outbox (меньше латентность, A8/A1).
    """
    command = await command_outbox.put(
        CommandKind.INJECT, payload={'record': record.model_dump(mode='json')}
    )
    await record_admin_op(
        analytics,
        operation='inject',
        by=by,
        pipeline=None,
        details={'record_uid': str(record.uid), 'command_uid': str(command.uid)},
    )
    return command


# --- JSON-ручки /api/v1/admin (admin-only; роутер закрыт require_admin) ---

router = APIRouter(prefix='/api/v1/admin', tags=['admin'])


class SettingsPatch(BaseModel):
    """Частичный override динамики (не-``None`` поля применяются)."""

    paused_pipelines: list[str] | None = None
    registration_enabled: bool | None = None
    max_pending_registrations: int | None = None
    log_level: str | None = None
    sender_chat_per_second: float | None = None
    sender_account_per_minute: float | None = None
    catchup_max_messages_per_source: int | None = None
    catchup_max_age_days: int | None = None


class CatchupRequest(BaseModel):
    """Тело запроса ручного catch-up: ключ источника."""

    source_key: str


class CommandAccepted(BaseModel):
    """Подтверждение постановки команды в outbox (202)."""

    command_uid: uuid.UUID
    kind: str


def _to_patch(body: SettingsPatch) -> DynamicSettings:
    """Схема API → доменный ``DynamicSettings`` (frozenset из списка)."""
    data = body.model_dump(exclude_none=True)
    if 'paused_pipelines' in data:
        data['paused_pipelines'] = frozenset(data['paused_pipelines'])
    return DynamicSettings(**data)


@router.get('/settings')
async def get_settings(runtime_config: RuntimeConfigDep) -> dict[str, Any]:
    """Текущие override'ы динамики (``None``-поля исключены)."""
    return (await runtime_config.load()).model_dump(mode='json', exclude_none=True)


@router.put('/settings')
async def put_settings(
    body: SettingsPatch,
    admin: AdminUser,
    runtime_config: RuntimeConfigDep,
    analytics: AnalyticsDep,
    notifier: NotifierDep,
) -> dict[str, Any]:
    """Применить override'ы динамики (admin)."""
    settings = await save_settings(
        runtime_config, analytics, notifier, patch=_to_patch(body), by=admin.login
    )
    return settings.model_dump(mode='json', exclude_none=True)


@router.delete('/settings/{key}')
async def delete_setting(
    key: str,
    admin: AdminUser,
    runtime_config: RuntimeConfigDep,
    analytics: AnalyticsDep,
    notifier: NotifierDep,
) -> dict[str, Any]:
    """Снять override поля (возврат к файлу; admin)."""
    settings = await reset_setting(
        runtime_config, analytics, notifier, key=key, by=admin.login
    )
    return settings.model_dump(mode='json', exclude_none=True)


@router.post('/pipelines/{pipeline}/pause', status_code=status.HTTP_204_NO_CONTENT)
async def pause(
    pipeline: str,
    admin: AdminUser,
    runtime_config: RuntimeConfigDep,
    analytics: AnalyticsDep,
    notifier: NotifierDep,
) -> None:
    """Поставить пайплайн на паузу (admin); пауза ≠ потеря (§12.8)."""
    await set_pause(
        runtime_config,
        analytics,
        notifier,
        pipeline=pipeline,
        paused=True,
        by=admin.login,
    )


@router.post('/pipelines/{pipeline}/resume', status_code=status.HTTP_204_NO_CONTENT)
async def resume(
    pipeline: str,
    admin: AdminUser,
    runtime_config: RuntimeConfigDep,
    analytics: AnalyticsDep,
    notifier: NotifierDep,
) -> None:
    """Снять пайплайн с паузы (admin)."""
    await set_pause(
        runtime_config,
        analytics,
        notifier,
        pipeline=pipeline,
        paused=False,
        by=admin.login,
    )


@router.post('/dlq/{uid}/requeue', status_code=status.HTTP_204_NO_CONTENT)
async def requeue(
    uid: uuid.UUID,
    admin: AdminUser,
    dead_letters: DeadLettersDep,
    queue: QueueDep,
    analytics: AnalyticsDep,
) -> None:
    """Вернуть запись DLQ в очередь с ``attempt=0`` (admin)."""
    await requeue_dead_letter(dead_letters, queue, analytics, uid=uid, by=admin.login)


@router.post(
    '/restart',
    status_code=status.HTTP_202_ACCEPTED,
)
async def restart(
    admin: AdminUser, command_outbox: CommandOutboxDep, analytics: AnalyticsDep
) -> CommandAccepted:
    """Заявить graceful-перезапуск pipeline-процесса через outbox (admin)."""
    command = await request_restart(command_outbox, analytics, by=admin.login)
    return CommandAccepted(command_uid=command.uid, kind=command.kind.value)


@router.post(
    '/catchup',
    status_code=status.HTTP_202_ACCEPTED,
)
async def catchup(
    body: CatchupRequest,
    admin: AdminUser,
    command_outbox: CommandOutboxDep,
    analytics: AnalyticsDep,
) -> CommandAccepted:
    """Заявить ручной catch-up источника через outbox (admin)."""
    command = await request_catchup(
        command_outbox, analytics, source_key=body.source_key, by=admin.login
    )
    return CommandAccepted(command_uid=command.uid, kind=command.kind.value)
