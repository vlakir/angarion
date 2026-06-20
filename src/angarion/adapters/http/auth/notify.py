"""
Неблокирующее уведомление о заявке на регистрацию (§12.7/§12.9, T024).

После успешной саморегистрации api-процесс ставит команду ``notify`` в
командный outbox; consumer (pipeline-процесс) отправляет запись через
``SinkPort`` (§12.9). Постановка **неблокирующая**: сбой (нет
аккаунта уведомления, недоступный outbox) логируется и пишет
``notify_failed`` в аналитику, но на регистрацию не влияет — заявка всё
равно создана и видна в ``/ui/users``.

Цель уведомления задаётся секцией ``[api.notify]`` (``account`` +
``address``); пустая — уведомление выключено (no-op).

Без ``from __future__ import annotations``: модель собирается в runtime.
"""

import uuid
from datetime import UTC, datetime

from angarion.adapters.http.deps import AngarionDeps
from angarion.domain.errors import ConfigError
from angarion.domain.models import (
    AccountRef,
    AnalyticsEvent,
    CommandKind,
    Endpoint,
    OutboundRecord,
)
from angarion.log import get_logger

_log = get_logger('angarion.http.notify')

NOTIFY_FAILED = 'notify_failed'
"""Вид события аналитики при сбое постановки/исполнения уведомления (§12.9)."""


def build_registration_record(deps: AngarionDeps, login: str) -> OutboundRecord:
    """
    Собрать ``OutboundRecord`` уведомления о заявке по ``[api.notify]``.

    ``account`` резолвится в ``[accounts.*]`` за transport'ом; неизвестный
    аккаунт → ``ConfigError`` (ловится вызывающим как неблокирующий сбой).
    """
    notify = deps.settings.api.notify
    section = deps.settings.accounts.get(notify.account)
    if section is None:
        known = ', '.join(sorted(deps.settings.accounts)) or '<пусто>'
        msg = (
            f'[api.notify].account {notify.account!r} не найден в '
            f'[accounts.*]; известны: {known}'
        )
        raise ConfigError(msg)
    transport = section.transport
    return OutboundRecord(
        idempotency_key=f'notify:registration:{login}:{uuid.uuid4()}',
        target=Endpoint(
            transport=transport, address=notify.address, thread_id=notify.thread_id
        ),
        send_via=AccountRef(transport=transport, account_id=notify.account),
        text=f'angarion: новая заявка на регистрацию — {login}',
    )


async def notify_registration(deps: AngarionDeps, *, login: str) -> None:
    """
    Поставить команду ``notify`` о заявке (§12.9), не ломая регистрацию.

    Выключено (``[api.notify]`` пуст) → no-op. Любой сбой постановки →
    ``warning`` + ``notify_failed`` в аналитику; исключение наружу не
    выпускается (регистрация уже состоялась).
    """
    if not deps.settings.api.notify.enabled:
        return
    try:
        record = build_registration_record(deps, login)
        await deps.command_outbox.put(
            CommandKind.NOTIFY, payload={'record': record.model_dump(mode='json')}
        )
    except Exception as exc:  # неблокирующее уведомление (§12.9)
        _log.warning('notify_enqueue_failed', login=login, error=str(exc))
        await deps.analytics.record(
            AnalyticsEvent(
                uid=uuid.uuid4(),
                kind=NOTIFY_FAILED,
                payload={'login': login, 'stage': 'enqueue', 'error': str(exc)},
                at=datetime.now(UTC),
            )
        )
