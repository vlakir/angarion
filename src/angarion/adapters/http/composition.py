"""
Web composition root (§12.5/§12.8/§12.9, T024): проекция собранных
driven-портов в контейнер ``AngarionDeps`` для фабрики ``create_app``.

Отделено от ядрового ``bootstrap`` (которое остаётся fastapi-free, §14.9):
здесь web-адаптер берёт ``StorageBundle`` + очередь (для api-роли —
без конвейера, через ``build_storage``/``build_queue``; для
комбинированного — порты уже собранного ``AngarionApp``) и добавляет
in-process ``SettingsNotifier`` с подписчиком уровня лога (§12.8).

``auth_sessionmaker`` user store fastapi-users по умолчанию выводится из
``storage.sessions`` (тот же ``app.db``, ADR §3.1 — два писателя) при
``api.auth="users"``; вызывающий может передать свой.

Без ``from __future__ import annotations``: проекция строит pydantic-
модель ``AngarionDeps`` в runtime.
"""

from typing import Any

from angarion.adapters.http.deps import AngarionDeps
from angarion.application.settings import (
    SettingsNotifier,
    apply_log_level_on_change,
)
from angarion.config import AngarionSettings
from angarion.domain.plugin import StorageBundle
from angarion.domain.ports import EventQueuePort


def build_settings_notifier() -> SettingsNotifier:
    """
    Собрать ``SettingsNotifier`` события ``settings_changed`` (§12.8).

    Подписчик ``apply_log_level_on_change`` применяет динамический
    ``log_level`` (единственная настройка без естественной точки опроса);
    остальную динамику компоненты читают опросом в начале итерации.
    """
    notifier = SettingsNotifier()
    notifier.subscribe(apply_log_level_on_change)
    return notifier


def build_web_deps(
    settings: AngarionSettings,
    storage: StorageBundle,
    queue: EventQueuePort,
    *,
    notifier: SettingsNotifier | None = None,
    auth_sessionmaker: object = None,
    webhook_routers: tuple[Any, ...] = (),
    ingest: object = None,
) -> AngarionDeps:
    """
    Спроецировать порты хранилища и очереди в ``AngarionDeps`` (§12.5).

    ``notifier`` — ``SettingsNotifier`` (по умолчанию ``None``; composition
    root web-процесса передаёт ``build_settings_notifier()``).
    ``auth_sessionmaker`` при ``api.auth="users"`` и ``None`` выводится из
    ``storage.sessions`` — user store делит ``app.db`` с остальными
    порталами (ADR §3.1). ``ingest`` — ``IngestService`` ручного триггера
    (T038): combined передаёт его из ``AngarionApp`` (прямой event-путь),
    api-роль оставляет ``None`` (event-путь через ``CommandOutbox``).
    """
    if settings.api.auth == 'users' and auth_sessionmaker is None:
        auth_sessionmaker = getattr(storage, 'sessions', None)
    return AngarionDeps(
        queue=queue,
        analytics=storage.analytics,
        registry=storage.registry,
        state=storage.state,
        cursors=storage.cursors,
        runtime_config=storage.runtime_config,
        command_outbox=storage.command_outbox,
        dead_letters=storage.dead_letters,
        settings=settings,
        notifier=notifier,
        webhook_routers=webhook_routers,
        auth_sessionmaker=auth_sessionmaker,
        ingest=ingest,
    )
