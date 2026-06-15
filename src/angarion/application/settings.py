"""
Динамические настройки «на лету»: in-process событие ``settings_changed``
и применение уровня лога (§12.8, FR-4; T024, M5/C, фаза 3).

Часть динамики компоненты читают опросом в начале итерации
(``PipelineWorker`` — пауза, ``TelegramSender`` — лимиты троттлинга).
Уровень лога естественной точки опроса не имеет, поэтому применяется по
in-process событию: composition root (producer save, фаза 4) после
``RuntimeConfigPort.save`` зовёт ``SettingsNotifier.notify`` — подписчик
``apply_log_level_on_change`` переконфигурирует structlog.

Динамический ``apply_log_level`` переключает глобальный ``wrapper_class``
structlog — единственное место, где библиотека трогает глобальную
конфигурацию structlog (стартовую цепочку процессоров по-прежнему задаёт
приложение, см. ``angarion.log``; ADR 2026-06-15).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from angarion.domain.models import DynamicSettings

SettingsSubscriber = Callable[['DynamicSettings'], Awaitable[None]]
"""Подписчик события ``settings_changed``: реагирует на новые настройки."""


def _parse_level(level: str) -> int:
    """Имя уровня лога (без учёта регистра) → числовое значение logging."""
    try:
        return logging.getLevelNamesMapping()[level.upper()]
    except KeyError:
        msg = f'неизвестный уровень лога: {level!r}'
        raise ValueError(msg) from None


def apply_log_level(level: str) -> None:
    """
    Применить уровень лога глобально через structlog (§12.8).

    Переключает ``wrapper_class`` на фильтрующий логгер нужного уровня;
    логгеры, полученные после вызова, наследуют новый порог. Неизвестное
    имя уровня → ``ValueError`` (вызывающий решает, гасить или нет).
    """
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(_parse_level(level))
    )


async def apply_log_level_on_change(settings: DynamicSettings) -> None:
    """
    Подписчик ``settings_changed``: применяет ``log_level``, если задан.

    ``None`` (override уровня снят/не задан) — no-op: стартовый порог
    приложения остаётся в силе.
    """
    if settings.log_level is not None:
        apply_log_level(settings.log_level)


class SettingsNotifier:
    """
    In-process pub/sub события ``settings_changed`` (§12.8).

    Producer (composition root после ``RuntimeConfigPort.save``) зовёт
    ``notify``; подписчики применяют изменения, не имеющие естественной
    точки опроса (уровень лога). Доставка неблокирующая: сбой подписчика
    логируется и не срывает остальных — применение настройки не должно
    ломать саму операцию сохранения.
    """

    def __init__(self, *, log: FilteringBoundLogger | None = None) -> None:
        self._subscribers: list[SettingsSubscriber] = []
        self._log: FilteringBoundLogger = (
            log if log is not None else structlog.get_logger('angarion.settings')
        )

    def subscribe(self, subscriber: SettingsSubscriber) -> None:
        """Зарегистрировать подписчика события ``settings_changed``."""
        self._subscribers.append(subscriber)

    async def notify(self, settings: DynamicSettings) -> None:
        """Оповестить подписчиков; сбой одного не срывает остальных (§12.8)."""
        for subscriber in self._subscribers:
            try:
                await subscriber(settings)
            except Exception as exc:  # изоляция подписчиков (§12.8)
                self._log.warning(
                    'settings_subscriber_failed',
                    error=f'{type(exc).__name__}: {exc}',
                )
