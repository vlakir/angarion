"""
structlog-хелперы и маскирование секретов (§17.7 ТЗ, C-6).

Библиотека глобальную конфигурацию structlog не навязывает — это
право приложения (plan 2.8). Публичный процессор ``mask_secrets``
приложение включает в свою цепочку процессоров; ``get_logger`` —
точка получения логгера для модулей ядра и процессоров.

Имя модуля — ``log``, не ``logging``: тень stdlib (ruff A005).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

import structlog

if TYPE_CHECKING:
    from structlog.typing import EventDict, FilteringBoundLogger, WrappedLogger

SECRET_KEYS: Final = frozenset(
    {'api_hash', 'token', 'password', 'secret', 'authorization'}
)
"""Ключи §17.7, значения которых маскируются (без учёта регистра)."""

MASK: Final = '***'
"""Замена значения секретного ключа."""


def _masked(value: object) -> object:
    """Рекурсивно замаскировать секреты в значении (dict / list / tuple)."""
    if isinstance(value, Mapping):
        return {
            key: MASK if str(key).lower() in SECRET_KEYS else _masked(item)
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [_masked(item) for item in value]
    return value


def mask_secrets(
    _logger: WrappedLogger, _method_name: str, event_dict: EventDict
) -> EventDict:
    """
    structlog-процессор §17.7: значения ключей ``SECRET_KEYS`` (без
    учёта регистра, рекурсивно по payload) заменяются на ``***`` до
    записи.
    """
    return {
        key: MASK if key.lower() in SECRET_KEYS else _masked(value)
        for key, value in event_dict.items()
    }


def get_logger(name: str = 'angarion') -> FilteringBoundLogger:
    """Логгер structlog под именем ``name`` (конфигурация — приложения)."""
    logger: FilteringBoundLogger = structlog.get_logger(name)
    return logger
