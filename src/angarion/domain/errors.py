"""Доменные исключения (§8 ТЗ)."""

from __future__ import annotations


class AngarionError(Exception):
    """Корень иерархии ошибок angarion."""


class ProcessingError(AngarionError):
    """Ошибка процессора при обработке события."""


class DeliveryError(AngarionError):
    """Ошибка доставки исходящего сообщения."""


class CatchupError(AngarionError):
    """Ошибка catch-up-восстановления событий за время простоя."""


class ConfigError(AngarionError):
    """Невалидная или невыполнимая конфигурация (fail-fast при старте)."""


class NotSupportedError(AngarionError):
    """Операция вне матрицы возможностей адаптера (§12.10)."""
