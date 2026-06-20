"""
angarion — библиотека конвейеров обработки событий сообщений.

Гексагональная архитектура (ports & adapters): N источников →
нормализация в события → очередь → обработка → M получателей,
с аналитикой в БД. Полное ТЗ — в CONCEPT.md репозитория.

Публичная поверхность ручного триггера (T038): :class:`ManualEvent` +
:func:`build_manual_record` (payload → ``Record``) и базовые типы payload
(:class:`Endpoint`, :class:`MediaRef`, :class:`RecordKind`, :class:`Record`).
Впрыск — методы ``AngarionApp.submit_event`` / ``AngarionApp.run_pipeline``.
"""

from __future__ import annotations

from angarion.application.manual import (
    MANUAL_ACCOUNT,
    ManualEvent,
    build_manual_record,
)
from angarion.domain.models import Endpoint, MediaRef, Record, RecordKind

__version__ = '0.1.0'

__all__ = [
    'MANUAL_ACCOUNT',
    'Endpoint',
    'ManualEvent',
    'MediaRef',
    'Record',
    'RecordKind',
    '__version__',
    'build_manual_record',
]
