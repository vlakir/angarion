"""
Общие правила реестра сообщений (§5, §6.1 ТЗ) для всех реализаций
``MessageRegistryPort``: вынесены из InMemory-адаптера при появлении
SQLAlchemy-реализации (FR-5 спеки T003) — поведение обязано совпадать
дословно (SC-4), поэтому код один.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import AwareDatetime

    from angarion.domain.models import RegistryRecord


def effective_ts(rec: RegistryRecord) -> AwareDatetime:
    """Время последней активности записи: удаление, правка или создание."""
    return rec.deleted_at or rec.edit_ts or rec.event_at


def id_at_least(external_id: str, min_id: str) -> bool:
    """Числовое сравнение для десятичных id, иначе лексикографика (§5)."""
    if external_id.isdecimal() and min_id.isdecimal():
        return int(external_id) >= int(min_id)
    return external_id >= min_id
