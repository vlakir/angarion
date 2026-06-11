"""
Матрица возможностей адаптеров (§12.10 ТЗ).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime.
"""

from pydantic import BaseModel, ConfigDict


class AdapterCapabilities(BaseModel):
    """
    Декларация возможностей адаптера платформы.

    Bootstrap сверяет требования конфигурации с возможностями и
    реагирует управляемой деградацией (fail-fast для невыполнимых
    подписок, отключение catch-up при ``history_fetch=False``).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    user_account: bool
    edit_events: bool
    delete_events: bool
    history_fetch: bool
    threads: bool
    push_transport: str
    """Рекомендованные значения: ``client`` | ``webhook`` | ``longpoll``;
    открытая строка — сторонние транспорты допустимы."""
