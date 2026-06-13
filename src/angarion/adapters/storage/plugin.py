"""
Storage-бэкенд «sqlite» (FR-7, FR-8 спеки T003): фабрика
``StorageBundle`` для entry point ``angarion.storages``.

``[storage] backend = "sqlite"`` + ``path`` (+ опционально
``auto_migrate``, default true — C-3) резолвятся бутстрапом по
реестру (механизм M1, FR-12 T002). Зависимости — extra
``angarion[sqlite]`` (C-1).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from angarion.adapters.storage.engine import (
    apply_migrations,
    ensure_schema_current,
    make_engine,
)
from angarion.adapters.storage.stores import (
    SqliteAnalytics,
    SqliteCursorStore,
    SqliteDeadLetters,
    SqliteDedupStore,
    SqliteMessageRegistry,
    SqliteOutbox,
    SqliteSessionStore,
    SqliteStateStore,
)
from angarion.domain.errors import ConfigError
from angarion.domain.plugin import StorageBackend, StorageBundle

if TYPE_CHECKING:
    from angarion.config import StorageConfig


class _SqliteSettings(BaseModel):
    """Бэкенд-специфичные ключи секции ``[storage]``: путь и миграции."""

    model_config = ConfigDict(frozen=True, extra='ignore')

    path: Path
    auto_migrate: bool = True


class SqliteStorage(StorageBundle):
    """
    ``StorageBundle`` + движок: ``dispose()`` сверх контракта — нужен
    тестам и graceful shutdown (вызов из жизненного цикла — M3).
    """

    engine: AsyncEngine

    async def dispose(self) -> None:
        """Закрыть пул соединений с ``app.db``; идемпотентно."""
        await self.engine.dispose()


def _make_storage(config: 'StorageConfig') -> SqliteStorage:
    """Фабрика бэкенда для entry point ``angarion.storages`` (FR-8)."""
    try:
        settings = _SqliteSettings.model_validate(config.model_dump())
    except ValidationError as exc:
        msg = f'[storage] backend="sqlite": некорректная секция: {exc}'
        raise ConfigError(msg) from exc
    settings.path.parent.mkdir(parents=True, exist_ok=True)
    if settings.auto_migrate:
        apply_migrations(settings.path)
    else:
        ensure_schema_current(settings.path)
    engine = make_engine(settings.path)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    return SqliteStorage(
        dedup=SqliteDedupStore(sessions),
        outbox=SqliteOutbox(sessions),
        registry=SqliteMessageRegistry(sessions),
        cursors=SqliteCursorStore(sessions),
        state=SqliteStateStore(sessions),
        analytics=SqliteAnalytics(sessions),
        dead_letters=SqliteDeadLetters(sessions),
        session=SqliteSessionStore(sessions),
        engine=engine,
    )


STORAGE_BACKEND: Final = StorageBackend(name='sqlite', make=_make_storage)
"""Значение entry point ``angarion.storages:sqlite``."""
