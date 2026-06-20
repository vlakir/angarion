"""
Pydantic-схемы ответов встроенного роутера ``/api/v1`` (§12.5).

Та же DTO-дисциплина, что у домена: ответы — закрытые модели
(``frozen``/``extra='forbid'``), автоматический OpenAPI. Время — UTC
(``AwareDatetime``, §17.4). Схемы API отделены от доменных DTO
сознательно: эволюция домена не меняет внешний контракт молча.
"""

from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict


class _ApiModel(BaseModel):
    """База схем API: иммутабельность и закрытая схема."""

    model_config = ConfigDict(frozen=True, extra='forbid')


class HealthResponse(_ApiModel):
    """liveness без обращения к портам (§12.5)."""

    status: str
    version: str


class QueueDepthSchema(_ApiModel):
    """Глубина очереди: ожидающие и выданные-неподтверждённые (§17.5)."""

    pending: int
    unacked: int


class CursorStateSchema(_ApiModel):
    """Состояние курсора источника; ``updated_at=None`` — курсора ещё нет."""

    source_key: str
    updated_at: AwareDatetime | None = None


class DiagnosticsResponse(_ApiModel):
    """Сводка: очередь, события за 24 ч по видам, курсоры, пайплайны, uptime."""

    queue: QueueDepthSchema
    events_24h: dict[str, int]
    cursors: list[CursorStateSchema]
    pipelines: list[str]
    uptime_seconds: float


class EventSchema(_ApiModel):
    """Событие аналитики в проекции API (зеркало ``AnalyticsEvent``)."""

    uid: UUID
    kind: str
    record_uid: UUID | None = None
    pipeline: str | None = None
    payload: dict[str, Any]
    at: AwareDatetime


class EventsResponse(_ApiModel):
    """Последние события аналитики, новые первыми (§12.5)."""

    events: list[EventSchema]
