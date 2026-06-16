"""
Доменные DTO angarion (§4 ТЗ) и вспомогательные формы конвейера.

Все DTO: ``frozen=True``, ``extra='forbid'``, сериализуемы в JSON без
потерь; время — строго UTC (``AwareDatetime``, §17.4).

Модуль сознательно без ``from __future__ import annotations``:
аннотации pydantic-моделей вычисляются в runtime.

``ProcessorServices`` — НЕ DTO, а конструкция композиции (A-2 спеки
T002): frozen pydantic-модель с ``arbitrary_types_allowed``,
JSON-контракт на неё не распространяется.
"""

from collections.abc import Callable
from enum import StrEnum
from typing import Annotated, Any, Literal, Protocol, runtime_checkable
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SkipValidation,
    StringConstraints,
)
from structlog.typing import FilteringBoundLogger

Messenger = Annotated[str, StringConstraints(pattern=r'^[a-z][a-z0-9_]{1,31}$')]
"""Открытый строковый идентификатор платформы (§4.1).

Не enum: сторонние адаптеры регистрируют свои значения через entry
points (§12.11). Валидация по реестру загруженных плагинов — при
старте (fail-fast с перечнем известных).
"""


class DomainModel(BaseModel):
    """База доменных DTO: иммутабельность и закрытая схема."""

    model_config = ConfigDict(frozen=True, extra='forbid')


class Address(DomainModel):
    """Адрес чата/топика на платформе; thread_id входит в идентичность."""

    messenger: Messenger
    chat_id: str
    thread_id: str | None = None
    title: str | None = None


class AccountRef(DomainModel):
    """Ссылка на учётную запись, через которую идёт приём/отправка."""

    messenger: Messenger
    account_id: str


class EventKind(StrEnum):
    """Закрытый набор видов событий (§15.20); ответ — атрибут, не вид."""

    MESSAGE_NEW = 'message_new'
    MESSAGE_EDITED = 'message_edited'
    MESSAGE_DELETED = 'message_deleted'


class MediaRef(DomainModel):
    r"""
    Структурная ссылка на вложение события (§4.2, M7).

    ``kind`` — **открытая строка** (как ``Messenger``): photo / video /
    document / audio / voice / sticker / … — новые платформы вводят свои
    виды без правки домена. ``ref`` — непрозрачная платформенная ссылка
    для пересылки **без скачивания** (Telegram ``file_id``, Matrix
    ``mxc://``); ``local_path`` ставится при скачивании по требованию
    (фаза A3 M7), ``None`` = доступны только метаданные.

    Поле аддитивно к домену (§17.8.2): сериализуется без потерь, не ломает
    существующие гарантии. Размерности/длительность опциональны — адаптер
    заполняет то, что знает.
    """

    kind: str
    ref: str | None = None
    mime_type: str | None = None
    file_name: str | None = None
    size: int | None = None
    width: int | None = None
    height: int | None = None
    duration: int | None = None
    local_path: str | None = None


class InboundEvent(DomainModel):
    """Нормализованное входящее событие (§4.2)."""

    uid: UUID
    kind: EventKind
    dedup_key: str
    origin: Literal['live', 'catchup']
    source: Address
    received_by: AccountRef
    external_id: str
    sender_id: str | None = None
    sender_name: str | None = None
    text: str | None = None
    previous_text: str | None = None
    content_hash: str | None = None
    media_hash: str | None = None
    reply_to_external_id: str | None = None
    media: list[MediaRef] = Field(default_factory=list)
    event_at: AwareDatetime
    received_at: AwareDatetime
    raw: dict[str, Any] = Field(default_factory=dict)

    @property
    def has_media(self) -> bool:
        """
        Факт наличия вложений (§4.2): производное от ``media`` (M7 A2).

        Обратная совместимость по **доступу** (``event.has_media`` — как до
        M7); задаётся только через ``media``. В JSON-дамп не входит (выводится
        из ``media``, который сериализуется) — ``@computed_field`` потребовал
        бы pydantic-плагина mypy (project-wide config), осознанно не вводим.
        """
        return bool(self.media)


class OutboundMessage(DomainModel):
    """Исходящее сообщение (§4.3); extra ядро не интерпретирует."""

    idempotency_key: str
    target: Address
    send_via: AccountRef
    text: str
    media: list[MediaRef] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class Verdict(StrEnum):
    """Решение процессора: доставлять или подавить."""

    DELIVER = 'deliver'
    DROP = 'drop'


class AnalyticsEvent(DomainModel):
    """Событие аналитики (§4.3); kind — открытая строка."""

    uid: UUID
    kind: str
    event_uid: UUID | None = None
    pipeline: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    at: AwareDatetime


class ProcessingResult(DomainModel):
    """Результат обработки события процессором (§4.3)."""

    verdict: Verdict
    outbound: list[OutboundMessage] = Field(default_factory=list)
    events: list[AnalyticsEvent] = Field(default_factory=list)
    note: str | None = None


class TargetSpec(DomainModel):
    """Цель пайплайна: адрес + аккаунт отправки (§4.4)."""

    target: Address
    send_via: AccountRef


class PipelineContextData(DomainModel):
    """Чистый DTO контекста пайплайна для процессора (§4.4)."""

    pipeline: str
    targets: list[TargetSpec]
    settings: dict[str, Any] = Field(default_factory=dict)


class QueueEnvelope(DomainModel):
    """
    Элемент очереди после fan-out (§6.1).

    ``not_before`` — отложенная обработка (retry/defer-to-tail, C-8):
    worker возвращает «ранний» envelope в хвост очереди.
    """

    pipeline: str
    event: InboundEvent
    attempt: int = 0
    not_before: AwareDatetime | None = None


class QueueItem(DomainModel):
    """Выданный очередью элемент: envelope + непрозрачный receipt адаптера."""

    envelope: QueueEnvelope
    receipt: Any


class QueueDepth(DomainModel):
    """Диагностика очереди: ожидающие и выданные-неподтверждённые."""

    pending: int
    unacked: int


class DeliveryReceipt(DomainModel):
    """Подтверждение отправки: id у платформы (если сообщает) и время."""

    external_id: str | None = None
    delivered_at: AwareDatetime


class OutboxStatus(StrEnum):
    """Статус записи outbox исходящих (C-9)."""

    PENDING = 'pending'
    SENT = 'sent'
    FAILED = 'failed'


class OutboundRecord(DomainModel):
    """
    Запись outbox исходящих (C-9): журнал «что должно быть
    отправлено». Ключ записи — ``msg.idempotency_key`` (insert-if-absent
    в ``OutboxPort.put`` — выходная идемпотентность §7.3).

    ``finished_at`` — момент перехода в терминальный статус
    (sent/failed); по нему работает ``prune()``. ``pipeline`` и
    ``event_uid`` — контекст наблюдаемости для аналитики доставки.
    """

    msg: OutboundMessage
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    next_attempt_at: AwareDatetime
    created_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    receipt: DeliveryReceipt | None = None
    last_error: str | None = None
    pipeline: str | None = None
    event_uid: UUID | None = None


class DeadLetter(DomainModel):
    """
    Запись DLQ (§8, C-2): полный дамп envelope после исчерпания
    ретраев. Requeue (M5) ставит envelope обратно с ``attempt=0``.
    """

    uid: UUID
    envelope: QueueEnvelope
    error: str
    failed_at: AwareDatetime


class RegistryRecord(DomainModel):
    """Текущее состояние сообщения в реестре источника (§9.2)."""

    source_key: str
    external_id: str
    text: str | None = None
    content_hash: str | None = None
    media_hash: str | None = None
    sender_id: str | None = None
    sender_name: str | None = None
    event_at: AwareDatetime
    edit_ts: AwareDatetime | None = None
    deleted_at: AwareDatetime | None = None


class RegistryVersion(DomainModel):
    """Архивная версия текста, вытесненная редактированием (§9.2)."""

    text: str | None = None
    content_hash: str | None = None
    media_hash: str | None = None
    recorded_at: AwareDatetime


class RegistryOutcome(StrEnum):
    """Исход ``MessageRegistryPort.upsert``/``mark_deleted`` (§5, A-3)."""

    IS_NEW = 'is_new'
    TEXT_CHANGED = 'text_changed'
    UNCHANGED = 'unchanged'
    STALE = 'stale'


class RegistryDelta(DomainModel):
    """Результат upsert реестра; previous_text — только при text_changed."""

    outcome: RegistryOutcome
    previous_text: str | None = None


class SourceCursor(DomainModel):
    """Курсор catch-up per source; payload непрозрачен для ядра (§9.2)."""

    source_key: str
    payload: dict[str, Any] = Field(default_factory=dict)
    updated_at: AwareDatetime


class DynamicSettings(DomainModel):
    """
    Динамические настройки (§12.8): sparse-override поверх файла.

    Каждое поле — ``None`` означает «нет override'а, действует значение
    из TOML+env»; не-``None`` — БД-override (приоритет над файлом).
    ``paused_pipelines`` — чисто динамическое (файлового аналога нет),
    ``None`` трактуется как «ничего не на паузе». ``save(patch)``
    применяет частично — ``None``-поля patch'а не трогают сохранённое.
    """

    paused_pipelines: frozenset[str] | None = None
    registration_enabled: bool | None = None
    max_pending_registrations: int | None = None
    log_level: str | None = None
    sender_chat_per_second: float | None = None
    sender_account_per_minute: float | None = None
    catchup_max_messages_per_source: int | None = None
    catchup_max_age_days: int | None = None


class CommandKind(StrEnum):
    """
    Виды команд командного outbox v1 (§12.9): мост api→pipeline.

    ``notify`` — уведомление через ``MessageSinkPort`` (заявка на
    регистрацию §12.7); ``catchup`` — ручной catch-up источника
    (``payload['source_key']``); ``restart_pipeline`` — graceful
    перезапуск pipeline-процесса. Расширение — новый член enum'а;
    механизм outbox при этом не меняется (диспетчеризацию по виду
    делает consumer).
    """

    NOTIFY = 'notify'
    CATCHUP = 'catchup'
    RESTART_PIPELINE = 'restart_pipeline'


class CommandStatus(StrEnum):
    """
    Статус команды в командном outbox (§12.9).

    ``taken`` — команда атомарно захвачена consumer'ом на исполнение
    (pending → taken); терминальные ``done`` / ``failed`` ставятся после
    исполнения. ``failed`` — неуспех исполнения, виден в аудите (разбор
    ручной, аналог DLQ для команд).
    """

    PENDING = 'pending'
    TAKEN = 'taken'
    DONE = 'done'
    FAILED = 'failed'


class OutboxCommand(DomainModel):
    """
    Команда командного outbox (§12.9, M5/C T024): мост из api-процесса
    в pipeline-процесс. Producer (api) кладёт ``put``, consumer
    (pipeline) атомарно захватывает ``take`` (pending → taken),
    исполняет и помечает терминально (``done`` / ``failed``).

    ``payload`` — параметры команды (JSON; например ``source_key`` для
    ``catchup``). ``executed_at`` — момент терминального исхода; по нему
    работает retention. ``result`` / ``error`` — итог исполнения (для
    аудита и ``/ui``).
    """

    uid: UUID
    kind: CommandKind
    payload: dict[str, Any] = Field(default_factory=dict)
    status: CommandStatus = CommandStatus.PENDING
    created_at: AwareDatetime
    executed_at: AwareDatetime | None = None
    result: str | None = None
    error: str | None = None


@runtime_checkable
class ScopedState(Protocol):
    """
    Структурный вид state-хранилища процессора (§10.3).

    Namespace (имя пайплайна) уже зафиксирован worker'ом; реализация —
    ``ScopedStateStore`` application-слоя.
    """

    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str) -> None: ...

    async def delete(self, key: str) -> None: ...

    async def keys(self, prefix: str = '') -> list[str]: ...


class ProcessorServices(BaseModel):
    """
    Сервисы, собираемые worker'ом для процессора (§4.4, не DTO).

    ``log`` не валидируется (``SkipValidation``): конкретный класс
    логгера зависит от конфигурации structlog в приложении, и прокси
    (``BoundLoggerLazyProxy``) isinstance-проверку протокола не
    проходит.

    ``make_idempotency_key`` — частичное применение
    ``domain.keys.make_idempotency_key`` с зафиксированным пайплайном
    (A-9): процессор передаёт событие, цель и порядковый номер
    исходящего.
    """

    model_config = ConfigDict(frozen=True, extra='forbid', arbitrary_types_allowed=True)

    log: SkipValidation[FilteringBoundLogger]
    state: ScopedState
    make_idempotency_key: Callable[[InboundEvent, Address, int], str]
