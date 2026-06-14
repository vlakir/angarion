"""
Асинхронные Protocol-порты ядра (§5 ТЗ; объём M1 — C-1 спеки T002).

``RuntimeConfigPort`` и ``CommandOutboxPort`` сознательно не
определены: появятся аддитивно в M5 вместе со своими DTO (C-1: порт
без потребителя — мёртвый контракт). Протокол ``Listener`` живёт в
``angarion.domain.plugin`` (§12.11).

Методы ``prune()`` — ретеншн-очистка §17.3 (A-7): в M1 реализуются и
тестируются, фоновый запуск появится в M3 вместе с рантаймом.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from uuid import UUID

    from pydantic import AwareDatetime, BaseModel

    from angarion.domain.models import (
        AnalyticsEvent,
        DeadLetter,
        DeliveryReceipt,
        InboundEvent,
        OutboundMessage,
        OutboundRecord,
        PipelineContextData,
        ProcessingResult,
        ProcessorServices,
        QueueDepth,
        QueueEnvelope,
        QueueItem,
        RegistryDelta,
        RegistryRecord,
        RegistryVersion,
        SourceCursor,
    )


@runtime_checkable
class EventQueuePort(Protocol):
    """Очередь событий после fan-out: FIFO, at-least-once (§5, §6.1)."""

    async def put(self, item: QueueEnvelope) -> None:
        """Положить envelope в хвост очереди."""

    async def get(self) -> QueueItem:
        """Ждать и выдать голову очереди; элемент переходит в unacked."""

    async def ack(self, item: QueueItem) -> None:
        """Подтвердить обработку; повторный ack того же item — no-op."""

    async def nack(self, item: QueueItem) -> None:
        """Аварийный возврат item в очередь (§8); после ack — no-op."""

    async def recover(self) -> int:
        """Вернуть все unacked в очередь (старт после падения, §8)."""

    async def depth(self) -> QueueDepth:
        """Диагностика: количество pending/unacked (§17.5)."""


@runtime_checkable
class MessageSinkPort(Protocol):
    """Доставка исходящих сообщений платформе (§5)."""

    async def send(self, msg: OutboundMessage) -> DeliveryReceipt:
        """Отправить сообщение; время в receipt — UTC (§17.4)."""


@runtime_checkable
class DedupStorePort(Protocol):
    """
    Идемпотентность входа (§7.2): проверка ``seen()`` и отметка
    ``mark_inbound()``. Ingest проверяет на входе, а отметку пишет
    строго после fan-out (A-11 T003): падение между ``queue.put`` и
    отметкой даёт повторную обработку envelope (дубль гасит outbox),
    а не потерю события. Выходная идемпотентность после C-9 —
    первичный ключ outbox (``OutboxPort.put``), не здесь.
    """

    async def seen(self, dedup_key: str) -> bool:
        """True — ключ уже отмечен; чистое чтение, без записи (A-11)."""

    async def mark_inbound(self, dedup_key: str) -> bool:
        """True — ключ новый; False — дубль (§7.2)."""

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить отметки старше порога (``dedup_ttl_days``, §17.3)."""


@runtime_checkable
class OutboxPort(Protocol):
    """
    Outbox исходящих (C-9): обработка фиксирует outbound здесь до
    ``ack`` envelope; доставкой занимается отдельный цикл
    (``DeliveryWorker``). Ключ — ``msg.idempotency_key``.
    """

    async def put(
        self,
        msg: OutboundMessage,
        *,
        pipeline: str | None = None,
        event_uid: UUID | None = None,
    ) -> bool:
        """
        Положить исходящее (insert-if-absent): True — записано,
        False — ключ уже известен (повторная обработка envelope).
        """

    async def due(self, limit: int = 50) -> list[OutboundRecord]:
        """Pending-записи с ``next_attempt_at <= now``, FIFO, до limit."""

    async def mark_sent(self, idempotency_key: str, receipt: DeliveryReceipt) -> None:
        """Терминально пометить отправленной; не-pending/неизвестная — no-op."""

    async def reschedule(
        self, idempotency_key: str, *, not_before: AwareDatetime, error: str
    ) -> None:
        """Отложить ретрай: attempts+1; не-pending/неизвестная — no-op."""

    async def mark_failed(self, idempotency_key: str, error: str) -> None:
        """
        Терминально пометить недоставленной (исчерпаны ретраи §8);
        разбор ручной — аналог DLQ для исходящих.
        """

    async def get(self, idempotency_key: str) -> OutboundRecord | None:
        """Запись по ключу или None."""

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить терминальные записи с ``finished_at`` старше порога."""


@runtime_checkable
class MessageRegistryPort(Protocol):
    """Реестр сообщений источников: тексты + история версий (§9.2)."""

    async def upsert(self, rec: RegistryRecord) -> RegistryDelta:
        """
        Зафиксировать состояние сообщения. Исходы (A-3): ``is_new`` /
        ``text_changed`` (вытесненная версия архивируется,
        ``previous_text`` заполнен) / ``unchanged`` / ``stale``
        (staleness-guard §6.1: запись старше сохранённого состояния
        игнорируется).
        """

    async def mark_deleted(
        self, source_key: str, external_id: str
    ) -> RegistryRecord | None:
        """
        Пометить удалённым; вернуть последнее известное состояние
        (для обогащения DELETED-события, §6.1) или None, если
        сообщение реестру неизвестно. Повторный вызов идемпотентен.
        """

    async def known_ids(self, source_key: str, min_id: str) -> set[str]:
        """
        Известные (не удалённые) id источника с ``id >= min_id``.
        Сравнение id: числовое, когда обе строки десятичные, иначе
        лексикографическое (упорядоченность id — свойство платформы,
        §5; платформам без неё фильтр не подходит — передавать '').
        """

    async def get(self, source_key: str, external_id: str) -> RegistryRecord | None:
        """Текущее состояние сообщения или None."""

    async def versions(
        self, source_key: str, external_id: str
    ) -> list[RegistryVersion]:
        """Архив вытесненных версий, старые первыми."""

    async def prune(self, older_than: AwareDatetime) -> int:
        """
        Вычистить записи и версии старше окна (``registry_window_days``,
        §9.2, §17.3); вернуть число удалённых записей.
        """


@runtime_checkable
class CursorStorePort(Protocol):
    """Курсоры catch-up per source; payload непрозрачен ядру (§9.2)."""

    async def load(self, source_key: str) -> SourceCursor | None:
        """Курсор источника или None (первый запуск)."""

    async def save(self, cursor: SourceCursor) -> None:
        """Сохранить (перезаписать) курсор источника."""


@runtime_checkable
class SessionStorePort(Protocol):
    """
    Сессии аккаунтов платформы per ``account_id`` (M3, T005): хранение
    непрозрачной строки сессии (для Telegram — ``StringSession``).

    Порт оперирует **открытой** строкой; шифрование at-rest (Q2 спеки
    T005) — забота потребителя (декоратор ``EncryptedSessionStore``
    telegram-адаптера), а не самого хранилища. ``account_id`` — имя
    секции ``[accounts.*]``.
    """

    async def load(self, account_id: str) -> str | None:
        """Строка сессии аккаунта или None (сессия не выпущена)."""

    async def save(self, account_id: str, session_string: str) -> None:
        """Сохранить (перезаписать) строку сессии аккаунта."""

    async def account_ids(self) -> list[str]:
        """Аккаунты с сохранённой сессией, отсортированы."""


@runtime_checkable
class StateStorePort(Protocol):
    """
    KV-состояние stateful-процессоров (§10.3): значения — JSON-строки,
    ключи неймспейсятся по пайплайну (worker оборачивает в
    ``ScopedStateStore`` — FR-11).
    """

    async def get(self, ns: str, key: str) -> str | None:
        """Значение или None."""

    async def set(self, ns: str, key: str, value: str) -> None:
        """Записать значение (JSON-строка)."""

    async def delete(self, ns: str, key: str) -> None:
        """Удалить ключ; отсутствующий — no-op."""

    async def keys(self, ns: str, prefix: str = '') -> list[str]:
        """Ключи namespace с данным префиксом, отсортированы."""


@runtime_checkable
class AnalyticsPort(Protocol):
    """Аналитика конвейера: запись событий + read-сторона (§5)."""

    async def record(self, event: AnalyticsEvent) -> None:
        """Записать событие аналитики."""

    async def recent(
        self,
        *,
        kind: str | None = None,
        pipeline: str | None = None,
        limit: int = 50,
    ) -> list[AnalyticsEvent]:
        """Последние события, новые первыми; фильтры — по равенству."""

    async def counts_by_kind(
        self, *, since: AwareDatetime, pipeline: str | None = None
    ) -> dict[str, int]:
        """Количество событий по видам начиная с ``since``."""

    async def prune(self, older_than: AwareDatetime) -> int:
        """Удалить события старше порога (``analytics_retention_days``)."""


@runtime_checkable
class DeadLetterPort(Protocol):
    """
    DLQ (§8, C-2): узкий контракт put/list/take. Автоматической
    очистки нет — разбор ручной (§17.3); requeue (M5) делает
    ``take`` + ``queue.put`` с ``attempt=0``.
    """

    async def put(self, letter: DeadLetter) -> None:
        """Положить запись в DLQ."""

    async def list(
        self, *, pipeline: str | None = None, limit: int = 50
    ) -> list[DeadLetter]:
        """Записи в порядке поступления, опционально по пайплайну."""

    async def take(self, uid: UUID) -> DeadLetter | None:
        """Изъять запись по uid; None, если записи нет."""


@runtime_checkable
class ProcessorPort(Protocol):
    """Контракт процессора (§10.1)."""

    name: str

    def config_model(self) -> type[BaseModel] | None:
        """
        Pydantic-схема ``processor_config`` для валидации на старте
        (FR-0 T021; по образцу ``account_config_model`` адаптера §12.11),
        либо ``None`` — процессор без конфигурации (валидация
        пропускается). ``build_app`` валидирует конфиг до приёма событий:
        невалидный → ``ConfigError`` при старте, а не на первом событии.
        """
        ...

    async def process(
        self,
        event: InboundEvent,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Обработать событие; исключение → retry-политика §8."""
