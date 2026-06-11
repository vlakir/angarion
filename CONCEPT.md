# ТЗ: Библиотека `angarion`


## 1. Цель и назначение

Универсальная Python-библиотека для построения конвейеров обработки **событий сообщений**:

```
N источников → нормализация в события → очередь → обработка → M получателей
                                            ↘ аналитика в БД
```

Событие — первоклассная сущность домена. Отслеживаются: **новое сообщение** (включая ответы), **редактирование сообщения**, **удаление сообщения**. Надёжное отслеживание редактирований — обязательное требование (must have), включая редактирования, произошедшие во время простоя приложения.

Первое целевое приложение: приём событий из закрытых Telegram-групп (пользовательские аккаунты, MTProto), произвольная трансформация (в перспективе — локальная LLM) и отправка результата в другие группы / личные диалоги, с записью аналитики.

Архитектура — **гексагональная (ports & adapters)**: ядро не зависит от Telegram, persist-queue, SQLite, FastAPI и способа запуска. Замена мессенджера, очереди, БД — добавление адаптера без изменения ядра. **Web API (FastAPI)** входит в состав библиотеки как driving-адаптер: встроенная диагностика + механизм пользовательских ручек (§12.5).

### 1.1. Не входит в рамки v1

- Реакции/лайки, просмотры, опросы — игнорируются.
- Гарантия exactly-once на уровне инфраструктуры (обеспечивается at-least-once + идемпотентность, §7).
- Контент медиа — в v1 только факт наличия (`has_media`); скачивание/пересылка медиа — M7.
- Горизонтальное масштабирование, распределённый запуск, графический UI (Web API — есть, §12.5; web-интерфейс — нет).
- Перенос reply-связей в целевую группу (ответ фиксируется в событии, но в целевой группе сообщение отправляется без reply-привязки).

---

## 2. Глоссарий

| Термин | Значение |
|---|---|
| **Событие (Event)** | Атомарный факт в источнике: новое сообщение / редактирование / удаление. |
| **Источник (Source)** | Канал поступления событий: группа, личный диалог, в будущем — HTTP-endpoint. |
| **Получатель (Sink)** | Адрес доставки результата: группа, личный диалог, webhook и т.п. |
| **Аккаунт (Account)** | Учётная запись мессенджера, от имени которой ведётся приём/отправка. |
| **Пайплайн (Pipeline)** | Именованная связка: источники + подписка на виды событий → процессор → получатели. |
| **Процессор (Processor)** | Пользовательская логика обработки события. Плагин. Может иметь состояние (§10.3). |
| **Реестр сообщений (Registry)** | Служебное хранилище сообщений источников (полные тексты + история версий) для детекции редактирований/удалений, обогащения событий и catch-up. |
| **Курсор (Cursor)** | Позиция последнего обработанного состояния источника (для catch-up). |
| **Порт / Адаптер** | Интерфейс ядра / его реализация для конкретной технологии. |

---

## 3. Архитектура

### 3.1. Слои и правило зависимостей

```
┌─────────────────────────────────────────────────────────┐
│  Driving-адаптеры (входящие)                            │
│  TelethonListener (live + catch-up) · CLI · FastAPI     │
└──────────────────────┬──────────────────────────────────┘
                       ▼  вызывают use-case'ы
┌─────────────────────────────────────────────────────────┐
│  Application (use-cases / сервисы)                      │
│  IngestService · PipelineWorker · Router                │
└──────────────────────┬──────────────────────────────────┘
                       ▼  использует
┌─────────────────────────────────────────────────────────┐
│  Domain (ядро)                                          │
│  Pydantic-модели (DTO) · Порты (Protocols) · правила    │
└──────────────────────▲──────────────────────────────────┘
                       │  реализуют порты
┌──────────────────────┴──────────────────────────────────┐
│  Driven-адаптеры (исходящие)                            │
│  TelethonSender · PersistQueueAdapter · SAAnalytics     │
│  SADedupStore · SARegistry · SAStateStore · InMemory*   │
└─────────────────────────────────────────────────────────┘
```

Правило зависимостей: все стрелки импортов направлены к домену. Домен не импортирует `application/` и `adapters/`; application не импортирует `adapters/`. Composition root — только в точке входа (`bootstrap.py`/CLI).

**Передача данных между слоями — исключительно через DTO на Pydantic** (иммутабельные модели, `frozen=True`). Никаких «живых» объектов Telethon, соединений, логгеров внутри DTO. Сервисные объекты (логгер, фабрики) передаются отдельными аргументами, не внутри моделей данных.

**Доменные DTO ≠ ORM-сущности.** Модели SQLAlchemy — внутренняя деталь storage-адаптеров (`adapters/storage/orm.py`): они не покидают адаптер и нигде не импортируются доменом или application-слоем. Преобразование ORM ↔ Pydantic DTO выполняется внутри адаптера. Это сохраняет заменяемость хранилища: порт оперирует только DTO.

### 3.2. Структура пакета

```
angarion/
├── domain/
│   ├── models.py          # DTO: события, адреса, результаты
│   ├── ports.py           # Protocol-интерфейсы
│   └── errors.py
├── application/
│   ├── ingest.py          # IngestService (+ fan-out по пайплайнам)
│   ├── worker.py          # PipelineWorker
│   ├── router.py          # source × event_kind → [pipelines]
│   └── registry.py        # реестр процессоров
├── adapters/
│   ├── telegram/
│   │   ├── listener.py    # live-события
│   │   ├── catchup.py     # дозабор после простоя
│   │   ├── sender.py
│   │   └── mapping.py     # raw event → InboundEvent
│   ├── queue/             # persistqueue_, memory
│   ├── storage/           # orm.py (SQLAlchemy-сущности),
│   │                      # sqlalchemy_analytics, sqlalchemy_dedup,
│   │                      # sqlalchemy_registry, sqlalchemy_state, memory
│   └── http/              # FastAPI driving-адаптер:
│       ├── app.py         #   create_app(deps, routers=..., pages=...)
│       ├── deps.py        #   DI-провайдеры портов (Depends)
│       ├── schemas.py     #   Pydantic-схемы ответов
│       ├── routers/
│       │   └── diagnostics.py   # встроенные /health, /diagnostics, /events
│       └── ui/            # Web UI (§12.6)
│           ├── pages.py   #   роуты /ui, register_page()
│           ├── templates/ #   base.html, dashboard.html, events.html, фрагменты
│           └── static/    #   htmx.min.js, pico.min.css (в составе пакета)
├── migrations/            # Alembic (env.py, versions/)
├── config.py
├── bootstrap.py
└── cli.py
```

---

## 4. Доменные модели (Pydantic v2, DTO)

Все модели: `frozen=True`, `extra="forbid"`, сериализуемы в JSON без потерь.

### 4.1. Адресация

```python
Messenger = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{1,31}$")]
# Открытый строковый идентификатор платформы. НЕ enum: сторонние адаптеры
# регистрируют свои значения через entry points (§12.11). Валидация — по
# реестру зарегистрированных плагинов при старте (fail-fast с перечнем известных).
# Закреплённые значения: "telegram" (v1);
# зарезервированы: "matrix", "max", "vk", "discord" (§12.10).

class Address(BaseModel):
    messenger: Messenger
    chat_id: str
    thread_id: str | None = None   # топик/тред внутри чата: форум-топики Telegram,
                                   # треды Discord, споры Matrix; None = основной канал чата
    title: str | None = None

class AccountRef(BaseModel):
    messenger: Messenger
    account_id: str
```

`thread_id` входит в идентичность адреса: маршрутизация, `source_key` (§7.2) и идемпотентность различают топики одного чата. Адаптеры, не поддерживающие треды, всегда оставляют `None`.

### 4.2. События

```python
class EventKind(StrEnum):
    MESSAGE_NEW = "message_new"        # включая ответы (см. reply_to)
    MESSAGE_EDITED = "message_edited"
    MESSAGE_DELETED = "message_deleted"

class InboundEvent(BaseModel):
    uid: UUID                       # внутренний id события
    kind: EventKind
    dedup_key: str                  # §7.2
    origin: Literal["live", "catchup"]
    source: Address
    received_by: AccountRef
    external_id: str                # id сообщения в мессенджере
    sender_id: str | None = None
    sender_name: str | None = None
    text: str | None = None         # для DELETED — текст из реестра (если в окне)
    previous_text: str | None = None  # для EDITED: предыдущая версия (из реестра)
    content_hash: str | None = None   # sha256 нормализованного текста
    reply_to_external_id: str | None = None   # признак ответа
    has_media: bool = False
    event_at: AwareDatetime         # date / edit_date в мессенджере
    received_at: AwareDatetime
    raw: dict[str, Any] = {}
```

Ответ на сообщение моделируется как `MESSAGE_NEW` с заполненным `reply_to_external_id` — это атрибут сообщения, а не отдельный вид события. Пайплайн может фильтровать «только ответы» предикатом в конфиге (§11).

Для `MESSAGE_DELETED` мессенджер не передаёт содержимое; `text`, `sender_*` и прочие поля восстанавливаются **из реестра** (доступны, если сообщение в пределах окна реестра, иначе — `None`). Для `MESSAGE_EDITED` из реестра подставляется `previous_text` — процессор получает обе версии и может, например, строить diff.

### 4.3. Исходящие, результат, аналитика

```python
class OutboundMessage(BaseModel):
    idempotency_key: str            # §7.3
    target: Address
    send_via: AccountRef
    text: str
    extra: dict[str, Any] = {}      # адаптер-специфичные параметры отправки:
                                    # telegram: parse_mode, silent, disable_preview;
                                    # email: subject, cc; и т.п. Ядро поле не интерпретирует.

class Verdict(StrEnum):
    DELIVER = "deliver"
    DROP = "drop"

class ProcessingResult(BaseModel):
    verdict: Verdict
    outbound: list[OutboundMessage] = []
    events: list[AnalyticsEvent] = []
    note: str | None = None

class AnalyticsEvent(BaseModel):
    uid: UUID
    kind: str                       # ingested/processed/delivered/dropped/
                                    # failed/unrouted/duplicate/catchup_*/произвольные
    event_uid: UUID | None = None
    pipeline: str | None = None
    payload: dict[str, Any] = {}
    at: AwareDatetime
```

### 4.4. Контекст процессора (DTO + сервисы раздельно)

```python
class PipelineContextData(BaseModel):     # чистый DTO
    pipeline: str
    targets: list[TargetSpec]             # Address + AccountRef
    settings: dict[str, Any] = {}

class ProcessorServices:                  # НЕ DTO; собирается worker'ом
    log: BoundLogger
    state: ScopedStateStore               # §10.3, namespace = pipeline
    make_idempotency_key: Callable[[InboundEvent, Address, int], str]
```

---

## 5. Порты (`domain/ports.py`)

Все порты — асинхронные `typing.Protocol`.

```python
class EventQueuePort(Protocol):
    async def put(self, item: QueueEnvelope) -> None: ...
    async def get(self) -> QueueItem: ...
    async def ack(self, item: QueueItem) -> None: ...
    async def nack(self, item: QueueItem) -> None: ...
    async def recover(self) -> int: ...
    async def depth(self) -> QueueDepth: ...        # pending/unacked — для диагностики

class MessageSinkPort(Protocol):
    async def send(self, msg: OutboundMessage) -> DeliveryReceipt: ...

class DedupStorePort(Protocol):
    async def mark_inbound(self, dedup_key: str) -> bool: ...      # False = дубль
    async def mark_delivered(self, idempotency_key: str) -> bool: ...

class MessageRegistryPort(Protocol):
    """Сообщения источников: полные тексты + история версий."""
    async def upsert(self, rec: RegistryRecord) -> RegistryDelta: ...
        # RegistryDelta: is_new | text_changed(previous_text) | unchanged;
        # при text_changed предыдущая версия архивируется в историю
    async def mark_deleted(self, source_key: str, external_id: str) -> RegistryRecord | None: ...
        # возвращает последнее известное состояние удалённого сообщения
    async def known_ids(self, source_key: str, min_id: str) -> set[str]: ...
    async def get(self, source_key: str, external_id: str) -> RegistryRecord | None: ...
    async def versions(self, source_key: str, external_id: str) -> list[RegistryVersion]: ...
    async def prune(self, older_than: AwareDatetime) -> int: ...

class CursorStorePort(Protocol):
    async def load(self, source_key: str) -> SourceCursor | None: ...
    async def save(self, cursor: SourceCursor) -> None: ...

class StateStorePort(Protocol):
    """KV-хранилище состояния stateful-процессоров; ключи неймспейсятся по пайплайну."""
    async def get(self, ns: str, key: str) -> str | None: ...      # значения — JSON-строки
    async def set(self, ns: str, key: str, value: str) -> None: ...
    async def delete(self, ns: str, key: str) -> None: ...
    async def keys(self, ns: str, prefix: str = "") -> list[str]: ...

class AnalyticsPort(Protocol):
    async def record(self, event: AnalyticsEvent) -> None: ...
    # read-сторона (диагностика, Web API, пользовательские ручки):
    async def recent(self, *, kind: str | None = None, pipeline: str | None = None,
                     limit: int = 50) -> list[AnalyticsEvent]: ...
    async def counts_by_kind(self, *, since: AwareDatetime,
                             pipeline: str | None = None) -> dict[str, int]: ...

class RuntimeConfigPort(Protocol):
    """Динамические настройки (§12.8): БД-override'ы поверх файла."""
    async def load(self) -> DynamicSettings: ...
    async def save(self, patch: DynamicSettingsPatch, updated_by: str) -> DynamicSettings: ...
    async def reset(self, key: str) -> None: ...   # сброс override'а к значению из файла

class CommandOutboxPort(Protocol):
    """Командный outbox (§12.9): api-процесс ставит команды,
    исполняет процесс-владелец Telethon-клиентов (pipeline)."""
    async def put(self, cmd: OutboxCommand) -> None: ...
    async def poll(self, limit: int = 10) -> list[OutboxCommand]: ...
    async def mark_done(self, cmd_id: UUID, result: str | None = None) -> None: ...
    async def mark_failed(self, cmd_id: UUID, error: str) -> None: ...

class ProcessorPort(Protocol):
    name: str
    async def process(
        self, event: InboundEvent,
        ctx: PipelineContextData, svc: ProcessorServices,
    ) -> ProcessingResult: ...
```

`QueueEnvelope = (pipeline: str, event: InboundEvent, attempt: int = 0)` — элемент очереди после fan-out (§6.1); `attempt` — счётчик попыток обработки (§8). `QueueItem` — envelope + непрозрачный receipt адаптера.

Примечание к `external_id`: в доменной модели это строка, и **домен не предполагает упорядоченности id** — это свойство конкретного мессенджера. Семантикой упорядочивания и позиционирования владеет адаптер (§9.2: курсор непрозрачен). Для Telegram адаптер сравнивает id численно (не лексикографически: «9» > «10» при строковом сравнении — классическая ошибка); для платформ без монотонных id (Matrix) используются их собственные механизмы позиции.

---

## 6. Application-слой

### 6.1. `IngestService` (с fan-out)

Вход — `InboundEvent` от driving-адаптера (live или catch-up):

1. `dedup.mark_inbound(dedup_key)` — дубль (реконнект, повторный catch-up, запоздавший буферизованный live после catch-up) → событие `duplicate`, выход **до любых записей в реестр**. Ключи всех видов событий вычисляются из самого события, реестр для этого не нужен.
2. Поддержать реестр: `registry.upsert()` для NEW/EDITED (полный текст; предыдущая версия архивируется), `registry.mark_deleted()` для DELETED. **Staleness-guard**: реестр игнорирует запись, чьи `event_at`/`edit_ts` старше уже сохранённого состояния (возвращая `stale`), — защита от перезаписи актуального текста устаревшим событием. Из возвращаемых данных событие **обогащается**: EDITED получает `previous_text`, DELETED — текст и метаданные удалённого сообщения.
3. `router.resolve(source, kind, event)` → список пайплайнов (multicast; здесь же применяются фильтры-предикаты пайплайна, например `only_replies`). Пусто → событие `unrouted`, выход.
4. Для каждого пайплайна: `queue.put(QueueEnvelope(pipeline, event))` — **отдельный элемент очереди на каждый пайплайн**: ретраи, DLQ и порядок изолированы по пайплайнам, частичный сбой одного пайплайна не затрагивает другие.
5. `analytics.record("ingested")`.

### 6.2. `Router`

Таблица: `(Address источника, EventKind) → set[pipeline]`. Источник может входить в произвольное число пайплайнов (multicast — базовое требование). Пайплайн в конфиге декларирует подписку на виды событий (§11).

### 6.3. `PipelineWorker`

Конкурентность v1 = 1 (строгий FIFO не требуется, но единичный worker даёт его бесплатно; параллелизм — будущее расширение без изменения портов).

```
item = queue.get()                       # envelope: (pipeline, event)
result = processor.process(event, ctx, svc)     # исключение → §8
if result.verdict == DELIVER:
    for out in result.outbound:
        if dedup.mark_delivered(out.idempotency_key):
            sink.send(out)
            analytics.record("delivered")
analytics.record("processed"|"dropped")
queue.ack(item)
```

Инвариант: отправка и фиксация доставки — строго до `ack`. Повтор после падения гасится `mark_delivered`.

---

## 7. Гарантии доставки и идемпотентность

### 7.1. Модель

Сквозная гарантия — **at-least-once** (включая период простоя — за счёт catch-up, §9). Дубли подавляются идемпотентностью на входе и выходе.

### 7.2. Ключ входящей дедупликации

`source_key = f"{messenger}:{account_id}:{chat_id}"` (+ `:{thread_id}`, если задан)

| Вид события | dedup_key |
|---|---|
| MESSAGE_NEW | `{source_key}:{external_id}:new` |
| MESSAGE_EDITED | `{source_key}:{external_id}:edit:{content_hash}` |
| MESSAGE_DELETED | `{source_key}:{external_id}:del` |

Для EDITED в ключ входит **хэш содержимого**, а не `edit_date`: гранулярность времени в Telegram — секунда, два быстрых редактирования могли бы слиться; хэш различает версии надёжно и одинаково работает в live и catch-up. Следствие: редактирование, вернувшее текст к уже виденной версии, считается дублем — принимается осознанно.

Формирование ключей и хэшей — **публичные хелперы ядра** (`angarion.domain.keys`: `make_source_key()`, `make_dedup_key()`, `normalize_and_hash()`); адаптеры, включая сторонние, обязаны использовать их, а не реализовывать формат самостоятельно — иначе ломается сквозная идемпотентность. Правила нормализации текста перед хэшированием — часть публичного контракта и меняются только мажорной версией.

### 7.3. Ключ идемпотентности исходящего

`idempotency_key = f"{dedup_key}->{pipeline}:{target.chat_id}:{n}"` — имя пайплайна включено обязательно: при multicast два пайплайна могут слать в одну группу и не должны подавлять друг друга.

### 7.4. Хранилище

Реализация — storage-адаптер на SQLAlchemy: `insert(...).on_conflict_do_nothing()` (диалект SQLite) + проверка `rowcount` — атомарное «отметить, если не было». TTL-очистка (по умолчанию 30 дней) — фоновая задача.

---

## 8. Обработка ошибок и повторы

| Сбой | Поведение |
|---|---|
| Исключение в процессоре | **retry = re-enqueue**: `queue.put(envelope.copy(attempt+1))` с экспоненциальной задержкой + `ack` исходного item'а; после `attempt ≥ max_retries` (default 5) → `ack` + `failed` + запись в DLQ (`dead_letters`, полный дамп envelope). Requeue из DLQ (§12.8) ставит envelope с `attempt = 0`. `nack` зарезервирован для аварийных случаев (recovery после падения процесса). |
| Ошибка отправки | FloodWait — честное ожидание; сеть — ретраи внутри sender'а (tenacity-политика), затем re-enqueue envelope (как выше). |
| Падение процесса | `queue.recover()` при старте (роль pipeline) + дедупликация. Пропущенные за время простоя события источников восстанавливает catch-up (§9). |
| Нет маршрута | `ack` + событие `unrouted`. |

Примечание: под ретраями счётчики аналитики (`processed` и т.п.) — **приблизительные** (событие обработки может записаться повторно); точна только дедуплицированная доставка (`delivered`). Это осознанное следствие at-least-once.

Доменные исключения: `ProcessingError`, `DeliveryError`, `CatchupError`, `ConfigError`, `NotSupportedError` (операция вне матрицы возможностей адаптера, §12.10–12.11).

---

## 9. Catch-up после простоя (must have)

### 9.1. Задача

Live-события поступают только при активном подключении. После простоя приложение обязано восстановить по каждому источнику: новые сообщения, **редактирования** и удаления, произошедшие за время простоя.

### 9.2. Данные

- **Курсор** per source — **непрозрачный для ядра** `SourceCursor(source_key, payload: dict, updated_at)`: содержимым и семантикой payload владеет адаптер источника. Для Telegram payload = `{last_seen_external_id, last_scan_at}` с численным сравнением id; для других платформ — их собственные механизмы позиции (например, sync-токены Matrix). Ядро курсор только хранит (`CursorStorePort`) и передаёт адаптеру.
- **Реестр сообщений** per source: текущее состояние (`external_id`, `event_at`, `edit_ts`, `text`, `content_hash`, `deleted_at`) + **история версий** (каждое редактирование архивирует вытесненную версию). Глубина хранения — `registry_window` (конфиг, по умолчанию 7 дней; `0` = бессрочно); записи старше окна вычищаются `prune()`.

### 9.3. Алгоритм (на старте listener'а, per source, до включения live-подписки; описание — для Telegram-адаптера, другие адаптеры реализуют эквивалент в своих терминах позиции)

1. Зафетчить историю: все сообщения с `id > cursor.last_seen_external_id` **плюс** весь диапазон `registry_window` (непрерывный диапазон, покрывающий реестр), с пагинацией и троттлингом; ограничение глубины — `catchup_max_messages` / `catchup_max_age` (конфиг; при превышении — событие `catchup_truncated` в аналитику).
2. `id > cursor` → эмиссия `MESSAGE_NEW` (origin=`catchup`).
3. Для сообщений в пределах окна, уже известных реестру: `content_hash` отличается → эмиссия `MESSAGE_EDITED`.
4. Id, известные реестру (не помеченные deleted), отсутствующие в зафетченном непрерывном диапазоне → эмиссия `MESSAGE_DELETED`. **Только в пределах фактически покрытого фетчем диапазона**: если фетч был усечён лимитами (`catchup_truncated`), id вне покрытого диапазона удалёнными не помечаются — лучше пропустить удаление, чем эмитировать ложное.
5. Обновить курсор. Буферизованные за время catch-up live-события пропустить через тот же ingest — дедупликация устранит пересечения.

Catch-up и live сходятся в одном `IngestService`; различие — только поле `origin` (процессор вправе обрабатывать догнанные события иначе, например не ретранслировать устаревшие).

### 9.4. Ограничения платформы (фиксируются в README)

1. Удаления и правки за пределами `registry_window` не детектируются (реестр уже очищен); при `registry_window = 0` ограничение снимается ценой роста БД.
2. Live-удаления в **супергруппах** (`-100...`) приходят с указанием канала — надёжно. В legacy-группах малого размера Telegram не передаёт chat id в update удаления; адаптер делает резолв по реестру (поиск `external_id` среди отслеживаемых источников), коллизии id логируются. Целевой кейс библиотеки — супергруппы.
3. Catch-up видит только **итоговое** состояние сообщения: цепочка правок A→B→C за время простоя даст одно событие EDITED (previous=A, current=C); промежуточная версия B не восстановима — это ограничение платформы, не реализации.
4. Редактирование, вернувшее ранее виденный текст (A→B→A→B), на втором появлении B будет подавлено дедупликацией (следствие хэш-модели §7.2). В live-режиме с историей версий кейс A→B→A обрабатывается корректно.

---

## 10. Процессоры (plugin API)

### 10.1. Регистрация и контракт

- `@processor("name")` → реестр; внешние пакеты — через entry points (`angarion.processors`).
- Сигнатура: `process(event, ctx: PipelineContextData, svc: ProcessorServices) -> ProcessingResult`.
- Побочные эффекты: допустимы сетевые вызовы (LLM) и работа с `svc.state`; всё остальное — через возвращаемый результат.
- Процессор обязан корректно обрабатывать все виды событий, на которые подписан его пайплайн (включая `text=None` у DELETED).

### 10.2. Встроенные процессоры v1

`passthrough`, `template` (Jinja по полям события; для edited/deleted — свои шаблоны). LLM-процессор — M4: OpenAI-совместимый endpoint через `httpx.AsyncClient`, настройки из `processor_config`.

### 10.3. Stateful-процессоры

Состояние — через `svc.state` (`ScopedStateStore` — тонкая обёртка worker'а над `StateStorePort` с зафиксированным namespace = имя пайплайна; реализация хранилища v1 — таблица в `app.db`). Значения — JSON-строки (Pydantic-модели состояния сериализует сам процессор). Семантика at-least-once распространяется и на состояние: процессор обязан быть идемпотентным относительно повторной обработки события (рекомендуемый приём — хранить в состоянии `dedup_key` последних учтённых событий). Типовой сценарий: накопление сообщений для дайджеста с периодическим сбросом.

---

## 11. Конфигурация

`pydantic-settings`: TOML + env для секретов. Валидация и ссылочная целостность — fail-fast при старте; среди инвариантов: `dedup_ttl_days ≥ registry_window_days` (иначе повторный catch-up мог бы ре-эмитировать события с уже вычищенными dedup-ключами).

```toml
[accounts.main]
messenger = "telegram"
session = "sessions/main.session"
# api_id/api_hash — из env

[storage]
backend = "sqlite"
path = "data/app.db"
dedup_ttl_days = 30
registry_window_days = 7
analytics_retention_days = 90     # 0 = бессрочно; prune — фоновая задача (§17.3)

[queue]
backend = "persistqueue"
path = "data/queue.db"
depth_warn = 500                  # порог глубины: событие queue_depth_warning + подсветка в UI (§17.5)

[catchup]
enabled = true
max_messages_per_source = 2000
max_age_days = 7

[api]
enabled = true
host = "127.0.0.1"          # наружу — только осознанно (за reverse proxy + TLS)
port = 8080
ui_enabled = true           # Web UI на /ui (§12.6)

[api.auth]
mode = "users"                    # "users" (default) | "none" — только для разработки
registration_enabled = true       # саморегистрация с одобрением админа (§12.7)
max_pending_registrations = 20
jwt_lifetime_seconds = 3600
cookie_secure = false             # включить за TLS-прокси
# JWT-секрет — из env: ANGARION_API__SECRET (обязателен при mode = "users")
# bootstrap первого админа: env ANGARION_ADMIN_LOGIN / ANGARION_ADMIN_PASSWORD

[api.auth.notify]                 # опц.: уведомление о заявках в Telegram (§12.7)
account = "main"
chat_id = "123456789"             # личный диалог админа
base_url = "https://example.org"  # для ссылки на /ui/users в тексте уведомления

[pipelines.digest]
processor = "template"
events = ["message_new", "message_edited"]        # подписка на виды событий
only_replies = false                               # опц. фильтр-предикат
sources = [{ account = "main", chat_id = "-1001111111111" }]
targets = [{ account = "main", chat_id = "-1002222222222" }]

[pipelines.digest.processor_config]
template = "{{ sender_name }}: {{ text }}"
template_edited = "✏ {{ sender_name }}: {{ text }}"

[pipelines.audit]                                  # multicast: тот же источник
processor = "my_audit"
events = ["message_deleted", "message_edited"]
sources = [{ account = "main", chat_id = "-1001111111111" }]
targets = [{ account = "main", chat_id = "-1003333333333" }]
```

---

## 12. Адаптеры v1

### 12.1. Telegram (Telethon)

- **Listener**: мультиаккаунт (клиент + session-файл на аккаунт, общий event loop); подписки `events.NewMessage`, `events.MessageEdited`, `events.MessageDeleted` по источникам из конфига; маппинг в `mapping.py` (отдельно, тестируемо на фикстурах); буферизация live-событий на время catch-up. **Ограничение сессий**: session-файл Telethon (SQLite + состояние MTProto) принадлежит ровно одному процессу — конкурентное использование двумя процессами ведёт к потере апдейтов и риску отзыва сессии; поэтому все клиенты живут в одном процессе роли `pipeline`.
- **Catch-up** (`catchup.py`): §9.3, `iter_messages` с пагинацией и паузами.
- **Sender**: общий `ClientRegistry` с listener'ом; token-bucket per (account, chat): ≤ 1 msg/s на чат, ≤ 20/min на аккаунт (конфигурируемо); `FloodWaitError` → ожидание.

### 12.2. Очередь

`persistqueue.SQLiteAckQueue`, файл `queue.db`; сериализация envelope — `model_dump_json()` (строки, не pickle); синхронные вызовы — `asyncio.to_thread()`; `recover()` → `resume_unack_tasks()`.

### 12.3. Хранилище: SQLAlchemy 2.0 (async) + Alembic

- Движок: `create_async_engine("sqlite+aiosqlite:///data/app.db")`; `PRAGMA journal_mode=WAL` и `foreign_keys=ON` — через event-хук `connect`.
- Стиль — декларативный SQLAlchemy 2.0 (`DeclarativeBase`, `Mapped`/`mapped_column`), полная типизация.
- **Миграции — Alembic** (async-шаблон): любое изменение ORM-сущностей сопровождается миграцией; автогенерация с обязательной ручной ревизией; применение — при старте приложения (`alembic upgrade head`, конфигурируемо) и командой CLI `angarion migrate`.
- ORM-сущности (`adapters/storage/orm.py`) — приватные для адаптеров (§3.1). Состав:

| Сущность | Назначение | Ключи/индексы |
|---|---|---|
| `AnalyticsEventRow` | журнал аналитики | PK `id`; unique `uid`; index (`kind`, `at`) |
| `InboundDedupRow` | дедуп входящих | PK `dedup_key` |
| `DeliveredDedupRow` | дедуп доставки | PK `idempotency_key` |
| `MessageRow` | реестр: текущее состояние сообщения (полный текст) | составной PK (`source_key`, `external_id`); index `event_at`; связь 1→N с версиями |
| `MessageVersionRow` | история версий (вытесненные правками тексты) | unique (`source_key`, `external_id`, `version_n`); FK на `MessageRow` |
| `SourceCursorRow` | курсоры catch-up (payload непрозрачен, §9.2) | PK `source_key`; `payload` (JSON), `updated_at` |
| `ProcessorStateRow` | KV состояния процессоров | составной PK (`ns`, `key`) |
| `DeadLetterRow` | DLQ | PK `id` |
| `UserRow` | пользователи Web API/UI (§12.7) | PK `id`; unique `login`; поля `password_hash`, `role`, `is_active`, `registered_at`, `approved_at`, `approved_by` |
| `AppSettingRow` | override'ы динамических настроек (§12.8) | PK `key`; `value` (JSON), `updated_at`, `updated_by` |
| `OutboxCommandRow` | командный outbox api → pipeline (§12.9) | PK `id`; `kind`, `payload` (JSON), `status`, `created_at`, `executed_at`, `result`/`error` |

Репрезентативный фрагмент (стиль, обязательный для всех сущностей):

```python
class Base(DeclarativeBase):
    pass

class MessageRow(Base):
    __tablename__ = "message_registry"

    source_key: Mapped[str] = mapped_column(primary_key=True)
    external_id: Mapped[str] = mapped_column(primary_key=True)
    sender_id: Mapped[str | None]
    sender_name: Mapped[str | None]
    text: Mapped[str | None]
    content_hash: Mapped[str | None]
    reply_to_external_id: Mapped[str | None]
    has_media: Mapped[bool] = mapped_column(default=False)
    event_at: Mapped[datetime] = mapped_column(index=True)
    edit_ts: Mapped[datetime | None]
    deleted_at: Mapped[datetime | None]

    versions: Mapped[list["MessageVersionRow"]] = relationship(
        back_populates="message", cascade="all, delete-orphan",
    )
```

- Поле `payload` аналитики и значения KV — `JSON`-колонки SQLAlchemy (на SQLite — TEXT с сериализацией).
- Каждый storage-адаптер получает `async_sessionmaker` через конструктор (инъекция из bootstrap); транзакции — `async with session.begin()`; никаких глобальных сессий.
- Очередь (persist-queue) остаётся вне SQLAlchemy — это отдельный файл и отдельная подсистема со своим механизмом (§12.2).

### 12.4. InMemory*

Полные in-memory реализации всех driven-портов — часть библиотеки: на них строятся unit-тесты пайплайна, контрактные тесты и прототипы процессоров.

### 12.5. Web API (FastAPI, driving-адаптер)

#### Принцип

HTTP — ещё один вход в гексагон, симметричный Telethon-listener'у: FastAPI-слой не содержит бизнес-логики и общается с системой **только через порты**. ORM-сущности, сессии SQLAlchemy и объекты Telethon в HTTP-слой не проникают.

#### Фабрика приложения

```python
def create_app(
    deps: AngarionDeps,                      # контейнер портов из bootstrap
    *,
    routers: Sequence[APIRouter] = (),       # пользовательские JSON-роутеры
    pages: Sequence[Page] = (),              # пользовательские UI-страницы (§12.6)
    title: str = "angarion",
) -> FastAPI: ...
```

`AngarionDeps` — контейнер composition root (порты: queue, analytics, registry, state, cursors + конфиг). Фабрика кладёт его в `app.state` и подключает встроенный роутер диагностики, затем пользовательские.

#### DI поверх портов

Библиотека публикует типизированные FastAPI-зависимости — это **публичный API** для пользовательских ручек:

```python
# angarion.adapters.http.deps
AnalyticsDep = Annotated[AnalyticsPort, Depends(get_analytics)]
RegistryDep  = Annotated[MessageRegistryPort, Depends(get_registry)]
StateDep     = Annotated[StateStorePort, Depends(get_state)]
QueueDep     = Annotated[EventQueuePort, Depends(get_queue)]
CursorsDep   = Annotated[CursorStorePort, Depends(get_cursors)]
```

#### Встроенные ручки (роутер `/api/v1`, образец для пользовательских)

| Ручка | Назначение |
|---|---|
| `GET /api/v1/health` | liveness: `{"status": "ok", "version": ...}` — без обращения к портам |
| `GET /api/v1/diagnostics` | глубина очереди (`queue.depth()`), счётчики событий за 24 ч по видам (`analytics.counts_by_kind`), состояние курсоров по источникам, список пайплайнов из конфига, uptime |
| `GET /api/v1/events?kind=&pipeline=&limit=` | последние события аналитики (`analytics.recent`) |

Ответы — Pydantic-схемы (`schemas.py`): та же DTO-дисциплина, автоматический OpenAPI. Встроенные ручки — **read-only**, единственное write-исключение — управление пользователями для роли admin (§12.7); прочие управляющие операции (пауза пайплайна и т.п.) — вне рамок v1.

#### Пользовательские ручки (контракт расширения)

```python
from angarion.adapters.http import create_app
from angarion.adapters.http.deps import AnalyticsDep, StateDep

my = APIRouter(prefix="/api/my")

@my.get("/digest-status")
async def digest_status(analytics: AnalyticsDep, state: StateDep):
    pending = await state.get("digest", "buffer_size")
    delivered = await analytics.counts_by_kind(since=day_ago(), pipeline="digest")
    return {"pending": pending, "delivered": delivered}

app = create_app(deps, routers=[my])
```

Пользователь получает полный инструментарий FastAPI (валидация, OpenAPI-документация своих ручек, middleware), оставаясь в рамках портов.

#### Запуск и безопасность

- Режимы: встроенный (`angarion run --with-api` — uvicorn как asyncio-задача в общем процессе) и раздельный (`--role pipeline` + `--role api`): api-процесс читает те же `app.db` (WAL допускает конкурентных читателей) и `queue.db`, Telethon-клиентов **не имеет**; действия, требующие клиентов, ставятся в командный outbox (§12.9). Роли ingest и worker **не разделяются** на процессы: они делят Telethon-клиентов, а session-файл не может использоваться двумя процессами (§12.1).
- Аутентификация/авторизация — §12.7; по умолчанию bind `127.0.0.1`. Выставление наружу — через reverse proxy + TLS (ответственность пользователя), но auth-механизм рассчитан на работу приложения в общей сети.

#### Тестирование (TDD)

Ручки тестируются через `httpx.AsyncClient(transport=ASGITransport(app))` поверх `create_app` с **InMemory-портами** — без сети, БД и Telegram; контракт DI-зависимостей покрывается тем же способом.

### 12.6. Web UI (человекоориентированный дашборд)

#### Стек и обоснование

**SSR: Jinja2 + htmx + Pico.css.** Без Node.js, npm и этапа сборки — UI остаётся чисто Python-артефактом:

- **Jinja2** — страницы рендерятся на сервере из тех же данных портов, что отдаёт JSON API (одни данные — два представления);
- **htmx** (~14 КБ, один JS-файл) — динамика декларативными атрибутами: блоки дашборда сами перезапрашивают HTML-фрагменты (`hx-get` + `hx-trigger="every 5s"`), JavaScript не пишется;
- **Pico.css** — classless: семантичный HTML выглядит аккуратно без вёрстки, тёмная тема из коробки.

**Ассеты (htmx.min.js, pico.min.css) включаются в пакет** и отдаются через `StaticFiles` (`/ui/static/...`). Внешние CDN не используются — UI обязан работать в офлайн/ограниченной сети.

#### Встроенные страницы

| Страница | Содержимое |
|---|---|
| `GET /ui` | Дашборд: глубина очереди, счётчики событий за 24 ч по видам, состояние курсоров по источникам, таблица пайплайнов; автообновление блоков htmx-поллингом |
| `GET /ui/pipelines` | **Графическая визуализация пайплайнов** (см. ниже) |
| `GET /ui/events` | Журнал аналитики: таблица с фильтрами по `kind`/`pipeline` (формы htmx, без перезагрузки страницы) |
| `GET /ui/fragments/*` | HTML-партиалы для поллинга (внутренние, используются страницами) |

#### Страница `/ui/pipelines`: визуализация и управление

Трёхдольный граф «источники → пайплайны → получатели», **SVG генерируется на сервере** Jinja-шаблоном (колонки узлов + кривые связей) — без графических JS-библиотек, в рамках стека §12.6. Данные — из конфига и портов (`RuntimeConfigPort`, `AnalyticsPort`, `EventQueuePort`):

- цвет узла пайплайна — статус: активен / на паузе (динамическая настройка §12.8) / есть `failed`-события за последний час;
- аннотации: `delivered` за 24 ч и текущая глубина очереди по пайплайну;
- клик по узлу пайплайна (только admin) — htmx-форма pause/resume, вызывающая существующие admin-ручки §12.8, с обновлением фрагмента без перезагрузки;
- автообновление графа — htmx-поллинг.

Страница доступна обеим ролям (просмотр), управление — только admin. Визуализация отображает декларативную топологию конфига; **редактирование топологии через UI — вне рамок** (статическая конфигурация, §12.8).

Данные страницы получают через те же DI-зависимости портов (§12.5); прямой доступ к ORM из шаблонного слоя запрещён, как и из JSON-ручек. Дашборд и журнал — read-only; вход `/ui/login`, регистрация `/ui/register`, администрирование пользователей `/ui/users` (только admin) — §12.7.

#### Пользовательские страницы (контракт расширения)

Симметричен пользовательским JSON-ручкам:

1. Библиотека публикует свой Jinja-environment и базовый layout: пользовательский шаблон начинается с `{% extends "angarion/base.html" %}` и получает навигацию, стили и автообновление бесплатно.
2. Каталог шаблонов пользователя подключается через `ChoiceLoader` (поиск: сначала пользовательский каталог, затем встроенный).
3. Регистрация: `register_page(title="Digest", path="/ui/digest", router=my_ui_router)` — страница автоматически появляется в навигации дашборда.
4. В роутах страницы — те же `AnalyticsDep` / `StateDep` / `RegistryDep` и т.д.

```python
ui = APIRouter()

@ui.get("/ui/digest", response_class=HTMLResponse)
async def digest_page(request: Request, state: StateDep, analytics: AnalyticsDep):
    return templates.TemplateResponse(request, "digest.html", {
        "pending": await state.get("digest", "buffer_size"),
        "delivered": await analytics.counts_by_kind(since=day_ago(), pipeline="digest"),
    })

app = create_app(deps, pages=[Page("Digest", "/ui/digest", ui)])
```

#### Тестирование (TDD)

Страницы и фрагменты — тем же ASGI-клиентом на InMemory-портах: статус-коды, наличие ключевых маркеров в HTML (значения счётчиков, строки таблиц), корректность подключения пользовательской страницы в навигацию.

### 12.7. Аутентификация и авторизация (fastapi-users)

#### Выбор и обоснование

База — **fastapi-users** с адаптером `fastapi-users-db-sqlalchemy`: асинхронный, ложится в наш стек SQLAlchemy 2.0 (таблица пользователей — обычная ORM-сущность, миграция через Alembic), из коробки даёт хэширование паролей (argon2), стратегии токенов и готовые auth-роутеры. Приложение рассчитано на работу **в общей сети**: авторизация включена по умолчанию.

#### Модель пользователей и ролей

- ORM-сущность `UserRow` (id, login, password_hash, role, is_active, registered_at, approved_at, approved_by) — в `adapters/storage/orm.py`, схема — миграцией Alembic, как и всё остальное.
- Роли v1: **`admin`** и **`viewer`**. Встроенные диагностические ручки и страницы доступны обеим ролям; управление пользователями — только `admin`.

#### Жизненный цикл пользователя: саморегистрация + одобрение администратором

1. **Регистрация**: форма `/ui/register` (и `POST /api/v1/auth/register`) — логин + пароль. Аккаунт создаётся с `is_active = false`: войти им **нельзя** (fastapi-users отклоняет неактивных на логине), он лишь появляется в списке ожидающих. Страница сообщает: «Заявка отправлена, дождитесь одобрения администратора».
2. **Одобрение**: администратор в Web UI (`/ui/users`) видит ожидающие заявки и активирует аккаунт, назначая роль (по умолчанию `viewer`). С этого момента — реальный доступ.
3. Деактивация/удаление/смена роли — там же.

Защита от замусоривания: лимит `max_pending_registrations` (конфиг, default 20) — при достижении регистрация временно закрывается с понятным сообщением; повторная регистрация занятого логина невозможна. Опционально (`registration_enabled = false`) саморегистрацию можно отключить целиком — тогда пользователей заводит админ через `/ui/users`.

#### Уведомление администратора о заявке (dogfooding)

Опциональная секция `[api.auth.notify]` (account + chat_id): при создании заявки регистрация ставит в **командный outbox** (§12.9) команду `notify` с текстом «Новая заявка на регистрацию: `<login>`. Одобрение: `<base_url>/ui/users`». Команду исполняет процесс-владелец Telethon-клиентов через штатный `MessageSinkPort` — библиотека доставки сообщений уведомляет о себе своим же механизмом, и схема одинаково работает в комбинированном и раздельном режимах. Постановка команды **неблокирующая**: сбой исполнения логируется и пишется в аналитику (`notify_failed`), но на саму регистрацию не влияет.

#### Администрирование — через Web UI и API, не через CLI

- **`/ui/users`** (страница, только `admin`): таблица пользователей со статусами, кнопки «одобрить (+роль)», «деактивировать», «удалить», «создать вручную» — всё на htmx, без перезагрузок.
- Симметричные JSON-ручки (только `admin`): `GET /api/v1/users`, `PATCH /api/v1/users/{id}` (активация/роль), `DELETE /api/v1/users/{id}`, `POST /api/v1/users`.
- Это единственное **write**-исключение из read-only-правила встроенных ручек и страниц.

#### Bootstrap первого администратора (без SSH-ритуалов в эксплуатации)

При старте **роли с включённым api**, если таблица пользователей **пуста**, приложение создаёт администратора из env: `ANGARION_ADMIN_LOGIN` / `ANGARION_ADMIN_PASSWORD` (заданы → создать и записать в лог; не заданы при `mode="users"` → fail-fast с внятным сообщением). Чистый `--role pipeline` без api эту проверку не выполняет. Env задаётся один раз при деплое — дальше вся работа с пользователями идёт через браузер.

CLI-команды (`angarion user ...`) сохраняются как **аварийный fallback** — на случай «забыл пароль единственного админа», не как основной инструмент.

#### Два транспорта аутентификации поверх одного хранилища пользователей

| Потребитель | Механизм |
|---|---|
| JSON API (`/api/v1/*`) | **JWT (Bearer)**: `POST /api/v1/auth/login` → access-токен; срок жизни `jwt_lifetime` (конфиг, default 1 ч); секрет — из env `ANGARION_API__SECRET` (обязателен при `auth = "users"`, иначе fail-fast) |
| Web UI (`/ui/*`) | **HTTPOnly-cookie** (cookie-transport fastapi-users): форма `/ui/login`, logout `/ui/logout`; `Secure`-флаг конфигурируем (за TLS-прокси — включить) |

Оба backend'а fastapi-users работают над одним user store — один пользователь, один пароль, два способа входа.

#### Применение к ручкам

- Авторизация навешивается зависимостью **на уровне роутеров** в `create_app`: встроенные JSON-ручки и UI-страницы защищены автоматически. Публичные исключения: `GET /api/v1/health` (liveness), `POST /api/v1/auth/login`, `POST /api/v1/auth/register`, страницы `/ui/login` и `/ui/register`, статика `/ui/static/*` (CSS/JS нужны странице логина до авторизации).
- Для пользовательских ручек библиотека экспортирует зависимости — это часть публичного API наряду с портами:

```python
from angarion.adapters.http.auth import CurrentUser, AdminUser

@my.get("/digest-status")
async def digest_status(user: CurrentUser, analytics: AnalyticsDep): ...

@my.post("/digest-flush")                 # пример: запись — только админу
async def digest_flush(user: AdminUser, state: StateDep): ...
```

- Пользовательские роутеры, переданные в `create_app(routers=..., pages=...)`, по умолчанию закрыты `CurrentUser`; открытая ручка — осознанное действие (`public=True` при регистрации роутера).

#### Режимы

`auth = "users"` (default) | `"none"` — для разработки и строго локальных запусков; при `"none"` и bind не на `127.0.0.1` приложение пишет громкое предупреждение в лог при старте.

#### Тестирование (TDD)

ASGI-тесты: матрица «аноним / pending / viewer / admin» × «health / register / встроенная ручка / users-ручка / UI-страница» (401/403/200); сценарий полного цикла «регистрация → отказ в логине (неактивен) → одобрение админом → успешный логин»; bootstrap админа на пустой БД; лимит pending-регистраций; логин-логаут cookie-сценарий; отправка уведомления о заявке (InMemory-sink) и невлияние его сбоя на регистрацию.

### 12.8. Динамические настройки и административные операции

#### Два класса настроек (принципиальное разделение)

| Класс | Состав | Источник | Изменение |
|---|---|---|---|
| **Статические** | аккаунты (api_id/api_hash, сессии), источники/цели пайплайнов, пути queue/storage, host/port, секреты | TOML + env | правка файла + перезапуск; в UI — **read-only**, секреты маскируются |
| **Динамические** | пауза/возобновление пайплайнов; `registration_enabled`, `max_pending_registrations`; лимиты catch-up; параметры троттлинга sender'а; log level | БД (поверх файла) | Web UI / API, **применяются без перезапуска** |

Обоснование: статические настройки — это композиция приложения (bootstrap, клиенты Telethon, подписки); их горячая замена требует пересборки рантайма и не окупается. Секреты через веб-форму не редактируются принципиально.

#### Механизм динамических настроек

- DTO `DynamicSettings` (Pydantic, валидация значений) + порт `RuntimeConfigPort` (`load() -> DynamicSettings`, `save(patch) -> DynamicSettings`); реализация — ORM-сущность `AppSettingRow` (key-value JSON) + InMemory для тестов.
- Приоритет: значение из БД (если задано) перекрывает значение из файла; «сбросить к файлу» — удаление override'а (кнопка в UI).
- Применение на лету: компоненты читают динамические значения через `RuntimeConfigPort` в начале каждой итерации (worker — пауза пайплайна, sender — лимиты троттлинга) либо подписаны на in-process событие `settings_changed` (log level).
- Пауза пайплайна: ingest продолжает принимать и класть в очередь (события не теряются); worker откладывает envelope'ы приостановленного пайплайна (возврат в хвост очереди с задержкой, событие `deferred`); возобновление — обработка накопленного. Семантика: пауза ≠ потеря.

#### Страница `/ui/settings` (только admin)

Две зоны: «Конфигурация» — эффективные статические значения read-only (секреты: `api_hash = ********`); «Управление» — формы динамических настроек с текущими значениями и пометкой источника (файл/override).

#### Административные операции (только admin)

| Операция | Ручка / UI | Механика |
|---|---|---|
| Перезапуск | `POST /api/v1/admin/restart` + кнопка | graceful shutdown (как SIGTERM) с кодом 0; **подъём — обязанность супервизора** (systemd `Restart=always`); требование к деплою фиксируется в README. Семантика в раздельном режиме: операция действует на **процесс, её получивший** (api); для перезапуска pipeline-процесса ставится команда `restart_pipeline` в outbox (§12.9). UI предупреждает перед подтверждением и указывает, какой процесс будет перезапущен |
| Пауза/возобновление пайплайна | `POST /api/v1/admin/pipelines/{name}/pause|resume` + переключатели на дашборде | через динамические настройки (см. выше) |
| Requeue DLQ | `POST /api/v1/admin/dlq/{id}/requeue` (+ `GET /api/v1/admin/dlq`, страница `/ui/dlq`) | envelope из `dead_letters` возвращается в очередь с `attempt = 0`; запись помечается `requeued_at` |
| Ручной catch-up | `POST /api/v1/admin/catchup/{source_key}` | команда `catchup` в outbox (§12.9); исполняет pipeline-процесс — внеочередной прогон алгоритма §9.3 по источнику |

**Аудит**: каждая операция и каждое изменение динамической настройки пишутся в аналитику событием `admin_op` (payload: пользователь, операция, старое/новое значение) и видны в `/ui/events`. Все операции выполняются через порты; подтверждение в UI — для разрушительных (restart, requeue).

#### Тестирование (TDD)

Контрактные тесты `RuntimeConfigPort` (InMemory + SQLAlchemy); приоритет БД-override над файлом и сброс; пауза → события копятся → возобновление → доставка без потерь и дублей; requeue DLQ → повторная обработка с `attempt = 0`; запрет операций для viewer (403); наличие `admin_op` в аналитике после каждой операции.

### 12.9. Командный outbox (мост api → pipeline)

Раздельный режим порождает асимметрию: Telethon-клиенты живут только в pipeline-процессе, а команды, требующие клиентов (уведомление о заявке, ручной catch-up, перезапуск pipeline), инициируются в api-процессе. Мост — **командный outbox** в общей БД:

- ORM-сущность `OutboxCommandRow` (id, kind, payload JSON, created_at, status: pending/done/failed, result/error, executed_at), порт `CommandOutboxPort` (§5), InMemory-реализация для тестов.
- **Producer** — api-процесс (и application-слой в комбинированном режиме); **consumer** — pipeline-процесс: фоновая задача поллит pending-команды (интервал `outbox_poll_seconds`, default 5), исполняет, помечает done/failed. Результат виден в UI (статус команды на странице операции).
- Семантика — at-least-once с защитой по статусу: команда берётся в работу атомарным `UPDATE ... WHERE status='pending'` (`rowcount`), повторное исполнение исключено в пределах одной записи.
- Виды команд v1: `notify` (отправка через `MessageSinkPort`), `catchup` (source_key), `restart_pipeline` (graceful shutdown pipeline-процесса). Расширение — добавление вида команды, без изменения механизма.
- В комбинированном режиме механизм тот же (producer и consumer в одном процессе) — поведение едино, тестируется одинаково.

#### Тестирование (TDD)

Контрактные тесты `CommandOutboxPort` (InMemory + SQLAlchemy); атомарность взятия команды; цикл «регистрация → команда notify в outbox → исполнение → done»; failed-команда с ошибкой видна в аудите.

### 12.10. Матрица возможностей адаптеров (мультимессенджерность)

#### Механизм

Каждый адаптер мессенджера декларирует свои возможности классом-константой:

```python
class AdapterCapabilities(BaseModel):
    user_account: bool        # доступ от личного аккаунта (не только бот)
    edit_events: bool         # события редактирования
    delete_events: bool       # события удаления
    history_fetch: bool       # доступ к истории (необходим для catch-up)
    threads: bool             # топики/треды (thread_id в Address)
    push_transport: str       # рекомендованные значения: "client" | "webhook" | "longpoll";
                              # открытая строка — сторонние транспорты допустимы
```

Bootstrap сверяет требования конфигурации с возможностями и реагирует **управляемой деградацией, а не падением**:

- пайплайн подписан на `message_edited`, а адаптер источника не даёт `edit_events` → fail-fast с внятным сообщением (конфигурация невыполнима);
- `history_fetch = false` → catch-up по источнику отключается автоматически, при старте — предупреждение в лог и событие `catchup_unavailable` в аналитику;
- `threads = false`, а в конфиге задан `thread_id` → fail-fast;
- `push_transport = "webhook"` → listener реализуется роутом в HTTP-адаптере (§12.5), вебхук-секрет — из env.

Матрица отображается в `/ui/diagnostics` (что умеет каждый подключённый адаптер). Контрактные тесты портов параметризуются возможностями: адаптер обязан проходить набор, соответствующий его декларации.

#### Кандидаты на адаптеры (приоритезация M7+, по результатам обзора 2026-06)

| Платформа | Профиль | Приоритет |
|---|---|---|
| **Matrix** | user_account ✓, edit ✓ (`m.replace`), delete ✓ (redactions), history ✓ (sync), self-hosted | **№1**: единственный полный аналог Telegram-профиля; идеален как доказательство переносимости портов |
| **MAX** | бот (официальный Bot API, webhook; userbot официально отсутствует), edit/delete/history — верифицировать (наследие TamTam) | **№2**: жизненная необходимость в РФ-контексте; первый тест механизма деградации |
| **VK** | бот сообщества (Bots Long Poll), `message_edit` ✓, история ✓; user-token к сообщениям закрыт | №3 |
| **Discord** | только бот (self-bot запрещён), edit/delete/history ✓ (gateway) | по запросу |
| WhatsApp | официально — без чтения групп участником; неофициальные пути — риск бана | **отклонён** |
| Signal | signal-cli (linked device), профиль ограничен | по запросу |

Зарезервированные значения `Messenger` — §4.1. Решение о реализации каждого адаптера — отдельное, вне рамок v1.

### 12.11. Расширение третьими сторонами (plugin contract)

Цель: пользователь добавляет поддержку произвольной платформы (мессенджер, email, внешняя очередь сообщений как источник/приёмник), альтернативную внутреннюю очередь или хранилище **отдельным pip-пакетом, без форка библиотеки**.

#### Контракт плагина адаптера платформы

Entry point группа **`angarion.adapters`**; плагин предоставляет объект:

```python
class AdapterPlugin(BaseModel):
    name: Messenger                          # идентификатор платформы ("xyz")
    capabilities: AdapterCapabilities        # §12.10
    account_config_model: type[BaseModel]    # Pydantic-схема секции [accounts.*]
                                             # этой платформы (свои токены/URL/секреты)
    make_listener: ListenerFactory           # (deps, accounts, sources) -> Listener
    make_sender: SenderFactory               # (deps, accounts) -> MessageSinkPort
    webhook_router: APIRouter | None = None  # для push_transport="webhook":
                                             # роуты монтируются в create_app
```

**Протокол `Listener`** (формализованный жизненный цикл driving-адаптера):

```python
class Listener(Protocol):
    async def start(self) -> None: ...       # подключение, catch-up, live-подписка
    async def stop(self) -> None: ...        # graceful shutdown (вызывается ядром)
    async def catchup(self, source_key: str) -> None: ...
        # ручной catch-up (outbox-команда §12.9 диспетчится сюда);
        # при capabilities.history_fetch=False — поднимает NotSupportedError
```

Listener получает `IngestService` через deps и эмитит в него `InboundEvent`'ы, собранные **публичными хелперами ключей** (§7.2). Bootstrap: загружает плагины из entry points → валидирует `[accounts.*]` моделью соответствующего плагина (неизвестный `messenger` → fail-fast с перечнем зарегистрированных) → сверяет конфигурацию пайплайнов с capabilities (§12.10) → монтирует webhook-роутеры.

#### Альтернативные очереди и хранилища

Entry points **`angarion.queues`** (реализация `EventQueuePort`) и **`angarion.storages`** (комплект storage-портов: dedup, registry, cursors, state, analytics, outbox, runtime-config). Значение `backend = "..."` в конфиге резолвится по реестру. Сторонний storage-бэкенд **сам владеет своей схемой и миграциями**: Alembic-миграции библиотеки относятся только к встроенному SQLAlchemy/SQLite-бэкенду.

#### Сертификация: `angarion.testing`

Контрактные наборы тестов всех портов (§13) публикуются как **импортируемый публичный пакет**: автор стороннего адаптера параметризует их своей реализацией и матрицей возможностей — зелёный прогон означает соответствие контракту. Это часть публичного API.

#### Публичный API и стабильность

Публичная поверхность (декларируется в документации и `__all__`): доменные DTO и `Messenger`; порты; хелперы `angarion.domain.keys`; `IngestService`; `AdapterPlugin`/`Listener`/`AdapterCapabilities`; DI-зависимости и auth-зависимости HTTP-адаптера; `create_app`/`Page`/`register_page`; базовый Jinja-layout; `angarion.testing`. Версионирование — **SemVer**: ломающие изменения публичной поверхности — только в мажорных версиях, с deprecation-периодом не менее одной минорной версии. Всё вне публичной поверхности (включая ORM-сущности) может меняться без предупреждения.

#### Примеры применимости (проверка модели)

- **Email**: `capabilities(user_account=True, edit_events=False, delete_events=True, history_fetch=True, threads=True, push_transport="client")` — IMAP IDLE как listener, SMTP как sender, `thread_id` = почтовый тред, `external_id` = Message-ID, subject — через `OutboundMessage.extra`. Catch-up работает (IMAP-выборка), события правок честно недоступны по матрице.
- **Внешняя очередь (RabbitMQ/Kafka/NATS) как платформа**: источник/приёмник событий — обычный `AdapterPlugin` (`chat_id` = топик/exchange). Как замена внутренней очереди — реализация `EventQueuePort` через `angarion.queues`.

---

## 13. Методика разработки и тестирование

### 13.1. TDD (обязательная методика)

Разработка ведётся по test-driven development: **тест пишется до кода** (red → green → refactor). Конкретизация для проекта:

- Для каждого порта сначала пишется **контрактный набор тестов** (поведенческая спецификация порта), затем InMemory-реализация доводится до зелёного, затем тот же набор натравливается на «настоящий» адаптер (SQLAlchemy, persist-queue).
- Use-case'ы application-слоя (ingest с fan-out, worker, catch-up-алгоритм) разрабатываются от сквозных тестов на InMemory-адаптерах.
- Багфиксы начинаются с воспроизводящего теста.
- Код без покрывающего теста не мержится; CI гоняет полный обязательный набор. Гексагональная структура здесь работает на TDD: ядро тестируется быстро и без I/O, что делает цикл red-green-refactor коротким.
- **Тестовое покрытие — не менее 90%** (pytest-cov, замер по обязательному набору; CI падает при снижении порога). Из расчёта покрытия исключаются: `migrations/` (Alembic), сгенерированный код, `__main__`-обвязка CLI. Покрытие — нижняя планка, а не цель: 90% бессодержательных тестов не заменяют контрактных.

### 13.2. Уровни тестов

1. **Unit/контрактные** (обязательные, CI): сквозной пайплайн на InMemory-адаптерах; контрактный набор тестов на каждый порт, прогоняемый по всем реализациям (InMemory и SQLAlchemy/persistqueue); маппинг Telethon-событий — на зафиксированных фикстурах; алгоритм catch-up — на синтетической истории (включая сценарии: правка во время простоя, удаление во время простоя, обрезка по лимиту); kill-тест M2 (см. §16); тесты SQLAlchemy-адаптеров — на временной SQLite-базе, схема разворачивается миграциями Alembic (это заодно проверяет сами миграции).
2. **Интеграционные** (опциональные, по умолчанию skip): pytest-маркер `integration`; запускаются на **реальном тестовом аккаунте и нескольких тестовых группах**; реквизиты (api_id/api_hash/телефон-сессия, id групп) — только из env / локального не-коммитимого файла. Сценарии: live-доставка new/edited/deleted сквозь весь пайплайн; multicast в две группы; рестарт с непустой очередью; catch-up (остановить listener → внести правки/удаления в группе → запустить → проверить эмиссию событий); дедуп при повторном catch-up. Темп — с большим запасом под лимиты Telegram; тесты должны прибирать за собой (удалять отправленные сообщения).

---

## 14. Нефункциональные требования

1. Python ≥ 3.12; полная типизация; `mypy --strict` для `domain/` и `application/`.
2. **Линтеры и статический анализ** — обязательная часть CI: `ruff` (lint + format) и `mypy` на всём пакете. **Каталог `migrations/` (Alembic) исключается** из проверок линтеров, mypy и расчёта покрытия — это сгенерированный/шаблонный код со своими конвенциями. Конфигурация исключений — централизованно в `pyproject.toml`.
3. **Разработка ведётся по TDD** (§13.1), тестовое покрытие — **не менее 90%** (там же) — методологические требования наравне с техническими.
4. Ядро — asyncio; синхронные библиотеки изолируются в адаптерах (`to_thread`).
5. Слой хранения: **SQLAlchemy 2.0 (async, `sqlite+aiosqlite`) + Alembic**; доступ к БД — только через storage-адаптеры (§12.3), raw SQL вне адаптеров запрещён.
6. Логирование — `structlog`, JSON-режим; correlation id = `event.uid` сквозь пайплайн.
7. Graceful shutdown (SIGINT/SIGTERM): стоп приёма → дообработка текущего item → закрытие клиентов и БД; курсоры сохраняются.
8. Запуск: `angarion run --config app.toml` (опц. `--with-api`); раздельный режим: `--role pipeline` (ingest+worker, владелец Telethon-клиентов) и `--role api`; `angarion migrate` — применение миграций Alembic.
9. Упаковка: `pyproject.toml`; extras `angarion[telegram]`, `angarion[sqlite]` (SQLAlchemy+Alembic+aiosqlite), `angarion[api]` (FastAPI+uvicorn+Jinja2+fastapi-users[sqlalchemy]; статика htmx/pico — внутри пакета), `angarion[llm]`; ядро без инфраструктурных зависимостей.

---

## 15. Зафиксированные решения (бывшие открытые вопросы)

1. **Multicast** — да, в v1; реализация через fan-out на ingest (§6.1).
2. **Строгий FIFO** — не требуется; при worker=1 порядок в основном сохраняется, но ретраи (re-enqueue), пауза пайплайнов и requeue DLQ его локально нарушают — это допустимо.
3. **Тексты сообщений хранятся в полном объёме** в реестре, включая историю версий (решение пересмотрено в v0.3; ранее планировались только хэши). Глубина хранения управляется `registry_window` (`0` = бессрочно). Аналитика (`events`) по-прежнему хранит только метаданные и ссылки, тексты живут в реестре.
4. **Reply-цепочки** — перенос связи в целевую группу не реализуется; признак ответа доступен в событии (`reply_to_external_id`).
5. **Catch-up** — реализуется в полном объёме (§9), включая редактирования и удаления в пределах окна реестра.
6. **Stateful-процессоры** — закладываются сейчас (`StateStorePort`, §10.3).
7. **Редактирования** — must have, отслеживаются надёжно (live + catch-up). **Удаления** — отслеживаются. **Ответы** — атрибут нового сообщения. **Реакции/лайки** — игнорируются.
8. **Хранилище** — SQLAlchemy 2.0 (async) + Alembic (v0.4); ORM-сущности приватны для адаптеров, домен оперирует только Pydantic DTO.
9. **Методика разработки** — TDD (v0.4, §13.1).
10. **Web API** — FastAPI в составе библиотеки (v0.5): встроенная read-only диагностика + расширение пользовательскими роутерами через DI поверх портов (§12.5).
11. **Web UI** — SSR-дашборд Jinja2 + htmx + Pico.css (v0.6, §12.6): без Node/сборки, ассеты в пакете (офлайн-режим), пользовательские страницы через общий layout и `register_page()`.
12. **Auth** — fastapi-users (v0.8–0.9, §12.7): JWT для API + cookie для UI, роли admin/viewer; **саморегистрация с обязательным одобрением админа** (неактивный аккаунт до активации), администрирование пользователей через Web UI `/ui/users`, bootstrap первого админа из env при пустой БД, CLI — только аварийный fallback; авторизация включена по умолчанию.
13. **Уведомление о заявках** (v0.10–0.11, §12.7) — опционально, в Telegram через командный outbox и штатный `MessageSinkPort`; неблокирующее, сбой не влияет на регистрацию.
14. **Динамические настройки и админ-операции** (v0.10, §12.8) — операционные параметры редактируются админом через Web UI (БД-override поверх файла, без перезапуска); статическая конфигурация и секреты — только файл+env, в UI read-only. Операции: restart (через супервизор), пауза пайплайнов (без потери событий), requeue DLQ, ручной catch-up; всё с аудитом `admin_op`.
15. **Роли процессов** (v0.11) — `pipeline` (ingest+worker, единоличный владелец Telethon-клиентов; не разделяется из-за ограничения сессий Telethon) и `api`; мост между ними — командный outbox (§12.9).
16. **Ретраи** (v0.11, §8) — re-enqueue с инкрементом `attempt` в envelope; `nack` — только аварийный механизм recovery.
17. **Качество кода** (v1.0) — покрытие ≥ 90% (pytest-cov, порог в CI); ruff + mypy на всём пакете; `migrations/` исключён из линтеров, mypy и покрытия.
18. **tgcf** (v1.1) — интеграция как зависимости **отклонена** (приложение с глобальным состоянием, без публичного API; нарушает изоляцию ядра). Принято: ревизия исходников перед M3 и заимствование отдельных решений под MIT с атрибуцией; tgcf допускается как временное standalone-решение прикладной задачи на период разработки M1–M3. Shim совместимости tgcf-плагинов — отклонён (себестоимость выше выгоды).
19. **Мультимессенджерность** (v1.2, §12.10) — матрица возможностей адаптеров с управляемой деградацией (catch-up отключается при отсутствии history_fetch, невыполнимая конфигурация — fail-fast); курсор catch-up непрозрачен, семантикой владеет адаптер; `thread_id` в адресации; кандидаты M7+: Matrix (№1, полный профиль) → MAX (№2) → VK (№3); WhatsApp отклонён.
20. **Сторонняя расширяемость** (v1.3, §12.11) — `Messenger` открыт (строка + реестр плагинов); контракт `AdapterPlugin` с entry points `angarion.adapters`/`angarion.queues`/`angarion.storages`; формализованный жизненный цикл `Listener` (start/stop/catchup); `OutboundMessage.extra`; публичные хелперы ключей; `angarion.testing` как сертификационный набор; публичный API под SemVer с deprecation-политикой. `EventKind` остаётся закрытым осознанно (три вида событий — это домен; расширение видов = мажорная версия). Встроенный Telegram-адаптер реализуется через тот же plugin-контракт (нулевой пациент механизма).
21. **Эксплуатация и эволюция** (v2.0, §17) — лицензия MIT; пакет 1.0.0 после M5; ретеншн всех данных параметризован; время — строго UTC; порог глубины очереди; бэкап-гайд и маскирование секретов в логах обязательны; санкционированные пути эволюции (параллелизм по пайплайнам, аддитивные медиа, новые команды/настройки) не требуют ревизии ТЗ; документация — deliverable каждого этапа.
22. **Визуализация пайплайнов** (v2.1, §12.6) — серверный SVG без графических JS-зависимостей; управление через существующие admin-ручки; отображение топологии, но не её редактирование (конструктор пайплайнов через UI — отклонён, топология остаётся статической конфигурацией).

---

## 16. Этапы реализации

| Этап | Содержимое | Критерий готовности |
|---|---|---|
| **M1** | Домен (события, `thread_id`, открытый `Messenger`), порты (вкл. `AdapterCapabilities`, непрозрачный курсор, контракт `AdapterPlugin`/`Listener`), application (fan-out, worker, деградация по матрице), реестр плагинов через entry points, InMemory-адаптеры (оформлены как плагин — нулевой пациент), конфиг, хелперы ключей | Сквозной тест new/edited/deleted + multicast, всё in-memory; fail-fast/деградация по матрице покрыты; InMemory-плагин загружается через entry point |
| **M2** | PersistQueue-адаптер; SQLAlchemy-адаптеры (dedup, registry, cursors, state, analytics) + Alembic-миграции; DLQ; **публикация `angarion.testing`** | Контрактные тесты зелёные на реальных реализациях (через angarion.testing); kill -9 в произвольный момент не теряет и не дублирует доставку |
| **M3** | Telethon listener + **catch-up** + sender **через plugin-контракт §12.11**, мультиаккаунт, троттлинг, CLI. **Перед реализацией — ревизия исходников tgcf (MIT)**: FloodWait/реконнекты, пагинация истории, резолв сущностей, session string; заимствованные фрагменты помечаются атрибуцией | Боевой пайплайн работает сутки; тест «простой → правки/удаления → рестарт → корректные события» проходит |
| **M4** | LLM-процессор; пример stateful-процессора (дайджест) | Обработка через локальную модель |
| **M5** | Web API + Web UI + Auth + Admin: `create_app`, DI; диагностика и журнал; **визуализация `/ui/pipelines`**; fastapi-users (регистрация с одобрением, /ui/users, bootstrap, уведомление о заявках); динамические настройки и `/ui/settings`; админ-операции с аудитом; **командный outbox** | ASGI-тесты зелёные: auth-матрица, цикл «регистрация → команда notify → исполнение → done», пауза без потерь (вкл. через /ui/pipelines), requeue с attempt=0, контракты RuntimeConfigPort и CommandOutboxPort (§12.5–12.9) |
| **M6** | Интеграционный тестовый контур на реальном аккаунте | Набор §13.2 зелёный |
| **M7** | Медиа; второй мессенджер по приоритету §12.10 (Matrix → MAX → VK) — подтверждение переносимости портов и механизма деградации | — |

---

## 17. Эксплуатация и эволюция (закрытие отложенных вопросов)

### 17.1. Лицензия

**MIT** — совместима с заимствованиями из tgcf (MIT, §15.18), максимально дружелюбна к авторам сторонних плагинов и коммерческому использованию. Файл LICENSE — с первого коммита.

### 17.2. Версионирование пакета (≠ версия ТЗ)

Версии пакета на PyPI — независимый SemVer: `0.x` на M1–M4 (публичный API best-effort, ломающие изменения допустимы с changelog'ом), **`1.0.0` — по завершении M5** (с этого момента действуют гарантии стабильности §12.11 в полном объёме). Данное ТЗ версионируется отдельно и на номера пакета не влияет.

### 17.3. Ретеншн данных

| Данные | Параметр | Default |
|---|---|---|
| Дедуп-ключи | `dedup_ttl_days` | 30 |
| Реестр сообщений + версии | `registry_window_days` | 7 (`0` = бессрочно) |
| Аналитика (`events`) | `analytics_retention_days` | 90 (`0` = бессрочно) |
| DLQ | не чистится автоматически | разбор — ручной (requeue/удаление в `/ui/dlq`) |

Все prune-задачи — фоновые в pipeline-процессе, факт очистки — событие в аналитике.

### 17.4. Конвенция времени

Все временные значения — **UTC**: в DTO — `AwareDatetime` (наивные datetime запрещены, валидируется Pydantic), в БД — ISO 8601 с явным смещением. Отображение в локальной зоне — задача UI (браузер) и только его.

### 17.5. Мониторинг здоровья очереди

Порог `queue.depth_warn` (default 500): превышение → событие `queue_depth_warning` (не чаще раза в 10 минут) + подсветка на дашборде. Защита от тихого распухания очереди при остановленном worker'е или заваленном пайплайне.

### 17.6. Бэкапы (README-гайд, обязательная часть документации)

Бэкапить: `app.db` (горячая копия через SQLite backup API / `VACUUM INTO`, не файловое копирование под нагрузкой; для непрерывной репликации — Litestream), session-файлы (`chmod 600`, бэкап в зашифрованном виде), TOML-конфиг. `queue.db` — по выбору: потеря очереди при живом catch-up восстановима для событий в окне реестра.

### 17.7. Секреты в логах

Обязательный structlog-процессор маскирования: значения ключей `api_hash`, `token`, `password`, `secret`, `authorization` (без учёта регистра, рекурсивно по payload) заменяются на `***` до записи. Покрывается тестом.

### 17.8. Санкционированные пути эволюции (заранее одобренные направления, не требующие ревизии ТЗ)

1. **Параллелизм worker'ов**: один worker на пайплайн (элементы очереди уже изолированы по пайплайнам — §6.1) — без изменения портов и гарантий. Параллелизм внутри пайплайна — запрещён без ревизии (ломает порядок и упрощения идемпотентности).
2. **Медиа (M7)**: опциональное поле `media: list[MediaRef] | None` в `InboundEvent` — аддитивное изменение (минорная версия); до M7 метаданные медиа доступны через `raw`. Политика скачивания/хранения — проектируется в M7.
3. **Управляющие операции v2** (пауза источника, редактирование пайплайнов через UI): через существующие механизмы динамических настроек и outbox — аддитивно.
4. **Новые виды outbox-команд и динамических настроек** — аддитивно по построению.

### 17.9. Документация (deliverable, не послесловие)

mkdocs-material; обязательные разделы: быстрый старт (готов к M3), деплой (systemd, бэкапы, reverse proxy — к M5), **гайд автора плагина** с walkthrough по `angarion.testing` (к M5), справочник публичного API (автогенерация, с M2), реестр ограничений платформ (§9.4 и аналоги — пополняется с каждым адаптером). Релиз этапа без его документации не считается завершённым.

---

## 18. Журнал ревизий

| Версия | Суть |
|---|---|
| v0.1 | Первоначальный каркас: гексагональная архитектура, Pydantic-домен, порты, persist-queue + SQLite |
| v0.2 | Домен переведён на события (new/edited/deleted); catch-up; stateful-процессоры; multicast; DTO; интеграционные тесты |
| v0.3 | Реестр хранит полные тексты с историей версий |
| v0.4 | SQLAlchemy 2.0 + Alembic; TDD |
| v0.5 | Web API (FastAPI) как driving-адаптер; DI поверх портов |
| v0.6 | Web UI: Jinja2 + htmx + Pico.css |
| v0.7 | Имя **angarion** |
| v0.8 | Auth: fastapi-users, JWT + cookie, роли |
| v0.9 | Саморегистрация с одобрением; администрирование через UI; bootstrap из env |
| v0.10 | Уведомление о заявках; динамические настройки; админ-операции |
| v0.11 | Правки архитектурной ревизии: порядок ingest, командный outbox, роли pipeline/api, ретраи через attempt |
| v1.0 | Покрытие ≥90%, линтеры; первая заморозка |
| v1.1 | Ревизия tgcf: заимствование под MIT, интеграция отклонена |
| v1.2 | Мультимессенджерность: матрица возможностей, непрозрачный курсор, thread_id |
| v1.3 | Сторонняя расширяемость: plugin-контракт, entry points, angarion.testing, SemVer |
| **v2.0** | **Базовая линия**: §17 (эксплуатация и эволюция), журнал ревизий; открытых вопросов нет |
| v2.1 | Минорный аддитивный релиз (§17.8.3): страница `/ui/pipelines` — серверный SVG-граф пайплайнов с pause/resume |


