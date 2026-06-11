# Plan: T002 — M1: ядро angarion

**Дата:** 2026-06-11
**База:** `spec.md` (Analyzed), ТЗ `CONCEPT.md` v2.1
**Назначение:** дизайн-решения, отложенные из clarify/analyze
(C-8, A-4…A-6, A-9), структура кода и нарезка на фазы реализации.

---

## 1. Структура пакета (M1-подмножество §3.2, src-layout)

```
src/angarion/
├── domain/
│   ├── models.py        # DTO §4 + QueueEnvelope/QueueItem/QueueDepth/
│   │                    #   DeliveryReceipt/RegistryRecord/RegistryVersion/
│   │                    #   RegistryDelta/SourceCursor/TargetSpec
│   ├── ports.py         # Protocol-порты M1 (FR-5)
│   ├── errors.py        # доменные исключения §8
│   ├── keys.py          # make_source_key/make_dedup_key/
│   │                    #   normalize_and_hash/make_idempotency_key
│   ├── capabilities.py  # AdapterCapabilities (§12.10)
│   └── plugin.py        # AdapterPlugin, Listener (§12.11, без webhook_router — A-1)
├── application/
│   ├── ingest.py        # IngestService (§6.1)
│   ├── router.py        # Router (§6.2)
│   ├── worker.py        # PipelineWorker + ScopedStateStore + retry/defer (§6.3, §8, C-8)
│   └── processors.py    # реестр процессоров, @processor, passthrough (§10.1)
├── adapters/
│   └── memory/
│       ├── queue.py     # MemoryQueue (EventQueuePort)
│       ├── storage.py   # MemoryDedup/Registry/Cursors/State/Analytics/DeadLetters
│       ├── sink.py      # MemorySink (MessageSinkPort) — журнал отправленного
│       ├── listener.py  # MemoryListener — программная инжекция событий
│       └── plugin.py    # PLUGIN: AdapterPlugin платформы "memory" + фабрики
│                        #   entry points angarion.queues / angarion.storages
├── config.py            # pydantic-settings (§11, объём C-4)
├── bootstrap.py         # composition root: плагины, валидация, деградация
└── log.py               # structlog-хелперы + маскирование секретов (§17.7)
                         #   (не logging.py — ruff A005, тень stdlib)
```

Тесты: `tests/unit/...` (зеркалит пакет) + `tests/contracts/` —
контрактные наборы портов, параметризуемые фабрикой реализации
(задел под `angarion.testing` в M2). `tests/integration/` (T014) не
трогаем.

## 2. Зафиксированные дизайн-решения

### 2.1. Нормализация текста для хэша (A-4, публичный контракт)

`normalize_and_hash(text)`:

1. Unicode-нормализация **NFC**;
2. перевод строк: `\r\n` и одиночный `\r` → `\n`;
3. **больше ничего** (без trim/lower/схлопывания пробелов —
   агрессивная нормализация склеила бы содержательно разные версии);
4. `sha256(utf-8).hexdigest()`.

Правила фиксируются тестами с golden-значениями; изменение — только
мажорная версия (§7.2). ADR в `DECISIONS.md`.

### 2.2. Retry/backoff (C-8 + A-5)

- `delay = min(backoff_base * 2**attempt, backoff_cap)`;
  default: `backoff_base = 1.0 с`, `backoff_cap = 60.0 с`,
  `max_retries = 5`. Конфиг — секция `[worker]` (аддитивно к §11).
- Механизм — defer-to-tail (C-8): retry кладётся
  `put(envelope.copy(attempt+1, not_before=now+delay))` **до** `ack`
  исходного; worker, получив envelope с `not_before` в будущем,
  возвращает его в хвост (`put`+`ack`) и спит
  `min(not_before - now, 1.0 с)`.
- Worker ловит исключение **процессора** (`Exception` → retry-ветка).
  Ошибки доставки после C-9 обрабатывает `DeliveryWorker` (см. 2.11);
  tenacity-политика внутри sender'а — по-прежнему M3, §8.

### 2.3. Вспомогательные формы (A-6)

- `QueueDepth(pending: int, unacked: int)`.
- `DeliveryReceipt(external_id: str | None, delivered_at: AwareDatetime)`
  — id отправленного сообщения у платформы, если она его сообщает.
- `QueueItem(envelope: QueueEnvelope, receipt: Any)` — receipt
  непрозрачен для ядра (у InMemory — внутренний int).

### 2.4. Idempotency-фабрика (A-9)

`ProcessorServices.make_idempotency_key: Callable[[InboundEvent,
Address, int], str]` — worker создаёт её частичным применением
`domain.keys.make_idempotency_key(...)` со своим `pipeline`.
`OutboundMessage.idempotency_key` — обязательное поле, его проставляет
**процессор** при конструировании сообщения, вызывая `svc`-фабрику с
порядковым `n` исходящего (passthrough так и делает). Worker ключи не
дочинивает — отсутствие ключа невозможно по типам.

### 2.5. Контракты entry points для очередей/хранилищ (§12.11 не детализирует)

- `angarion.queues`: объект `QueueBackend(name: str,
  make: Callable[[QueueConfig], EventQueuePort])`.
- `angarion.storages`: объект `StorageBackend(name: str,
  make: Callable[[StorageConfig], StorageBundle])`, где `StorageBundle`
  — контейнер портов dedup/registry/cursors/state/analytics/dead_letters.
- Резолв `backend = "..."` по имени в реестре загруженных entry points;
  неизвестное имя → `ConfigError` с перечнем известных.

### 2.6. InMemory-плагин («нулевой пациент»)

- Платформа `"memory"`, capabilities: `user_account=True,
  edit_events=True, delete_events=True, threads=True,
  history_fetch=False, push_transport="client"`.
  `history_fetch=False` — честно (истории нет) и заодно постоянно
  прогоняет ветку деградации `catchup_unavailable` в обычном запуске;
  `Listener.catchup()` поднимает `NotSupportedError`.
- `MemoryListener` — тестовая обвязка: метод `emit(raw_event)` →
  маппинг → хелперы ключей → `IngestService`. Поведенчески — настоящий
  driving-адаптер, просто события подаются программно.
- Entry points в `pyproject.toml` библиотеки: `angarion.adapters:memory`,
  `angarion.queues:memory`, `angarion.storages:memory`,
  `angarion.processors:passthrough`. SC-3 проверяет загрузку через
  `importlib.metadata`, без прямого импорта.
- Для SC-2 (матрица) тесты конструируют синтетические `AdapterPlugin`
  с урезанными capabilities — отдельный плагин-пакет не нужен.

### 2.7. Конфиг (C-4)

`AngarionSettings(BaseSettings)`: TOML-источник + env-override
(`ANGARION_*`, `__`-вложенность). Двухступенчатая валидация:

1. структурная (pydantic): секции `[accounts.*]` (как сырые dict —
   модель аккаунта принадлежит плагину), `[storage]`, `[queue]`,
   `[worker]` (2.2), `[catchup]`, `[pipelines.*]`;
2. ссылочная + плагинная (bootstrap): `accounts.*` валидируются
   `account_config_model` своего плагина; источники/цели ссылаются на
   существующие аккаунты; `messenger` — в реестре плагинов; матрица
   §12.10; инвариант `dedup_ttl_days ≥ registry_window_days`.

### 2.8. Логирование (C-6)

`angarion/logging.py`: `get_logger()` поверх structlog,
процессор-маскировщик §17.7 (ключи `api_hash/token/password/secret/
authorization`, без регистра, рекурсивно) — публичный, с тестом.
Библиотека глобальную конфигурацию structlog **не навязывает**
(это право приложения); bootstrap собирает `BoundLogger` для
`ProcessorServices.log` с `bind(event_uid=...)` (correlation id §14.6).

### 2.9. Рантайм без CLI

CLI — M3. В M1 bootstrap возвращает контейнер `AngarionApp`
(`ingest`, `worker`, `listeners`, порты) с `async start()/stop()`
(запуск worker-цикла asyncio-задачей, graceful-остановка: дообработать
текущий item, остановить listeners). Сквозные тесты гоняют его
напрямую.

### 2.10. Зависимости

`uv add pydantic pydantic-settings structlog` — всё. Без extras в M1
(FR-18). Dev-группа уже укомплектована (T001).

### 2.11. Outbox исходящих (C-9)

Разделение обработки и доставки. Контракты:

- `OutboundRecord` (DTO): `msg: OutboundMessage`,
  `status: OutboxStatus (pending|sent|failed)`, `attempts: int`,
  `next_attempt_at`, `created_at`, `finished_at | None` (момент
  sent/failed; по нему `prune`), `receipt: DeliveryReceipt | None`,
  `last_error: str | None`, контекст наблюдаемости
  `pipeline: str | None`, `event_uid: UUID | None`. Ключ записи —
  `msg.idempotency_key` (поле не дублируется).
- `OutboxPort`: `put(msg, *, pipeline, event_uid) -> bool`
  (insert-if-absent, False = дубль), `due(limit) -> list` (pending с
  `next_attempt_at <= now`, FIFO), `mark_sent(key, receipt)`
  (идемпотентно), `reschedule(key, *, not_before, error)`
  (attempts+1), `mark_failed(key, error)` (терминально),
  `get(key)`, `prune(older_than)` (только терминальные записи,
  по `finished_at`; A-7).
- `PipelineWorker`: process → `outbox.put(...)` на каждый outbound →
  `processed`/`dropped` → `ack`. `sink` и `dedup` из worker'а уходят
  (входной дедуп — в ingest; выходной — PK outbox).
- `DeliveryWorker` (`application/delivery.py`): цикл по одной
  due-записи (graceful-отмена дообрабатывает текущую, зеркально
  PipelineWorker): `send → mark_sent` + `delivered`; исключение →
  `attempts+1`; пока `attempts ≤ max_retries` — `reschedule` с тем же
  backoff 2.2, иначе `mark_failed` + `delivery_failed` (разбор
  ручной — аналог DLQ для исходящих). Пустой `due` → sleep
  `poll_interval` (default 1.0 с).
- Остаточное окно: падение между `send` и `mark_sent` → дубль при
  повторе (не потеря) — допустимо §7.1.

## 3. Фазы реализации

Каждая фаза: TDD, отдельный коммит на ветке `T002-m1-core`, 4 проверки
зелёные. Одна фаза ≈ одна сессия (`/clear` между). PR один на всю
T002, squash перед merge (C-5).

- **Фаза 1 — domain.** `models.py`, `errors.py`, `capabilities.py`,
  `plugin.py` (контракты), `keys.py` с нормализацией 2.1.
  Тесты: инварианты DTO (frozen/extra/JSON-roundtrip, aware-datetime),
  golden-тесты ключей и хэша, дедуп-семантика §7.2 (включая
  «правка вернула прежний текст = тот же ключ»). → SC-6.
- **Фаза 2 — порты + контрактные тесты + InMemory driven-адаптеры.**
  `ports.py`; `tests/contracts/` — поведенческие наборы на каждый порт
  (фикстура-фабрика, задел под `angarion.testing`); InMemory
  queue/dedup/registry/cursors/state/analytics/dead_letters/sink до
  зелёного. Особо: staleness-guard и история версий registry,
  recover/ack/nack очереди, `not_before` сохраняется round-trip. → SC-5.
- **Фаза 3 — application.** `router.py`, `ingest.py` (порядок §6.1,
  обогащение, fan-out, duplicate/unrouted), `worker.py` (инвариант
  «send+mark_delivered до ack», DROP, retry/defer 2.2, DLQ после
  max_retries, ScopedStateStore), `processors.py` (@processor,
  passthrough). Тесты — на InMemory-адаптерах напрямую (без bootstrap).
- **Фаза 3.5 — outbox исходящих (C-9).** `OutboundRecord`/`OutboxStatus`
  + `OutboxPort` (2.11) + контрактный набор + `MemoryOutbox`;
  `DedupStorePort` сужается до входа (минус `mark_delivered`, правка
  контракта и `MemoryDedupStore`); `DeliveryWorker`; перестройка
  `PipelineWorker` (process → outbox.put → ack). Тест «сбой доставки →
  сообщение доезжает ретраем» — фиксация ликвидации окна потери.
- **Фаза 4 — config + plugins + bootstrap + logging.** `config.py`
  (2.7), реестры entry points (2.5), `bootstrap.py` (загрузка,
  двухступенчатая валидация, матрица §12.10: 4 ветки, `AngarionApp`
  2.9), `logging.py` + тест маскирования; entry points InMemory в
  `pyproject.toml`. → SC-2, SC-3.
- **Фаза 5 — сквозные acceptance + закрытие.** E2E: new/edited/deleted
  с обогащением + multicast на двух пайплайнах (SC-1); ретрай-сценарий
  до DLQ; дедуп повторной подачи. Добивка coverage ≥ 90; ADR в
  `DECISIONS.md` (C-8 not_before; C-9 outbox исходящих; A-1
  webhook_router → M5; A-4 нормализация); `CHANGELOG.md [Unreleased]`;
  BOARD `Doing → Done`;
  squash → push → PR → self-review по чеклисту → merge.

## 4. Риски / заметки

- `select = ["ALL"]` у ruff на новом коде — ожидаемо шумный первый
  контакт; чиним, не глушим (правило проекта).
- Pydantic-модели с `Callable`-полями (`AdapterPlugin`,
  `QueueBackend`...) — проверить поведение mypy --strict + pydantic;
  при сопротивлении BaseModel допустим frozen dataclass (это
  конструкции композиции, не DTO — A-2), зафиксировать выбор в коде
  фазы 1.
- Контрактные тесты пишутся сразу реюзабельными (фабрика-фикстура),
  но публикация как `angarion.testing` — M2; не полировать
  преждевременно.
