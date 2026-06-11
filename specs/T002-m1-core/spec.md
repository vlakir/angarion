# Spec: T002 — M1: ядро angarion (домен, порты, application, InMemory-плагин)

**Статус:** Analyzed (clarify C-1…C-8 и analyze A-1…A-9 закрыты
2026-06-11; дизайн-решения — в `plan.md` рядом)
**Дата создания:** 2026-06-11
**Связанные документы:** `CONCEPT.md` (ТЗ v2.1) — §§3–11, 12.4,
12.10–12.11, 13–14, 16 (M1); `DECISIONS.md`; `BOARD.md` (T002)

> ТЗ v2.1 уже содержит полную техническую проработку. Эта спека **не
> дублирует** ТЗ, а фиксирует **границу этапа M1**: что из ТЗ входит в
> T002, что сознательно остаётся следующим этапам, и какие решения
> уточнены при clarify. При расхождении приоритет у ТЗ; уточнения
> clarify фиксируются здесь и (при архитектурной значимости) в
> `DECISIONS.md`.

---

## 1. Overview

Первый этап реализации angarion: работающий конвейер событий сообщений
целиком in-memory. После M1 библиотека существует как ядро: доменные
модели событий, порты, application-слой (ingest → очередь → worker →
доставка), контракт плагинов и InMemory-адаптеры как первый плагин
(«нулевой пациент» механизма расширения). Всё последующее (M2–M7) —
наращивание адаптеров без изменения ядра; M1 — единственный этап, где
ядро создаётся, поэтому его архитектурная аккуратность критична.

## 2. Сценарии использования

- Как **разработчик приложения на angarion**, я хочу собрать конвейер
  из конфига и InMemory-адаптеров, чтобы писать и отлаживать свой
  процессор без Telegram, БД и сети.
- Как **автор стороннего адаптера**, я хочу опираться на формальный
  контракт (`AdapterPlugin`, `Listener`, `AdapterCapabilities`, хелперы
  ключей), чтобы добавить свою платформу отдельным pip-пакетом без
  форка.
- Как **разработчики angarion (мы)**, я хочу ядро с зафиксированными
  портами, чтобы M2 (персистентность) и M3 (Telegram) были чистым
  добавлением адаптеров, без ревизии домена и application.

## 3. Functional Requirements

Нумерация FR-* — для ссылок из плана/тестов; содержимое — по ТЗ.

### Домен

- **FR-1** ДОЛЖНА: доменные DTO по §4 (Pydantic v2, `frozen=True`,
  `extra="forbid"`, JSON-сериализуемость): `Address` (с `thread_id`),
  `AccountRef`, `EventKind` (закрытый StrEnum), `InboundEvent`,
  `OutboundMessage`, `Verdict`, `ProcessingResult`, `AnalyticsEvent`,
  `PipelineContextData` + `ProcessorServices` (не-DTO), `QueueEnvelope`
  (расширен полем `not_before: AwareDatetime | None` — C-8),
  `QueueItem`, `SourceCursor` (непрозрачный payload), `RegistryRecord`,
  `RegistryVersion`, `RegistryDelta` (исходы: `is_new` / `text_changed` /
  `unchanged` / `stale` — A-3), `QueueDepth`, `DeliveryReceipt`,
  `OutboundRecord` / `OutboxStatus` (outbox исходящих — C-9),
  `TargetSpec`, `AdapterCapabilities` (§12.10), `AdapterPlugin` (§12.11).
  `DynamicSettings` / `OutboxCommand` — НЕ в M1: приходят вместе со
  своими портами в M5 (C-1).
- **FR-2** ДОЛЖНА: тип `Messenger` — открытая строка по паттерну §4.1,
  валидация по реестру загруженных плагинов при старте (fail-fast с
  перечнем известных).
- **FR-3** ДОЛЖНА: доменные исключения §8: `ProcessingError`,
  `DeliveryError`, `CatchupError`, `ConfigError`, `NotSupportedError`.
- **FR-4** ДОЛЖНА: публичные хелперы ключей `angarion.domain.keys`
  (§7.2–7.3): `make_source_key()`, `make_dedup_key()`,
  `normalize_and_hash()`, хелпер idempotency-ключа исходящего. Правила
  нормализации — публичный контракт (фиксируются тестами).

### Порты

- **FR-5** ДОЛЖНА: асинхронные `typing.Protocol`-порты §5:
  `EventQueuePort`, `MessageSinkPort`, `DedupStorePort` (после C-9 —
  только входная дедупликация: `mark_inbound`/`prune`),
  `OutboxPort` (C-9: журнал исходящих — put / due / mark_sent /
  reschedule / mark_failed / get / prune),
  `MessageRegistryPort`, `CursorStorePort`, `StateStorePort`,
  `AnalyticsPort`, `ProcessorPort`, протокол `Listener` (§12.11),
  `DeadLetterPort` (C-2). `RuntimeConfigPort` и `CommandOutboxPort` в M1
  НЕ определяются — добавятся аддитивно в M5 (C-1: порт без потребителя —
  мёртвый контракт).
- **FR-6** ДОЛЖНА: для каждого порта M1 — **контрактный набор тестов**
  (поведенческая спецификация, §13.1), который в M2+ будет натравлен на
  персистентные реализации. Оформление набора как публичного пакета
  `angarion.testing` — M2 (здесь — внутренние тесты, но написанные
  переиспользуемо).

### Application

- **FR-7** ДОЛЖНА: `IngestService` по §6.1 — последовательность строго:
  дедуп (до записей в реестр) → upsert/mark_deleted реестра со
  staleness-guard и обогащением (EDITED ← `previous_text`, DELETED ←
  текст и метаданные из реестра) → router (multicast + фильтры-предикаты,
  `only_replies`) → fan-out: отдельный `QueueEnvelope` на каждый пайплайн
  → `analytics.record("ingested")`. Событие `duplicate` / `unrouted` при
  соответствующих выходах.
- **FR-8** ДОЛЖНА: `Router` §6.2 — `(Address, EventKind) → set[pipeline]`,
  multicast.
- **FR-9** ДОЛЖНА: `PipelineWorker` §6.3 (в редакции C-9) —
  конкурентность 1; инвариант «все outbound зафиксированы в outbox
  строго до `ack`»; verdict DROP → без фиксации; аналитика
  `processed`/`dropped`. Доставка — отдельный `DeliveryWorker`:
  цикл по due-записям outbox, send → mark_sent (+`delivered`);
  сбой → reschedule с экспоненциальным backoff (параметры 2.2
  plan.md); после `max_retries` → статус `failed` (+`delivery_failed`,
  разбор ручной — аналог DLQ для исходящих). Идемпотентность выхода —
  insert-if-absent outbox по `idempotency_key` (вместо
  `dedup.mark_delivered`).
- **FR-10** ДОЛЖНА: обработка ошибок §8 — retry через re-enqueue с
  `attempt+1` и экспоненциальной задержкой. Механизм (C-8, вариант б):
  `put(envelope.copy(attempt+1, not_before=now+backoff))` **строго до**
  `ack` исходного (падение между ними → дубль, не потеря); worker,
  получив envelope с `not_before` в будущем, возвращает его в хвост
  (`put` + `ack`) с коротким sleep (cap ~1 с) против горячего цикла;
  после `max_retries` (default 5) → `failed` + DLQ через узкий
  `DeadLetterPort` (put/list/take) с InMemory-реализацией в M1,
  персистентная — M2 (C-2); `nack` — только аварийный механизм.
- **FR-11** ДОЛЖНА: `ScopedStateStore` — обёртка worker'а над
  `StateStorePort` с namespace = имя пайплайна (§10.3).

### Плагины и bootstrap

- **FR-12** ДОЛЖНА: реестр плагинов через entry points (§12.11):
  группы `angarion.adapters`, `angarion.queues`, `angarion.storages`;
  валидация `[accounts.*]` моделью плагина; неизвестный `messenger` →
  fail-fast с перечнем зарегистрированных.
- **FR-13** ДОЛЖНА: сверка конфигурации с `AdapterCapabilities`
  (§12.10) — управляемая деградация: невыполнимая подписка
  (edit/delete без поддержки, `thread_id` без threads) → fail-fast;
  `history_fetch=false` → catch-up off + предупреждение +
  `catchup_unavailable`.
- **FR-14** ДОЛЖНА: InMemory-реализации **всех** driven-портов M1
  (§12.4) + InMemory `Listener`, оформленные как `AdapterPlugin`,
  зарегистрированный entry point'ом в `pyproject.toml` самой библиотеки
  («нулевой пациент»).
- **FR-15** ДОЛЖНА: composition root (`bootstrap`) — сборка конвейера
  из конфига: загрузка плагинов → валидация → деградация → провязка
  ingest/worker/router. Без CLI (CLI — M3, см. C-5).

### Конфигурация и процессоры

- **FR-16** ДОЛЖНА: конфиг на `pydantic-settings` (§11): TOML + env,
  fail-fast валидация, ссылочная целостность (источники/цели ссылаются
  на существующие аккаунты), инвариант `dedup_ttl_days ≥
  registry_window_days`. Объём схемы M1 (C-4): `[accounts.*]` (через
  модели плагинов), `[storage]`, `[queue]` (`backend`-резолв по реестру
  + общие поля), `[pipelines.*]` полностью, `[catchup]` (нужен FR-13);
  секции `[api*]` добавляются в M5 аддитивно.
- **FR-17** ДОЛЖНА: встроенный процессор `passthrough` + декоратор
  `@processor("name")` + entry points `angarion.processors` (§10.1).
  `template` (и зависимость Jinja2) — отложен к M4 (C-3).
- **FR-18** НЕ ДОЛЖНА: тянуть инфраструктурные зависимости в ядро
  (§14.9): Telethon, SQLAlchemy, FastAPI, persist-queue в M1
  отсутствуют даже в extras.

## 4. Success Criteria

Acceptance §16 M1 дословно:

- **SC-1**: сквозной тест new/edited/deleted (+ обогащение
  previous_text / восстановление текста удалённого) + multicast —
  целиком на InMemory-адаптерах, зелёный.
- **SC-2**: fail-fast и деградация по матрице возможностей покрыты
  тестами (все четыре ветки §12.10).
- **SC-3**: InMemory-плагин загружается через entry point (тест через
  `importlib.metadata`, не прямым импортом).

Плюс качество (ТЗ §13–14 + проектный CLAUDE.md):

- **SC-4**: TDD (тест до кода), coverage ≥ 90%, `mypy --strict` на
  пакет, ruff чистый; 4 обязательные проверки перед каждым push.
- **SC-5**: контрактные тесты каждого порта проходят на InMemory.
- **SC-6**: дедуп-семантика §7.2 зафиксирована тестами на хелперы
  ключей (включая кейс «редактирование вернуло прежний текст = дубль»).

## 5. Key Entities

Полные определения — ТЗ §4 (DTO), §5 (порты), §12.10–12.11 (контракт
плагина). Спека их не дублирует. Структура пакета — §3.2 с поправкой
на `src/`-layout проекта: `src/angarion/{domain,application,adapters,...}`.

## 6. Assumptions & Constraints

- Python ≥ 3.12; обязательные runtime-зависимости ядра — pydantic v2,
  pydantic-settings и structlog: логирование (JSON, correlation id =
  `event.uid`) и маскирование секретов §17.7 закладываются сразу в M1,
  с покрывающим тестом (C-6).
- Процесс (C-5): T002 = **один PR** на ветке `T002-m1-core`; фазы
  реализации — отдельные коммиты на ветке, перед merge — squash в один
  коммит (правило «один PR — один коммит» соблюдается на merge).
- Релиз после M1 не публикуется: `CHANGELOG.md [Unreleased]` копит
  M1 + M2, версия на PyPI — после M3 (C-7).
- Ядро asyncio; тесты — pytest-asyncio.
- `EventKind` закрыт (три вида событий — домен, §15.20); `Messenger`
  открыт.
- Время — строго UTC, `AwareDatetime` (§17.4).
- Очередь M1 — in-memory: гарантия at-least-once **в пределах процесса**;
  «переживание рестарта» — свойство M2, в M1 проверяется только
  контрактом (`recover()` есть и работает для InMemory тривиально).

## 7. Out of Scope (T002 / M1)

- Персистентность: persist-queue, SQLAlchemy, Alembic, DLQ-таблица,
  `angarion.testing` как публичный пакет — **M2** (T003).
- Telegram-адаптер, реализация catch-up §9.3, CLI (`angarion run` /
  `migrate`) — **M3** (T005). В M1 от catch-up — только контракт
  (`Listener.catchup()`, `CursorStorePort`, поле `origin`).
- LLM-процессор и процессор `template` (с Jinja2) — **M4** (T007; C-3).
- Web API / UI / auth / динамические настройки / outbox-механика —
  **M5** (T008), **включая порты** `RuntimeConfigPort` и
  `CommandOutboxPort` с их DTO (C-1).
- Публикация версии на PyPI по итогам M1 — не делается, релиз после
  M3 (C-7).
- Медиа, второй мессенджер — **M7** (T010).
- Параллелизм worker'ов — санкционированная эволюция (§17.8), не M1.
- Фоновые prune/TTL-задачи — **M3** (с появлением долгоживущего
  рантайма/CLI); в M1 — только методы `prune()` портов с тестами (A-7).
- Поле `AdapterPlugin.webhook_router` и ветка деградации
  `push_transport="webhook"` — **M5** (требуют HTTP-адаптера; A-1).

---

## Clarify (заполняется Claude)

### Open questions

- (нет — clarify закрыт 2026-06-11)

### Resolved (с ответами, 2026-06-11)

- **C-9. Окно потери исходящих (порядок «mark до send» §6.3)** →
  **transactional outbox для исходящих** (решение Владимира
  2026-06-11; ТЗ не догма). Дословный §6.3 (`mark_delivered` до
  `send`) при сбое отправки терял помеченное сообщение. Новая схема:
  обработка и доставка разделены — `PipelineWorker` фиксирует
  outbound в `OutboxPort` (insert-if-absent по `idempotency_key`)
  строго до `ack`; `DeliveryWorker` асинхронно доставляет
  pending-записи с собственным retry/backoff; сбой отправки больше
  не теряет сообщение. `mark_delivered` удалён из `DedupStorePort`
  (порт без потребителя — мёртвый контракт, принцип C-1); выходная
  идемпотентность — первичный ключ outbox; §7.4 для исходящих
  реализуется таблицей outbox в M2. Остаточное окно «send выполнен,
  mark_sent не записан» при падении процесса даёт дубль, не потерю —
  допустимо at-least-once §7.1 (exactly-once без поддержки платформы
  недостижим). Бонусы: ретрай доставки не перезапускает процессор
  (значимо для LLM-процессоров M4), ретраи per-message, готовая
  точка персистентности (M2) и мониторинга/requeue (M5). Отвергнуты:
  (1) read-only `is_delivered` + «send → mark» — чинит потерю, но
  оставляет ре-выполнение процессора при ретраях доставки;
  (2) acquire/confirm с lease — нужен только при параллельных
  worker'ах (§17.8, не M1); (4) только tenacity в sender (M3) —
  mitigation, не fix. Вшито в FR-1, FR-5, FR-9, FR-10; ADR — в
  `DECISIONS.md` в задачном PR.

- **C-8. Экспоненциальная задержка retry** → **(б) `not_before` в
  envelope + defer-to-tail**. Retry: `put(attempt+1,
  not_before=now+backoff)` строго до `ack` исходного — порядок
  крэш-безопасен (падение между ними даёт дубль, не потерю; гасится
  `mark_delivered`). Worker возвращает «ранние» envelope в хвост
  (`put` + `ack`) с коротким sleep (cap ~1 с). Зеркально семантике
  паузы §12.8 — в M5 пауза ляжет на тот же механизм; порт не меняется,
  рестарт с персистентной очередью M2 переживается. Отвергнуты:
  (а) in-process таймер — окно потери retry между `ack` и `put` ломает
  at-least-once в M2; (в) `delay`-параметр в порте — усложняет контракт
  каждого адаптера очереди (persistqueue нативной задержки не имеет)
  ради того же результата. Вшито в FR-1, FR-10; ADR — в `DECISIONS.md`
  при реализации.

- **C-1. Объём портов в M1** → **(б)**: только порты, нужные конвейеру
  (queue, sink, dedup, registry, cursors, state, analytics, processor,
  Listener, DeadLetter). `RuntimeConfigPort` / `CommandOutboxPort` и их
  DTO — в M5, аддитивно. Вшито в FR-1, FR-5, Out of Scope.
- **C-2. DLQ в M1** → узкий `DeadLetterPort` (put/list/take) + InMemory
  сейчас; персистентная реализация — M2. Вшито в FR-5, FR-10.
- **C-3. `template` и Jinja2** → отложен к M4; в M1 только
  `passthrough`. Вшито в FR-17, Out of Scope.
- **C-4. Объём схемы конфига** → `[accounts.*]`, `[storage]`,
  `[queue]`, `[pipelines.*]`, `[catchup]`; `[api*]` — M5. Вшито в FR-16.
- **C-5. Нарезка PR** → **(а), решение Владимира**: один PR T002 на все
  фазы; фазы — коммиты на ветке, squash перед merge. Вшито в
  Assumptions.
- **C-6. structlog** → вводится сразу в M1, включая маскирование
  секретов §17.7 с тестом. Вшито в Assumptions, FR-1 (ProcessorServices).
- **C-7. Релиз после M1** → не публикуем; `[Unreleased]` копит до M3.
  Вшито в Assumptions, Out of Scope.

---

## Analyze (заполняется Claude)

Проход 2026-06-11 (spec v. clarified против ТЗ v2.1 целиком).

- 🔴 **A-1. `AdapterPlugin.webhook_router: APIRouter | None` тянет
  FastAPI в ядро.** §12.11 типизирует поле классом FastAPI, что
  противоречит FR-18 / §14.9 («ядро без инфраструктурных зависимостей»;
  FastAPI появляется только в M5 как extra). Предложение: в M1 контракт
  `AdapterPlugin` — **без поля `webhook_router`**; добавится в M5
  аддитивно (минорная версия, в духе §17.8). Деградационная ветка
  §12.10 «push_transport=webhook → роут в HTTP-адаптере» в M1
  соответственно не реализуется (HTTP-адаптера нет).
  **Resolved 2026-06-11: подтверждено Владимиром** — поле и
  webhook-ветка откладываются до M5; ADR — в `DECISIONS.md` в задачном
  PR.
- 🟡 **A-2. `AdapterPlugin` не удовлетворяет §4 «сериализуемы в JSON».**
  Поля-callables (`make_listener`, `make_sender`) и `type[BaseModel]`
  несериализуемы. Это внутреннее противоречие ТЗ (§4 говорит «все
  модели», §12.11 определяет AdapterPlugin как BaseModel). Фикс принят
  в спеку: требования §4 (frozen, extra=forbid, JSON) относятся к
  **DTO данных**; `AdapterPlugin`, `ProcessorServices`,
  `PipelineContextData`+services — конструкции композиции, JSON-контракт
  на них не распространяется (frozen — сохраняем где применимо).
- 🟡 **A-3. `RegistryDelta`: §5 перечисляет `is_new | text_changed |
  unchanged`, §6.1 упоминает четвёртый исход `stale`** (staleness-guard).
  Фикс принят в спеку (FR-1): `RegistryDelta` в M1 включает `stale`.
- 🟡 **A-4. Правила нормализации `normalize_and_hash()` в ТЗ не
  определены**, хотя объявлены публичным контрактом, меняемым только
  мажорной версией (§7.2) — т.е. первое же решение фиксируется надолго.
  Решить в plan.md. Предложение: минимальная нормализация — Unicode NFC
  + `\r\n`/`\r` → `\n`, **без** trim/lower (агрессивная нормализация
  склеивала бы содержательно разные версии); sha256 hex по UTF-8.
- 🟢 **A-5. Параметры экспоненциального backoff не заданы ТЗ** (только
  `max_retries=5`). Решить в plan: предложение — `base=1 с`,
  множитель 2, cap 60 с; конфигурируемо в `[queue]` или `[worker]`.
- 🟢 **A-6. Формы `QueueDepth`, `DeliveryReceipt`, `QueueItem.receipt`
  ТЗ не специфицирует** — определяются в plan (минимально достаточные:
  depth = pending/unacked, receipt — непрозрачный для ядра).
- 🟢 **A-7. Фоновые prune/TTL-задачи (§7.4, §17.3) не привязаны к
  этапу.** В M1 методы `prune()` портов реализуются и тестируются,
  фоновый запуск — M3 (вместе с рантаймом/CLI, где появляется
  долгоживущий процесс). Зафиксировано в Out of Scope.
- 🟢 **A-8. `not_before` — расширение определения envelope §5** (C-8).
  Аддитивно, в духе санкционированной эволюции §17.8; решение
  оформляется ADR в `DECISIONS.md` в задачном PR.
- 🟢 **A-9. `ProcessorServices.make_idempotency_key` без параметра
  `pipeline`** при том, что ключ §7.3 включает имя пайплайна — не
  ошибка: worker частично применяет фабрику, зафиксировав свой
  пайплайн. Фиксирую как требование к реализации (plan).
