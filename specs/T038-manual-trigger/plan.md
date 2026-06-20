# Plan: T038 — Ручной запуск пайплайна

**Спека:** `spec.md` (статус Clarified). Этот план — техническая раскладка
на фазы (фаза = сессия = коммит, `/clear` между) + черновик ADR.

## Архитектурная карта (grounded по коду)

- **Программный путь.** `AngarionApp.ingest: IngestService` существует
  (`bootstrap.py:534`). `IngestService.ingest(record)` принимает готовый `Record`
  с хранимыми `uid`/`dedup_key`/`external_id`. Нужна публичная **фабрика** payload
  → `Record` (ключи через `domain/keys.make_source_key`/`make_dedup_key`) и
  публичные методы на `AngarionApp`.
- **`Record.origin`** = `Literal['live','catchup','internal']` (`models.py:139`) →
  расширяем `'manual'` (аддитивно, как T037 добавлял `internal`).
- **Семантика «event»** — через `ingest.ingest` → `router.resolve` → dedup/реестр/
  fan-out. **Семантика «pipeline»** — сырой `QueueEnvelope(pipeline, record)` прямо
  в `queue.put`, минуя router/dedup/реестр (Q7).
- **Идемпотентность.** Клиентский `idempotency_key` → `external_id` записи →
  детерминированный `dedup_key` (ретрай гасится `dedup.seen()`); без ключа —
  свежий `uid` → `external_id=str(uid)` → каждый вызов уникален.
- **Веб-продьюсер команд** (`http/ops.py`): `request_restart`/`request_catchup`
  кладут `command_outbox.put(CommandKind.X, payload=…)`. Добавляем `request_inject`.
- **Consumer** (`application/outbox_consumer.py::_execute`): диспетчеризация по
  `CommandKind`. Сейчас инжектит `sink`/`analytics`/`catchup`/`request_restart` —
  для моста добавляем `ingest: IngestService` (и доступ к `queue`) в конструктор
  `OutboxConsumer` + проводку в `bootstrap`.
- **combined vs split.** `serve_combined` строит `AngarionDeps` из готового
  `AngarionApp` (есть `ingest`/`queue`); `serve_api`/`build_web_deps` — из
  storage+queue (нет `ingest`). Route диспетчеризует: `deps.ingest is not None` →
  прямой вызов (combined); иначе → `command_outbox.put` (split).
- **Auth.** Машинная HTTP-ручка — новый **API-ключ** (FastAPI-зависимость, читает
  секрет из конфига, проверяет заголовок). UI-форма — существующая admin-сессия
  (`AdminUser` из fastapi-users). Op-функция общая, две точки входа с разной
  авторизацией.

## Фазы

### Фаза 1 — Программный API (фундамент)
- `domain` / `application`: публичная фабрика `build_manual_record(...)` (payload →
  `Record`, `origin='manual'`, ключи, idempotency). Упрощённый payload-DTO
  (`ManualEvent`?) + приём готового `Record`.
- `AngarionApp`: публичные методы `submit_event(payload|record)` (event-семантика
  через `ingest`) и `run_pipeline(name, payload|record)` (pipeline-семантика, сырой
  `queue.put`); валидация имени пайплайна по конфигу.
- `Record.origin` += `'manual'`.
- Экспорт фабрики/DTO из публичного `angarion`.
- TDD: фабрика (ключи/idempotency/origin), оба метода `AngarionApp`, неизвестный
  пайплайн → ошибка.

### Фаза 2 — Мост CommandOutbox (split, только event-путь — A1)
- `CommandKind` += `INJECT` (event-семантика). **Pipeline-путь команды НЕ требует**
  (A1): сырой `queue.put` работает и в split — api-процесс уже пишет в общий
  `queue.db` (паттерн `requeue_dead_letter`).
- `OutboxConsumer`: ветка `INJECT` → `ingest.ingest(record)`; конструктор получает
  `ingest: IngestService`, проводка в `bootstrap`.
- `http/ops.py`: `request_inject(command_outbox, payload, …)` — продьюсер команды
  (только для split event-пути).
- TDD: consumer-ветка `INJECT`, сериализация `Record`-payload в команде,
  идемпотентность через клиентский ключ на стыке.

### Фаза 3 — HTTP write-ручка + API-ключ
- Новый API-ключ: конфиг (`[api] trigger_token`?), FastAPI-зависимость
  `ApiKeyDep` (заголовок, сравнение секрета; пусто/выкл → `503`/`403`).
- Роут(ы) `POST /api/v1/trigger` (event) и `POST /api/v1/run/{pipeline}`
  (pipeline). **Event**: combined → прямой `ingest`; split → `request_inject`
  (по `deps.ingest is None`). **Pipeline**: `queue.put` в обоих режимах (A1).
  Pydantic-схемы payload (упрощённый + полный `Record`).
- `AngarionDeps` += `ingest: Any = None` (combined несёт, api — `None`).
- TDD: auth (нет/неверный/верный ключ), combined прямой путь, split через outbox,
  валидация payload (`422`), неизвестный пайплайн.

### Фаза 4 — UI-affordance
- Форма/кнопка ручного триггера в `/ui` (htmx, под admin-сессией). Переиспользует
  op-функции фазы 3.
- TDD: рендер формы под admin, POST формы триггерит (combined/split), без сессии —
  редирект/403.

### Фаза 5 — Пример + документация + ADR
- `examples/trigger/` (user-facing фича — обязателен, договорённость 2026-06-13):
  программный впрыск + curl к HTTP-ручке.
- README (раздел ручного триггера), `docs/guides/web-api.md` (write-ручка, auth,
  split-поведение), `CHANGELOG` (Added + Changed/ломающее), `DECISIONS.md` (ADR).
- `.secrets.example` += API-ключ-плейсхолдер.

## Черновик ADR (2026-06-20) — Открытие write-пути ручного триггера

**Контекст.** §12.5 фиксировал веб-API как read-only (управляющие операции вне
v1). T024 ввёл исключение — admin-управление через `CommandOutbox` (restart/
catchup/notify). T038 расширяет write-поверхность ручным триггером пайплайна.

**Решение.**
1. **Программный API** — публичная фабрика `Record` + методы `AngarionApp`
   (`submit_event`/`run_pipeline`); ранее внутренний `ingest` обретает
   документированный публичный фасад.
2. **Веб-API** — новые write-ручки под **отдельным API-ключом** (не admin-сессия);
   combined зовёт `ingest`/`queue` напрямую, split — через `CommandOutbox` (новый
   `CommandKind`), сохраняя инвариант «api-процесс без конвейера».
3. **origin='manual'** — ручные события маркируются для аналитики/трассы.
4. **Идемпотентность** — опц. клиентский ключ → детерминированный `dedup_key`.
5. **Прямой запуск пайплайна** минует dedup/реестр/router (сырой stage) —
   осознанно «тестовый прогон конкретного пайплайна».

**Альтернативы.** (а) Только программный API без web — отвергнуто (оператору нужна
кнопка). (б) Единый путь «всё через router» без прямого запуска — отвергнуто
(Владимир выбрал оба). (в) Web-триггер всегда через `CommandOutbox` и в combined —
отвергнуто ради меньшей латентности прямого пути; combined зовёт `ingest` напрямую.
(г) Admin-сессия вместо API-ключа для машинной ручки — отвергнуто (службам/CI
сессия неудобна; ключ переиспользуем).

**Последствия.** Ломающе: `Record.origin` (+`manual`), `CommandKind` (+вид),
сериализация команды/очереди (аддитивно, pre-alpha — без миграции). Новый секрет
(API-ключ) — гигиена репозитория.
