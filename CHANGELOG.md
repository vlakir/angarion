# Changelog

История заметных изменений. Формат — упрощённый
[Keep a Changelog](https://keepachangelog.com/).

Записи группируются по версиям или датам релизов. Для проектов без
формального версионирования допустимо использовать дату как заголовок.

Категории:
- **Added** — новая функциональность.
- **Changed** — изменения в существующей функциональности.
- **Fixed** — исправления багов.
- **Removed** — удалённая функциональность.
- **Deprecated** — то, что помечено к удалению, но пока работает.
- **Security** — изменения, важные с точки зрения безопасности.

Если изменение связано с задачей из `BOARD.md` / `BACKLOG.md`,
запись **обязательно** содержит T-ID в скобках, например:
`Added: Превью постов в Telegram (T<NNN>).` Это сохраняет уникальность
T-ID между релизами — `CHANGELOG.md` единственное persistent-
хранилище номеров завершённых задач (см. правило нумерации
в `README.md`).

---

## [Unreleased]

<!-- Здесь накапливаются изменения для следующего milestone (M5). -->

### Added
- Ops — админ-операции + consumer outbox + роли процессов (**T024**/M5,
  §12.8/§12.9, фаза 4): завершение группы C. **Админ-операции** (admin-
  only, `/api/v1/admin/*` + SSR `/ui/settings`, `/ui/dlq`; viewer → 403):
  пауза/возобновление пайплайна (через `RuntimeConfigPort`, worker уже
  откладывает паузнутое — пауза без потерь), сохранение/сброс динамики,
  requeue из DLQ с `attempt=0` (+ `requeued_at`), restart и ручной
  catch-up. Каждая операция и изменение динамики пишет аудит `admin_op`
  (пользователь, операция, старое/новое) в аналитику. **Командный
  outbox** (§12.9): `OutboxConsumer` (pipeline-процесс, фоновый поллинг
  `worker.outbox_poll_seconds`) захватывает команды и диспетчеризует —
  `notify` (через `MessageSinkPort`), `catchup` (по listener'ам),
  `restart_pipeline` (взвод graceful-остановки, супервизор поднимает);
  сбой → `mark_failed` + `notify_failed`/`command_failed`. **Уведомление
  о заявке** на регистрацию (`[api.notify]`) — неблокирующее: команда
  `notify` в outbox, сбой не ломает регистрацию. **Роли процессов**:
  `angarion run --role pipeline|api|combined` / `--with-api` (uvicorn-
  раннер в http-адаптере; ядро fastapi-free). Web composition root
  `build_web_deps`/`build_settings_notifier`; `AngarionDeps` пополнен
  `runtime_config`/`command_outbox`/`dead_letters`/`notifier`. Два писателя
  `app.db` в раздельном режиме — `PRAGMA busy_timeout=5000` (ADR §3.1,
  2026-06-15); per-write ретрай — BACKLOG T028. Reaper зависших `taken` —
  BACKLOG T027.
- Ops — применение динамики на лету + пауза-без-потерь (**T024**/M5,
  §12.8, фаза 3): `PipelineWorker` читает `RuntimeConfigPort` в начале
  итерации — пайплайн из `paused_pipelines` откладывается defer-to-tail
  (`put` в хвост строго до `ack` + событие аналитики `deferred` +
  короткий sleep против горячего цикла); ingest продолжает копить, resume
  обрабатывает накопленное без потерь и дублей. `TelegramSender`
  применяет динамические `sender_chat_per_second` /
  `sender_account_per_minute` поверх файловых порогов (override → reset
  возвращает файл) через новый `TokenBucket.reconfigure`. Уровень лога —
  по in-process событию: `SettingsNotifier` (pub/sub) + подписчик
  `apply_log_level_on_change` (переключает `wrapper_class` structlog);
  producer события (после `save`) — в фазе 4. ADR 2026-06-15.
  Админ-операции, consumer outbox, роли процессов и `/ui/settings` —
  фаза 4.
- Ops — `CommandOutboxPort` (командный outbox api→pipeline) (**T024**/M5,
  §12.9, фаза 2): DTO `OutboxCommand` + enum'ы `CommandKind`
  (`notify`/`catchup`/`restart_pipeline`) и `CommandStatus`
  (`pending`/`taken`/`done`/`failed`); порт `put` (producer) /
  `take` (consumer, атомарный захват `pending` → `taken` через
  `UPDATE ... WHERE status='pending'` + rowcount, at-least-once) /
  `mark_done` / `mark_failed` / `get` / `prune`. Реализации —
  `OutboxCommandRow` (миграция Alembic `0005`, индекс по `status`) на
  SQLAlchemy (`RETURNING` для захвата) + InMemory; добавлены в
  `StorageBundle`. Контрактный набор `CommandOutboxContract`
  (FIFO-захват с лимитом, конкурентный захват без дублей, терминальные
  пометки, no-op вне `taken`, prune терминальных) — на обоих бэкендах.
  Producer/consumer-поллинг, виды команд и роли процессов — фаза 4.
- Ops — `RuntimeConfigPort` (динамические настройки) (**T024**/M5, §12.8,
  фаза 1): DTO `DynamicSettings` (sparse-override поверх файла: `None` —
  действует значение TOML+env, не-`None` — БД-override) + порт
  `load`/`save`/`reset`. Реализации — `AppSettingRow` (key-value JSON,
  миграция Alembic `0004`) на SQLAlchemy + InMemory; добавлены в
  `StorageBundle`. Контрактный набор `RuntimeConfigContract` (приоритет
  override над файлом, частичное применение `save`, сброс) прогоняется
  на обоих бэкендах. Применение на лету (worker/sender/log level) и
  `/ui/settings` — последующие фазы T024.
- Auth — регистрация-одобрение и админка пользователей (**T023**/M5,
  §12.7, фаза 3): завершение группы B. Cookie-вход для UI — страницы
  `/ui/login` (форма → JWT в HTTPOnly-cookie), `/ui/logout`,
  `/ui/register` (заявка + сообщение «ждите одобрения»). Лимит
  `max_pending_registrations` (429 при достижении). Управление
  пользователями только для `admin`: JSON-ручки `GET/POST /api/v1/users`,
  `PATCH /api/v1/users/{id}` (одобрить/деактивировать/сменить роль,
  аудит `approved_by`), `DELETE /api/v1/users/{id}` — единственное write-
  исключение из read-only встроенных ручек. SSR-страница `/ui/users`
  (таблица + htmx-действия approve/deactivate/delete/create). Навигация
  показывает `Users`/`Logout` по текущему пользователю (через
  `request.state.user`). Cookie-вход и одобрение — ADR 2026-06-14.
- Auth — fastapi-users core (**T023**/M5, §12.7, фаза 2): аутентификация
  и авторизация на уровне роутеров. Логин по JWT (`POST
  /api/v1/auth/login`, Bearer) и саморегистрация-заявка (`POST
  /api/v1/auth/register` — `is_active=False`, `role="viewer"`, можно
  отключить `registration_enabled=false`) поверх user store fastapi-users
  (`UserRow`, хэш argon2/pwdlib). Публичный API — зависимости
  `CurrentUser`/`AdminUser` (`angarion.adapters.http.auth`): встроенные
  diagnostics/events и `/ui` закрыты `CurrentUser`, write — `AdminUser`
  (роль `admin`); публичны `GET /health`, `/api/v1/auth/*`, статика и
  webhook-роутеры адаптеров. Пользовательские роутеры/страницы по
  умолчанию закрыты (`Page(public=True)` открывает). Два транспорта над
  одним store: Bearer для `/api/v1`, HTTPOnly-cookie для `/ui`. Режим
  `auth="none"` (dev/локально) — синтетический локальный админ; при bind
  не на localhost — громкое предупреждение. Bootstrap первого админа из
  env (`ANGARION_ADMIN_LOGIN`/`PASSWORD`) на пустой БД + fail-fast по
  секрету/admin-env (§12.7, FR-0). ASGI-матрица «аноним/pending/viewer/
  admin». Решения (свой register, ручная `require_user`, мост `UserRow` ↔
  протокол fastapi-users) — ADR 2026-06-14. Cookie-вход `/ui/login`,
  одобрение/лимит заявок и `/ui/users` — фаза 3.
- Auth — identity data layer (**T023**/M5, §12.7, фаза 1): фундамент
  аутентификации на fastapi-users. ORM-сущность `UserRow`
  (`adapters/storage/orm.py`) + миграция Alembic `0003` — `id` (UUID),
  уникальный `login`, `hashed_password` (argon2), `role`
  (`admin`/`viewer`), `is_active` (одобрение), `registered_at`/
  `approved_at`/`approved_by` (аудит). Секция конфига `[api]`
  (`ApiConfig`): `host`/`port`, `auth` (`users`/`none`), `jwt_lifetime`,
  `cookie_secure`, `registration_enabled`, `max_pending_registrations`;
  `api.secret` и bootstrap admin-credentials — из env (`ANGARION_API__SECRET`,
  `ANGARION_ADMIN_LOGIN`/`ANGARION_ADMIN_PASSWORD`, секреты не в TOML).
  Зависимость `fastapi-users[sqlalchemy]` добавлена в extra `web`. Схема
  `UserRow` и плоская `[api]` — ADR 2026-06-14. fastapi-users core
  (backends JWT+cookie, роутеры, `CurrentUser`/`AdminUser`, bootstrap,
  fail-fast) — фаза 2.
- Web UI core (**T022**/M5, §12.6, фаза 3): SSR-дашборд на Jinja2 + htmx
  + Pico.css, **без Node и сборки**. Страницы `GET /ui` (дашборд:
  глубина очереди, события за 24 ч по видам, курсоры источников, таблица
  пайплайнов — блоки автообновляются htmx-поллингом), `GET /ui/events`
  (журнал аналитики с фильтрами `kind`/`pipeline`), `GET /ui/fragments/*`
  (партиалы для поллинга). Ассеты `pico.min.css` (2.1.1) и `htmx.min.js`
  (2.0.10) упакованы и отдаются `StaticFiles` по `/ui/static/*` —
  офлайн-режим, без CDN (провенанс/лицензии в `static/README.md`).
  Контракт расширения: базовый layout `angarion/base.html`,
  `ChoiceLoader` для пользовательских шаблонов, дескриптор
  `Page(title, path, router)` + `create_app(deps, pages=[Page(...)])` —
  страница монтируется и автоматически появляется в навигации
  (ADR 2026-06-14). `/ui/pipelines` и auth — последующие фазы M5.
- Web API core (**T022**/M5, §12.5, фаза 2): HTTP — второй
  driving-адаптер, симметричный Telethon-listener'у. Пакет
  `angarion.adapters.http` (extra `web`, FastAPI вне ядра §14.9):
  фабрика `create_app(deps, *, routers, title)` кладёт контейнер портов
  `AngarionDeps` в `app.state`; встроенный read-only роутер `/api/v1`
  (`GET /health` без портов, `GET /diagnostics` — глубина очереди,
  события за 24 ч по видам, курсоры источников, список пайплайнов,
  uptime; `GET /events` с фильтрами `kind`/`pipeline`/`limit`).
  Опубликованы типизированные DI-зависимости поверх портов
  (`AnalyticsDep`, `RegistryDep`, `StateDep`, `QueueDep`, `CursorsDep`)
  как публичный API для пользовательских ручек. ASGI-тесты на
  InMemory-портах (`httpx.ASGITransport`), без сети/БД/Telegram.
- Поле `webhook_router` в контракт `AdapterPlugin` (§12.11): роутеры
  адаптеров `push_transport="webhook"` монтируются `create_app` из
  `deps.webhook_routers` (закрыт долг M3). Типизировано `Any`, чтобы
  ядро осталось fastapi-free (§14.9, ADR 2026-06-14).

### Changed
- Контракт `ProcessorPort` получил хук `config_model` (бывш. **T021**, в
  составе **T022**/M5, по образцу `account_config_model` адаптера §12.11):
  `processor_config` каждого пайплайна валидируется на старте
  (`build_app`), до приёма событий — невалидный конфиг падает
  `ConfigError` при сборке, а не на первом событии (fail-fast §12.7,
  FR-0). Встроенные `template`/`llm` объявляют схему; `passthrough` и
  функция-процессоры — без схемы (валидация пропускается). Синтаксис
  Jinja2-шаблонов/промптов по-прежнему компилируется лениво.

## [0.2.0] — 2026-06-14

Первый функциональный релиз: ядро библиотеки и конвейер этапов M1–M4
(домен/порты/application → персистентность → Telegram-адаптер →
трансформирующие процессоры). Закрывает milestone **M4**. Версия
`[0.1.0]` была инфраструктурной (каркас пакета + резерв имени на PyPI);
сами этапы накапливались в `[Unreleased]` и выпускаются здесь одной
версией.

### Added
- Пример `examples/digest/` (T019, M4): stateful-процессор «дайджест» с
  LLM-саммари — образец того, как написать **свой** процессор с
  состоянием. Накапливает сообщения группы-источника в `svc.state`
  (Pydantic-модель под JSON-ключом, §10.3) и при сбросе (по числу `n`
  ИЛИ возрасту накопления `max_age_s`, проверка событийная — worker без
  фонового шедулера) прогоняет батч через локальную OpenAI-совместимую
  модель (Ollama) единой сводкой на каждую цель, переиспользуя тонкую
  границу `LlmHttpClientPort` встроенного `llm` (тест без сети).
  Идемпотентность под at-least-once: учтённые `dedup_key` в `seen`
  (батч) + ограниченном хвосте `recent` (сброшенные) — повтор события
  не задваивает накопитель и не шлёт дайджест дважды. Кастомный
  процессор регистрируется in-process тонким `run.py` (entry-point-путь
  для упакованного процессора показан в README); конфиг `app.toml` +
  идемпотентный `run.sh` + README (настройка Ollama, troubleshooting).
  Unit-тест дайджеста (`tests/unit/examples/`, вне `--cov=src`, гоняется
  в CI) покрывает накопление/сброс/идемпотентность. Сверён
  `examples/forward` на актуальность API (правок не потребовал).
- LLM-процессор и `template`-процессор, этап M4 (T007): первые
  **трансформирующие** встроенные процессоры. `llm` обрабатывает текст
  события через OpenAI-совместимый endpoint (`/v1/chat/completions`,
  `httpx`): Jinja2-промпты `system`/`user` (+ опц. пер-видовые `user`),
  ключ из env по имени `api_key_env` (не в TOML, §17.7), tenacity-ретраи
  на сети/`5xx`/`429` с уважением `Retry-After` (потолок `timeout_s`),
  `4xx≠429` без ретраев → `ProcessingError`; HTTP-вызов за тонкой
  границей (`LlmHttpClientPort`) — тестируется без сети. `template`
  детерминированно переписывает событие Jinja2-шаблоном по его полям
  (база + опц. `edited`/`deleted`), закрывая долг C-3 из M2. Оба — под
  entry points `angarion.processors`; `llm` — extra `angarion[llm]`
  (`httpx`+`tenacity`), `jinja2` — core (`template` доступен из коробки).
  Дайджест (§10.3) и user-facing примеры — отдельная задача T019.
- Пример `examples/forward/` (T018): пересылка новых сообщений из одной
  группы Telegram в другую процессором `passthrough` — конфиг `app.toml`,
  скрипт «всё в одном» `run.sh` (идемпотентные migrate/login/run, рантайм
  и ключ сессии в git-ignored `angarion-data/`) и README с настройкой и
  troubleshooting'ом авторизации (канал доставки кода / VPN). Первый
  воспроизводимый quick start на реальном аккаунте (§17.9, к M3).
- Telegram-адаптер, этап M3 (T005): первый боевой адаптер платформы
  (Telethon, MTProto, пользовательские аккаунты) через тот же
  plugin-контракт `AdapterPlugin`, что и InMemory (entry point
  `angarion.adapters:telegram`, extra `angarion[telegram]`). Live-listener
  (new/edited/deleted, мультиаккаунт, буфер), catch-up §9.3 (новые/правки/
  удаления, усечение `catchup_truncated`, периодический фоновый прогон),
  sender (token-bucket троттлинг per chat/account, FloodWait-повтор того же
  сообщения, transient-ретраи, `extra`: parse_mode/silent/disable_preview),
  резолв сущностей с прогревом `get_dialogs` и управляемой деградацией
  источника (`source_unavailable`), сессии аккаунтов как зашифрованный
  `StringSession` в `app.db` (`SessionStorePort`, Fernet, ключ из env
  `ANGARION_SESSION_KEY`). CLI `angarion run / migrate / login` с graceful
  shutdown (SIGINT/SIGTERM) и фоновой prune-задачей ретеншна §17.3. Границы
  Telethon скрыты за тонким Protocol-портом — mapping/catch-up/sender/CLI
  тестируются на фейках без сети (coverage ≥ 90%). Конфиг платформы —
  секции `[telegram]`/`[telegram.sender]` и `[catchup].interval` (ADR
  2026-06-13). Acceptance §16 M3: сквозной тест «простой → правки/удаления
  → рестарт» через настоящий composition root (`build_app` на sqlite +
  memory-queue, граница Telethon — на fake-клиенте) — catch-up на рестарте
  даёт корректные edited (с previous_text) / deleted (с восстановленным
  текстом) / new; гайд бэкапа §17.6 в README (сессии в `app.db`, ключ
  `ANGARION_SESSION_KEY` бэкапится отдельно).
- Ревизия исходников tgcf перед M3 (T004, §16 M3 / §15.18): конспект
  `specs/T004-tgcf-review/notes.md` по четырём темам (FloodWait/
  реконнекты, пагинация истории, резолв сущностей, session string) +
  ADR 2026-06-13 с решениями «брать/не брать». Итог: прямых
  заимствований кода нет; берём приёмы Telethon на уровне концепции
  (`iter_messages` reverse+offset_id для catch-up, резолв в стабильный
  числовой peer id, `StringSession`), фиксируем антипаттерны и вопросы
  к спеке M3 (T005).
- Персистентность, этап M2 (T003): публичный пакет `angarion.testing`
  — контрактные наборы тестов портов + фабрики DTO для сертификации
  сторонних адаптеров (§12.11; extra `angarion[testing]`,
  pytest11-плагин для assert-rewriting, ADR 2026-06-12 про S101);
  персистентная очередь на `persistqueue.SQLiteAckQueue` (`queue.db`,
  JSON-сериализация envelope, отменяемый поллинг, явный `recover()`;
  extra `angarion[persistqueue]`); storage-бэкенд SQLAlchemy 2.0
  async + aiosqlite (`app.db`, PRAGMA WAL/foreign_keys, время —
  TEXT ISO 8601 в UTC через TypeDecorator) с реализациями всех портов
  `StorageBundle` и Alembic-миграциями внутри пакета (программный
  `upgrade head` при сборке, `auto_migrate = false` — fail-fast при
  расхождении схемы; extra `angarion[sqlite]`); kill-тест §16 M2 —
  SIGKILL в детерминированных точках конвейера и в произвольные
  моменты: доставка без потерь, дубли только в остаточном окне C-9.
- Ядро библиотеки, этап M1 (T002): доменные DTO событий (§4) и
  публичные хелперы ключей дедупликации/идемпотентности
  (`domain/keys`, нормализация — ADR 2026-06-11); Protocol-порты
  ядра с контрактными наборами тестов; application-слой —
  `IngestService` (дедуп → реестр с обогащением → multicast-роутер →
  fan-out), `PipelineWorker` (retry с backoff через `not_before`,
  DLQ), transactional outbox исходящих + `DeliveryWorker`
  (ADR 2026-06-11); реестр процессоров (`@processor`, `passthrough`);
  плагинная система через entry points (`angarion.adapters` /
  `queues` / `storages` / `processors`) с InMemory-плагином — «нулевой
  пациент»; конфиг pydantic-settings (TOML + env) с двухступенчатой
  валидацией и деградацией по матрице возможностей §12.10;
  composition root `build_app()` → `AngarionApp` (start/stop);
  structlog-хелперы с маскированием секретов; сквозные
  acceptance-тесты M1 (new/edited/deleted + обогащение + multicast,
  ретраи до DLQ, дедуп повторной подачи).
- Интеграционный контур Telegram (T014, предтеча M6): реквизиты в
  git-ignored `.secrets` (образец `.secrets.example`), одноразовая
  авторизация `scripts/tg_login.py`, pytest-маркер `integration`
  (по умолчанию пропускается), первый интеграционный тест — доступ
  к аккаунту и двум тестовым супергруппам с send/delete-roundtrip.
  Временно на ключах TDesktop (ADR 2026-06-11; замена — T015).
- Осмысленный README (T013): описание библиотеки, ключевые идеи,
  статус pre-alpha, дорожная карта M1–M7, бейджи PyPI/CI, гайд
  разработчика.

### Fixed
- Крэш-окно ingest (T003, A-11): отметка дедупа писалась до постановки
  envelope в очередь — kill -9 между ними терял событие безвозвратно
  (ре-эмит после рестарта гасился как «дубль»). Теперь проверка
  `seen()` на входе, отметка `mark_inbound()` строго после fan-out:
  дубль вместо потери, сквозной at-least-once §7.1 восстановлен
  (аддитивный метод `DedupStorePort`; ADR 2026-06-12).

### Retrospective

- **Что зашло:** гексагональная архитектура окупилась — M3 (Telegram) и
  M4 (процессоры) легли на готовые порты M1 без переделок ядра; узкие
  границы (`Listener`, `LlmHttpClientPort`) дали юнит-тесты сетевого кода
  без сети. TDD + контрактные наборы `angarion.testing` и CI-матрица
  3.12–3.14 поймали реальные баги (дыра дедупа A-11, гонка
  `PersistQueue.close()` на 3.14, неатомарные миграции). Ритуал
  spec/clarify/analyze ловил слепые зоны до кода (отступления M3 по
  сессии оформлены ADR заранее). Примеры как часть фичи окупились дважды:
  на демо M3 всплыл реальный баг резолва по числовому id, а дайджест M4
  прогнан end-to-end на реальном аккаунте.
- **Что не зашло:** `[Unreleased]` копился через четыре этапа без
  промежуточных версий — M1–M4 выпущены «одним куском», ретро сразу на
  четыре milestone (потеря гранулярности). T017 (интерактивный логин с
  доставкой кода) так и не проверен вживую — обходили переиспользованием
  сессии, долг остаётся. `mypy --strict` над сторонними libs (Telethon)
  потребовал `SkipValidation`/обёрток; coverage против принципиально
  нетестируемого сетевого кода — постоянный компромисс.
- **Правки методики:** резать milestone-версии по факту завершения
  этапа, а не копить `[Unreleased]` на несколько milestone (иначе
  ретроспектива размывается). Кандидат в «definition of done» этапа —
  включать релиз-разрез CHANGELOG, чтобы `BOARD → Done` не накапливался
  на месяцы.

## [0.1.0] — 2026-06-11

### Added
- Резервирование имени на PyPI (T011): метаданные пакета — classifiers,
  keywords, `project.urls`; публикация каркасной версии 0.1.0.
- Инфраструктура проекта (T001): структура из шаблона dreamteam,
  LICENSE (MIT), каркас пакета `src/angarion/` (py.typed),
  `pyproject.toml` под требования ТЗ — Python ≥ 3.12, packaging
  hatchling, `mypy --strict`, coverage ≥ 90%, исключение `migrations/`
  из линтеров/mypy/coverage; `.gitignore` с защитой секретов
  (Telethon-сессии, локальные БД); CI (GitHub Actions, матрица
  Python 3.12–3.14); бэклог этапов M1–M7 из ТЗ (CONCEPT.md v2.1).

### Retrospective

- **Что зашло:** полноценное ТЗ (CONCEPT.md v2.1) до первой строчки
  кода — этап подготовки прошёл без единого clarify-вопроса; шаблон
  dreamteam: от пустой папки до опубликованного на PyPI пакета с
  защищённым main и зелёным CI за один день; настройка GitHub
  (branch protection, squash-policy) через `gh api` без ручного
  кликанья.
- **Что не зашло:** дефолты шаблона разошлись с требованиями ТЗ
  (coverage 80 vs 90, Python 3.14 vs 3.12, mypy без strict) —
  выравнивание руками; `.idea/` успел попасть в индекс до появления
  `.gitignore`; mypy не умеет `strict` в per-module overrides —
  включили глобально (строже плана, осознанно); опечатка автора
  e-mail протекла из `dreamteam init` в pyproject.
- **Правки методики:** кандидаты для шаблона dreamteam — параметризовать
  coverage-порог и `requires-python` в `dreamteam init`, ставить
  `hooks/pre-push` в `.git/hooks/` автоматически, валидировать e-mail
  в ответах init. Занести в бэклог dreamteam при ближайшей работе там.

<!-- При закрытии этой версии (переход к новой `## [N.M.0]`)
     добавляется секция:

### Retrospective

- **Что зашло:** ...
- **Что не зашло:** ...
- **Правки методики:** ...

Это короткий разбор результата milestone. Не обязательная длинная
форма — несколько строк по делу. -->


---

<!-- Пример (удалить при заполнении шаблона):

## [0.1.0] — 2026-05-13

### Added
- Базовая структура проекта по шаблону dreamteam.
- Веб-форма публикации с превью.

### Fixed
- Падение при пустом теле поста.

-->
