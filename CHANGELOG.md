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
