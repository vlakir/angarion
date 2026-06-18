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

<!-- Здесь накапливаются изменения для следующих milestone (M6, M7). -->

### Added

- Пример Matrix-пайплайна (T010, M7 фаза B5): `examples/matrix/` — зеркало
  сообщений (текст + вложения, `passthrough`) между двумя Matrix-комнатами,
  с парольным `angarion login` и поддержкой E2EE-источника; аналог
  `examples/forward` на втором адаптере. README дополнен: roadmap M7 ✅,
  раздел «Ограничения платформ» (§17.9: UTD E2EE-истории, профиль Matrix),
  бэкап E2EE key-store (§17.6).
- Интеграционный контур Matrix (T010, M7 фаза B4): весь Matrix-конвейер
  (приём → пайплайн → отправка) на **живом локальном homeserver'е** —
  new/edited/deleted сквозь пайплайн, доставка из **E2EE-комнаты**, транзит
  медиа (`passthrough`). Маркер `integration` (default-skip); стенд —
  `tests/integration/matrix/` (docker-compose с Synapse либо Synapse из
  PyPI). Драйв источника тем же устройством, что и listener (для E2EE
  снимает кросс-девайсный обмен ключами — проверяется наш decrypt→map→
  deliver). Прогон зелёный против Synapse 1.155 (3 passed). ADR 2026-06-18.
- Matrix sender + catch-up (T010, M7 фаза B3): замкнута отправка и дозабор
  второго адаптера. `MatrixSender` (`MessageSinkPort`) доставляет
  `OutboundMessage` в комнату/тред (E2EE автоматически), переотправляет
  медиа по `mxc`-ссылке или заливкой скачанного файла (кросс-аккаунт/кросс-
  платформа), деградирует медиа→текст при недоступности и переживает
  rate-limit (`M_LIMIT_EXCEEDED`) / transient-сбои ретраями. Catch-up по
  `/messages` (`MatrixListener.catchup` + прогон на старте): Matrix-история
  несёт **явные** события (правка — отдельное `m.replace`, удаление —
  redaction), поэтому new/edited/deleted мапятся напрямую — без сверки
  хэшей/absence-детекции Telegram; дедуп гасит пересечения. Медиа-enrich
  (скачивание `mxc` принимающим аккаунтом по политике `[media]`) даёт
  процессорам `local_path` и кросс-платформенную пересылку. Listener и
  sender делят один пул nio-клиентов (мемо в `deps.shared`). Деградация по
  матрице возможностей §12.10 — generic-механизм (Matrix полнопрофилен,
  не деградирует). ADR 2026-06-18.
- Matrix listener + E2EE-приём (T010, M7 фаза B2): наполнен приём второго
  боевого адаптера. `MatrixListener` поверх границы `MatrixClientPort`
  (matrix-nio sync-loop) принимает live-события — new / edited (`m.replace`) /
  deleted (redaction) — и маппит в `InboundEvent` через те же публичные
  хелперы ключей, что и Telegram, включая `mxc`-вложения в `MediaRef`.
  **E2EE-комнаты** поддержаны (`matrix-nio[e2e]` → `python-olm`/`libolm`):
  key-store (olm/megolm) — отдельный sqlite в `[matrix].store_dir`
  (git-ignored), токен/`device_id` остаются в `app.db`. Нерасшифрованные
  события (UTD, нет ключа сессии) помечаются в аналитику и пропускаются —
  platform limitation Matrix E2EE, не падение (§17.9). Курсор — непрозрачный
  account-level `next_batch` sync (возобновление после простоя); глубокий
  catch-up по `/messages` и отправка — фаза B3. Маппинг покрыт юнит-тестами
  на nio-фикстурах (CI без живого homeserver). ADR 2026-06-18.
- Каркас Matrix-адаптера + парольный login (T010, M7 фаза B1): пакет
  `adapters/matrix/` (extra `angarion[matrix]`, entry point
  `angarion.adapters:matrix`) — матрица возможностей полного профиля
  (`edit_events`/`delete_events`/`history_fetch`/`threads`,
  `push_transport="client"`), схема `[accounts.*]` (`homeserver`/`user_id`/
  `device_name`; пароль — из env `ANGARION_MATRIX_PASSWORD`/`getpass`, не в
  TOML) и `angarion login --account <matrix>` (matrix-nio
  `AsyncClient.login` → `access_token`+`device_id` в зашифрованную сессию
  `app.db`). Базовый `matrix-nio` без E2EE; listener/sender/расшифровка —
  фазы B2/B3 (сейчас fail-fast-заглушки). ADR 2026-06-16.
- Шов логина в контракте плагина (T010, M7 фаза B1): `AdapterPlugin`
  получил аддитивное поле `make_login` — `angarion login` стал
  платформо-агностичным (резолвит плагин по `messenger` и делегирует ему
  обмен+персист сессии). Telegram перевёл логин в `make_login` без смены
  поведения; платформа без логина (InMemory) → внятный `ConfigError`.
  ADR 2026-06-16.
- Домен медиа (T010, M7 фаза A1): доменный DTO `MediaRef` (открытый
  строковый `kind`; платформенная ссылка `ref` для пересылки без
  скачивания; опц. mime/имя/размер/размерности/длительность; `local_path`)
  и аддитивные поля `InboundEvent.media` / `OutboundMessage.media`
  (default `[]`). Встроенный `passthrough` переносит вложения транзитом в
  каждый исходящий.
- Извлечение медиа из Telegram (T010, M7 фаза A2, inbound): Telethon
  `message.file` → структурный `MediaRef` (kind photo/video/voice/audio/
  sticker/animation/document + mime/имя/размер/размерности/длительность);
  гейт по `message.file` отсекает превью ссылок/опросы. `InboundEvent.media`
  теперь несёт реальные вложения; маппинг покрыт юнит-тестами на стабах.
- Пересылка медиа в Telegram (T010, M7 фаза A2, sender): `passthrough`
  доставляет медиа-only сообщения (пустая подпись), Telegram-sender
  переотправляет вложения **без скачивания** — рефетч исходного сообщения
  аккаунтом-отправителем по координатам `MediaRef.ref` (`"chat:msg"`) +
  `send_file` с подписью (FloodWait/transient-обёртка как у текста);
  источник недоступен/удалён → деградация до текста. Кросс-аккаунт со
  скачиванием — фаза A3. ADR 2026-06-16 (refetch-fast-path).

- Политика медиа + скачивание при ingest (T010, M7 фаза A3, срез 3):
  глобальная секция `[media]` (`download`/`allowed_kinds`/`max_size`/
  `storage_dir`/`retention_days`; по умолчанию не качаем — только
  метаданные). При `download=true` принимающий аккаунт скачивает подходящие
  вложения в git-ignored каталог и проставляет `MediaRef.local_path` (хук
  `enrich_with_downloads` в live и catch-up); процессоры получают локальный
  путь к контенту. Sender при наличии `local_path` грузит файл напрямую —
  включает **кросс-аккаунт/кросс-платформа** доставку (без `local_path` —
  refetch-fast-path A2). Ретеншн скачанных файлов — в фоновой prune-задаче
  (по mtime, §17.3). Новый метод порта `TelegramClientPort.download_media`.
  Per-pipeline переопределение пересылки медиа отложено (→ BACKLOG T033).
  ADR 2026-06-16.
- Media-хэш в реестре для catch-up (T010, M7 фаза A3, срез 2): `media_hash`
  в `RegistryRecord`/`RegistryVersion`/`InboundEvent` (+ миграция 0006:
  колонки `media_hash` в `messages`/`message_versions`). Детекция изменений
  в реестре и catch-up учитывает медиа (общий хелпер `content_unchanged`):
  подмена вложения за простой при том же тексте теперь поднимается как
  `MESSAGE_EDITED`. Контракт `MessageRegistryPort` дополнен тестом.
- Media-хэш в дедуп-ключе правок (T010, M7 фаза A3, срез 1): публичный
  хелпер `make_media_hash` (отпечаток опознающих метаданных вложений) +
  параметр `media_hash` в `make_dedup_key`. Правка медиа при том же тексте
  теперь = новая версия (Q5); **исправлен краш** медиа-only `MESSAGE_EDITED`
  (`text=None`) в `make_dedup_key` (для EDITED раньше был обязателен
  `content_hash`). Ключ EDITED для текста-без-медиа — байт-в-байт прежний
  (golden §7.2 не меняется). ADR 2026-06-16.

### Changed

- `InboundEvent.has_media` стал производным свойством от `media` (T010,
  M7 A2): доступ `event.has_media` сохранён, но это больше не входное поле
  (в JSON-дамп не входит — выводится из сериализуемого `media`). ADR
  2026-06-16 (`@property` вместо `@computed_field` — без pydantic-плагина
  mypy и `type: ignore`).

- Интеграционный тестовый контур §13.2 на реальном Telegram-аккаунте
  (T009, M6): маркер `integration` (default-skip), оснастка
  `tests/integration/harness.py` (реальный пайплайн через `build_app`
  на sqlite + persistqueue поверх одного test-owned Telethon-клиента) и
  сценарии `tests/integration/test_pipeline.py` — live new/edited/deleted,
  multicast в две цели, catch-up после простоя, дедуп при повторном
  catch-up, рестарт с непустой очередью, петлевой guard `source==target`.
  Третья тестовая группа (`TG_TEST_GROUP_C`) в `.secrets.example`.
- Петлевой guard `source == target` (T009, M6): `LoopGuardSink`
  (`application/loop_guard.py`) — декоратор `MessageSinkPort`, гасящий
  петлю собственных доставок dedup-пометкой произведённого сообщения;
  подключается в `build_app` при совпадении цели с источником. ADR
  2026-06-16 (ограничение по числовым `chat_id`).

### Changed

- Реконсиляция update-state на подключении Telethon-клиента (T030,
  всплыло в M6): `connect_client` после `connect()` делает `get_me()` +
  `catch_up()` — свежеподключённый `StringSession`-клиент принимает
  live-апдейты предсказуемее (особенно после простоя). Не аварийный фикс:
  основной прод-сценарий (зеркалирование входящих от других) работал и
  без этого; ненадёжным был лишь приём собственного исходящего в
  тест-драйве. ADR 2026-06-16.

## [0.3.0] — 2026-06-15

Закрытие milestone **M5** — Web-слой: Web API + Web UI (SSR, htmx) + Auth
(fastapi-users) + админ-операции/динамика + Viz (граф топологии) +
документация (§17.9). Версия минорная: §16 ТЗ предполагал 1.0.0 по
завершении M5, но формальные гарантии стабильности API (§12.11)
откладываются — остаёмся в pre-1.0 (решение Владимира 2026-06-15). T-ID
этапа: **T008** (умбрелла M5, вкл. бывш. T021), **T022–T025**, втянутый
**T006** (документация — закрыт в T025).

### Added
- Viz + Docs (**T025**/M5, §12.6/§17.9, группа D — последняя группа M5).
  **Граф топологии** `/ui/pipelines` (фаза 1): трёхдольный граф
  «источники → пайплайны → получатели», серверный SVG (Jinja, без
  JS-библиотек); цвет узла = статус (активен / пауза из `runtime_config` /
  failed за час), аннотации delivered/depth; клик по узлу (admin) → htmx
  pause/resume. **Документация** (фаза 2, втянутый **T006** — закрывается
  этой задачей): каркас mkdocs-material + автосправочник публичного API
  (mkdocstrings, статический анализ исходников — extra при сборке доков не
  нужны), быстрый старт и три гайда — деплой (роли процессов, systemd,
  reverse proxy + TLS, бэкапы, секреты через env), автору плагина
  (свой процессор/адаптер через entry points + сертификация контрактами
  `angarion.testing`), Web API и UI (своя ручка/страница через DI поверх
  портов). Группа `docs` в `[dependency-groups]`; `uv run mkdocs build
  --strict` зелёный. **Пример** `examples/web/` (фаза 3): combined-процесс
  (конвейер + Web в одном) с кастомной JSON-ручкой `/api/v1/ext/stats` и
  UI-страницей `/ui/ext` поверх портов (`AnalyticsDep`, `Page`,
  собственный composition root `build_app` + `build_web_deps` +
  `create_app(routers=…, pages=…)`). Всплыл gap: чистый `angarion run
  --with-api` не подгружает пользовательские страницы/ручки (нет seam'а) —
  запаркован в BACKLOG **T029**.
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
- Web UI переведён на английский (M5, в составе **T025**): весь видимый
  текст веб-морды (SSR-шаблоны, легенда/aria-label SVG-графа, метка
  источника динамики, ошибка входа) — English. Докстринги, лог-события и
  стартовые `ConfigError` остаются на русском (не часть веб-морды).
- Контракт `ProcessorPort` получил хук `config_model` (бывш. **T021**, в
  составе **T022**/M5, по образцу `account_config_model` адаптера §12.11):
  `processor_config` каждого пайплайна валидируется на старте
  (`build_app`), до приёма событий — невалидный конфиг падает
  `ConfigError` при сборке, а не на первом событии (fail-fast §12.7,
  FR-0). Встроенные `template`/`llm` объявляют схему; `passthrough` и
  функция-процессоры — без схемы (валидация пропускается). Синтаксис
  Jinja2-шаблонов/промптов по-прежнему компилируется лениво.

### Retrospective

- **Что зашло:** дробление M5 на 4 группы (T022–T025) со своими
  ветками/PR держало фокус; примеры ловили реальные проблемы запуска
  (всплыл порог Telegram-кредов → dev-helper `.secrets` + засев сессии),
  а gap раннера (нет seam'а для user-страниц в `--with-api`) вскрыт
  примером и запаркован T029, а не пофикшен «заодно».
- **Что не зашло:** scope растягивался от фидбэка в ходе T025 (dev-запуск
  примеров, i18n веб-морды) — полезно, но формально вне спеки группы D;
  впредь такие вещи — отдельной задачей. CodeRabbit нашёл реальный баг
  soft-fail засева сессии — стоило самому продумать `set -e`-путь.
- **Правки методики:** для user-facing локализации (язык UI) — явный
  пункт в спеке web-фич; правило «примеры закрывают долг по запуску»
  подтвердилось, оставляем.

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
