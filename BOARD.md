# Board

Лёгкая Kanban-альтернатива на одном markdown-файле: три колонки
(To Do / Doing / Done) под git, без внешних сервисов и инструментов.

## Соотношение с другими файлами

- `BACKLOG.md` — длинная очередь идей и побочных находок. Сюда падает
  «потом подумаем», «не сейчас». Парковка scope.
- `BOARD.md` (этот файл) — активный рабочий поток. Задачи, которые мы
  уже взяли или собираемся брать в ближайшее время.
- `specs/T<NNN>-*/spec.md` — куда вырастает крупная задача из BOARD, если
  она оказывается фичей >1 дня работы.

Жизненный цикл задачи: идея в `BACKLOG.md` → созрела → переезжает в
`To Do` здесь → берётся в работу (`Doing`) → закрывается (`Done`) →
после релиза переходит в `CHANGELOG.md` (запись обязательно содержит
T-ID), отсюда удаляется. **`CHANGELOG.md` — единственное persistent-
хранилище T-ID завершённых задач**, без него правило «ID не
переиспользуется» сломается.

## Формат задачи

Каждая задача — `- **T<NNN>** — <короткое описание>`. ID присваивается
при создании: новый = `max(существующих T-ID в BOARD.md, BACKLOG.md и
CHANGELOG.md) + 1`. ID никогда не переиспользуется. ID общий для
`BOARD.md` и `BACKLOG.md` — при перетекании задачи между ними
сохраняется; после релиза задача попадает в `CHANGELOG.md` с тем же
T-ID, что гарантирует уникальность номеров между релизами.

Имя ветки: `T<NNN>-<slug>` (без namespace типа `fixes/` / `feature/` —
ID уже даёт идентификацию). Имя PR: `T<NNN>: <title>`. Спецификация
крупной фичи: `specs/T<NNN>-<slug>/spec.md`.

По вкусу можно добавлять:

- метку даты взятия,
- ссылку на спеку,
- имя ветки.

Пример:

```
- **T<NNN>** — Превью постов в Telegram
  (`specs/T<NNN>-telegram-preview/`, ветка `T<NNN>-telegram-preview`).
```

---

## To Do

<!-- Готово к взятию. Очередь FIFO по умолчанию, можно поднимать
     приоритетное наверх. -->

<!-- Записи задач в формате `- **T<NNN>** — описание`. См. раздел
     «Формат задачи» выше. -->

## Doing

<!-- В работе прямо сейчас. Держим короткой: максимум 1-2 задачи на
     разработчика, иначе теряется фокус (классическое WIP-limit
     правило из Kanban). -->

## Done

<!-- Закрытые задачи, ждущие переноса в CHANGELOG.md при следующем
     релизе или значимой точке. После переноса — очищаем. -->

- **T042** — Сессия из env/конфига: авторизованная `StringSession` напрямую,
  без `login`/`seed` для dev-прогонов (ветка `T042-session-from-env`)
  [closed 2026-06-20, текущий PR]. `TelegramAccountConfig` получил
  опциональное поле `session` — если задано (обычно env
  `ANGARION_ACCOUNTS__<NAME>__SESSION`), `ClientRegistry` подключает клиента
  напрямую этой строкой через `env_sessions`, минуя `app.db` и
  `ANGARION_SESSION_KEY` (приоритетнее сохранённой). Боевой путь хранения в
  `app.db` не тронут (дефолт). ADR 2026-06-20 (безопасность: env-сессия —
  dev/CI-путь, plaintext учётные данные, не prod-дефолт). Примеры
  (`forward`/`media`/`digest`/`web`) подхватывают сессию из git-ignored
  `.secrets` (`scripts/example_dev.sh`: Telethon-файл → `StringSession` →
  env); `run.sh` ×4 и READMEs обновлены, CHANGELOG/README — тоже. Acceptance
  выполнен: живой прогон на реальном аккаунте — env-сессия подключается с
  **пустым** session-store и без ключа (resolve групп A/B), 5 интеграционных
  pipeline-тестов зелёные, буквальный `forward/run.sh` стартует без
  login/seed/ключа; Matrix-пример прогнан вживую на локальном Synapse
  (login → старт → зеркало SRC→DST подтверждено, 3 integration-теста зелёные);
  prod-путь `app.db` цел; `mypy --strict` чист, coverage 96.51%, 1116 тестов
  зелёные. По дороге фикс предсуществующего бага dev-хелпера: `example_dev.sh`
  мапил TG-креды на `main` любого транспорта (ломал Matrix-пример) — теперь
  гейтится `ANGARION_DEV_MAIN_TRANSPORT`.

- **T041** — Транспорт-агностичная модель ядра: `Record` + `transport` +
  capabilities адаптера (`specs/T041-record-transport-model/spec.md`)
  [closed 2026-06-20, текущий PR]. Фундамент волны v2: публичная модель и
  контракт обобщены с «события мессенджера» до транспорт-агностичной записи,
  мессенджер-семантика (new/edited/deleted, реестр, catch-up) → опциональные
  capabilities. Big-bang без алиасов (pre-alpha). Переименования:
  `InboundEvent`→`Record`, `OutboundMessage`→`OutboundRecord` (прежний
  outbox-журнал → `OutboxRecord`), `Address`→`Endpoint`
  (`messenger`→`transport`, `chat_id`→`address`), `EventKind`→`RecordKind`
  (`message_new/edited/deleted`→`new/edited/deleted`),
  `MessageSinkPort`→`SinkPort`; `angarion.testing` и ключи конфига TOML —
  тоже ломающе (см. CHANGELOG). Сохранены `MessageRegistryPort`/`Contract`
  (реестр = capability «мессенджер»), `EventQueuePort`, `AnalyticsEvent`.
  Persistence мигрирует Alembic `0008_t041_record_columns`. Реализация 5
  фазами-коммитами на одной ветке (домен → application+testing → адаптеры →
  конфиг/web → тесты/примеры/доки), финал → один squash-PR. Telegram/Matrix
  без регресса; 1110 тестов зелёные, coverage 96.5%, `mypy --strict` чист;
  ADR 2026-06-20. На T041 стоят T039 (брокеры) и T040 (email).

- **T036** — Каталоги шаблонов в шве `angarion.pages` (follow-up T029)
  [closed 2026-06-19, текущий PR]. Снято ограничение T029: шов передавал в
  `create_app` только `Page` без `template_dirs`, поэтому entry-point-
  страница не наследовала `angarion/base.html` через общий
  `request.app.state.templates`. Решение: опциональное поле
  `Page.template_dirs: tuple[Path, ...] = ()`; `create_app` собирает каталоги
  со всех `pages` и подмешивает в общий `ChoiceLoader` (после явного
  аргумента). Выбран вариант «поле у `Page`» (сохраняет симметрию «один entry
  point — один `Page`»), не отдельный дескриптор-обёртка. ADR 2026-06-19;
  гайд web-api + `examples/web/` обновлены (страница примера несёт
  `template_dirs` сама). Acceptance выполнен: entry-point-страница наследует
  `base.html` под чистым CLI без лаунчера — контрактный тест
  (`create_app(pages=load_pages())` рендерит страницу внутри каркаса
  `base.html`) зелёный; coverage 96.5%. Ветка `T036-page-template-dirs`.
- **T032** — Лёгкий поллинг недавнего окна — backstop правок/удалений
  [closed 2026-06-19, текущий PR]. Спека Clarified+Analyzed
  (`specs/T032-recent-window-poll/`); ADR 2026-06-19. Окно min(N сообщений,
  M минут), частый таймер поверх существующего catch-up каждого адаптера,
  per-pipeline opt-in `recent_poll`, per-source резолв (A-1), параметры
  `[catchup] recent_*`. **Фаза 1 (Telegram)** [PR #36, merged]: поверх
  `run_catchup` (`max_age: timedelta` + `record_truncation`), общий
  `last_seen`-курсор, подавление truncation-шума. **Фаза 2 (Matrix)**
  [текущий PR]: поверх `_catchup_room` по `/messages` (правки/удаления —
  явные `m.replace`/redaction в окне), `next_batch` не трогает. Тесты обеих
  платформ (catchup/listener/resolver/plugin/config). Acceptance выполнен:
  лёгкий поллинг ловит правку/удаление в окне без пере-скана; окно/частота
  конфигурируемы; покрыто тестами; coverage 96.5%. Ветки
  `T032-recent-window-poll` (фаза 1) + `T032-recent-window-poll-matrix`
  (фаза 2).
- **T029** — Entry-point-группа `angarion.pages` для UI-страниц в чистом CLI
  [closed 2026-06-19, текущий PR]. Встроенный раннер собирал `create_app(deps)`
  без пользовательских `pages` (Python-объект мимо CLI) — расширение Web
  требовало своего лаунчера. Решение: группа `angarion.pages` (по образцу
  `angarion.processors`/`angarion.adapters`), `load_pages()` в http-адаптере
  (не в ядре — §14.9) резолвит/проверяет тип/сортирует по `path`, раннер
  `_make_server` передаёт в `create_app`. Ограничение: только `Page` без
  `template_dirs` (наследование `base.html` через общий загрузчик — путь
  лаунчера; расширение шва вынесено в BACKLOG **T036**). Публичный API получил
  `load_pages`/`PAGES_GROUP`; гайд web-api + `examples/web` обновлены. ADR
  2026-06-19. Acceptance выполнен: контракт загрузки (резолв/сортировка/пропуск
  extra/проверка типа) + сквозной тест (страница в навигации и монтируется,
  в т.ч. через раннер) — зелёные; coverage 96.5%. Ветка `T029-pages-entrypoint`.
- **T012** — Перенос publish-тулинга из шаблона dreamteam
  [closed 2026-06-19, текущий PR]. `scripts/publish.sh` (`uv build` →
  `twine check` → `uv publish`, `--test` для TestPyPI), `twine` в dev-deps,
  `PYPI_TOKEN`/`PYPI_TEST_TOKEN` в `.secrets.example`, раздел «Публикация на
  PyPI» в README. Acceptance выполнен: артефакты `angarion` (whl + sdist)
  проходят `twine check` локально. Project-scoped PyPI-токен заводит
  Владимир отдельно (его действие; первая публикация — account-scoped).
  Ветка `T012-publish-tooling`. ADR не требуется (стандартный тулинг).
- **T027** — Reaper зависших `taken`-команд командного outbox
  [closed 2026-06-19, текущий PR]. Закрывает компромисс ADR §12.9/T024: краш
  consumer'а между `take` (`pending`→`taken`) и пометкой оставлял команду в
  `taken` навсегда (не переисполнялась — нарушение at-least-once на крайнем
  пути). Решение: колонка `outbox_commands.taken_at` (миграция `0007`,
  lease-маркер) + `CommandOutboxPort.reclaim_taken(older_than)` (зависший
  `taken` старше lease → `pending`) + reaper в фоновой prune-задаче; lease —
  `[worker] command_lease_seconds` (дефолт 300 с), активен при
  `prune_interval > 0`. Memory-эталон зеркально. ADR 2026-06-19. Acceptance
  выполнен: контрактный тест восстановления (зависший `taken` после lease
  снова берётся `take`; reaper игнорирует `pending`/терминальные) — зелёный
  и для Memory, и для Sqlite; coverage 96.4%. Ветка `T027-reap-stuck-taken`.
- **T028** — Per-write ретрай на `database is locked` в SQLite-сторах
  [closed 2026-06-19, текущий PR]. Backstop к ADR §3.1/T024: два писателя
  одного `app.db` в раздельном режиме под WAL сериализуются на едином
  write-lock'е; при исчерпании `busy_timeout` SQLite отдаёт `database is
  locked` и голая запись падает. Решение: декоратор `_retry_on_locked`
  (tenacity, 5 попыток, backoff, `reraise=True`) на всех write-методах
  `stores.py`; ретраится ровно `database is locked` (предикат `_is_locked`),
  не любой `OperationalError`. `make_engine` получил тест-параметр
  `busy_timeout_ms` (дефолт 5000 — прод не тронут). ADR 2026-06-19.
  Acceptance выполнен: тест воспроизводит контеншн двух писателей (голый
  писатель ловит `database is locked`, store-писатель пересиживает lock
  ретраем и коммитит); предикат покрыт; coverage 96.4%. Ветка
  `T028-db-locked-retry`.
- **T031** — Graceful shutdown виснет при заблокированном sender
  [closed 2026-06-18, текущий PR]. `app.stop()` отменял таски, но
  `PipelineWorker`/`DeliveryWorker` на отмене дожидались in-flight операции
  (shield-drain) — залипшая в долгом throttle/`FloodWait` sleep подвешивала
  стоп на всю длину сна (всплыло в T009/M6). Фикс: общий `shielded_drain` с
  границей `[worker] shutdown_drain_seconds` (дефолт 5.0), по истечении —
  аборт (возможный дубль покрывает at-least-once §7.1). ADR 2026-06-18.
  Acceptance выполнен: `app.stop()` ≤ таймаута при залипшем воркере, тесты
  обоих `run` воспроизводят. Ветка `T031-bounded-shutdown-drain`.

<!-- T001, T011 перенесены в CHANGELOG.md [0.1.0] — 2026-06-11. -->

<!-- T002, T003, T004, T005, T007, T013, T014, T018, T019 перенесены
     в CHANGELOG.md [0.2.0] — 2026-06-14 (milestone M4 закрыт). -->

<!-- T006, T008, T022, T023, T024, T025 перенесены в CHANGELOG.md
     [0.3.0] — 2026-06-15 (milestone M5 закрыт). -->

- **T016** — Ретеншн acked-строк `queue.db` [closed 2026-06-18, текущий PR].
  `SQLiteAckQueue` не вычищал подтверждённые записи → `queue.db` рос бессрочно.
  Решение: `EventQueuePort.purge_acked(keep_latest)` (persistqueue → обёртка
  `clear_acked_data`, memory → no-op), вызов из существующего `_prune_loop`;
  политика — ключ `[queue] keep_acked` (дефолт 1000; `0` = бессрочно).
  Обратная совместимость: чистка только при `prune_interval > 0`. ADR 2026-06-18.
  Acceptance выполнен: политика зафиксирована (ADR + конфиг), `queue.db` не
  растёт при штатной работе (adapter-тест по `COUNT(*)`). Ветка
  `T016-queue-acked-retention`.
- **T035** — Флак e2e kill-теста `test_..._keeps_delivery[before_ack]` на
  Python 3.12 в CI [closed 2026-06-18, текущий PR]. Диагноз: kill на `queue.ack`
  застаёт мишень уже в outbox, конкурентный `DeliveryWorker` в окне
  `send→mark_sent` (§7.1) даёт легитимный at-least-once дубль; тест ждал «ровно
  1» и падал `2 == 1` под нагрузкой CI. Воспроизведено локально (1/25 под CPU-
  нагрузкой), фикс — ассерт мишени `>= 1` (верхняя граница уже у
  `_assert_dup_budget`); после фикса 30/30 под нагрузкой зелёные. Продакшен-код
  не менялся. Acceptance (вариант 2): ассерт приведён к at-least-once с
  обоснованием. Ветка `T035-flaky-before-ack-at-least-once`.
- **T033** — Per-pipeline `forward_media` (флаг `[pipelines.*]`, дефолт `true`;
  при `false` worker стрипает медиа у исходящих перед outbox, processor-
  agnostic) [closed 2026-06-18, текущий PR]. Acceptance выполнен: тесты воркера
  (default keeps / `false` strips / `false` без медиа — no-op); ADR 2026-06-18.
  Ветка `T033-forward-media`. Отложено было из M7/A3.
- **T010** — **M7: медиа + Matrix-адаптер** (зонтичная, `specs/T010-m7-media-matrix/`)
  [closed 2026-06-18]. Закрыта целиком. Часть A (медиа): `MediaRef` + транзит,
  Telegram in/out, политика/реестр/скачивание, пример `examples/media` (T034).
  Часть B (Matrix+E2EE): B1 каркас+login — PR #26; B2–B5 (listener+E2EE-приём,
  sender+catch-up по `/messages`, интеграционный контур на живом Synapse —
  3 passed, пример `examples/matrix` + docs) — ветка `T010-b2-matrix-listener`,
  squash-PR части B. Acceptance Success Criteria §16 M7 выполнен: медиа сквозь
  пайплайн; Matrix new/edited/deleted + E2EE-комната; coverage ≥ 90%; четыре
  pre-push проверки чисты. Per-pipeline media-forward вынесен в BACKLOG **T033**.
- **T034** — Пример медиа `examples/media/` (M7/A4): зеркало одним аккаунтом
  + процессор `media_note`, читающий `local_path` скачанного вложения
  [closed 2026-06-16, текущий PR]. Acceptance выполнен: на реальном аккаунте
  документ из группы A доехал в B (msg 52) с аннотацией подписи; скачивание
  положило файл в git-ignored `angarion-data/media/`, процессор прочитал его
  (лог `media_seen`, `on_disk_bytes=40`); 9 unit-тестов примера зелёные.
  Ветка `T034-media-example` (стек поверх `T010-a1-mediaref`).
- **T009** — M6: интеграционный контур §13.2 + петлевой guard
  `source==target` [closed 2026-06-16, текущий PR]. Acceptance выполнен:
  `pytest -m integration` зелёный (8 passed) на реальном аккаунте; guard
  покрыт и в обязательном наборе (fake-client e2e). Контур драйвит через
  catch-up (надёжно), не через live self-send.
- **T030** — реконсиляция update-state (`get_me`+`catch_up`) в
  `connect_client` [closed 2026-06-16, текущий PR]. Не подтверждён как
  блокирующий прод-баг (см. ADR); оставлен как улучшение робастности.