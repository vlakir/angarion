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

<!-- T001, T011 перенесены в CHANGELOG.md [0.1.0] — 2026-06-11. -->

- **T007** — **M4** — процессоры `llm` (OpenAI-совместимый endpoint,
  httpx, Jinja2-промпты, `api_key_env`, tenacity-ретраи) + `template`
  (Jinja2, закрывает долг C-3) (`specs/T007-m4-llm-processor/spec.md`,
  ветка `T007-m4-llm-processor`). Реализован 3 фазами (Q8): Jinja2-хелпер
  + `template`, `llm` (тонкая граница `LlmHttpClientPort` + httpx +
  tenacity), entry points + Update. Acceptance (§16 M4): unit-тесты
  `llm`/`template` без сети зелёные (coverage llm.py 99%, всего 96%);
  ручной прогон через локальную модель (Ollama) — N5. Дайджест и примеры
  вынесены в T019; startup-валидация `processor_config` (W1) — в T021
  [closed 2026-06-14, текущий PR].

- **T018** — [2026-06-13] Пример `examples/forward/`: пересылка новых
  сообщений из группы A в B (passthrough) — `app.toml` + скрипт
  «всё в одном» `run.sh` (idempotent login/migrate/run, рантайм в
  git-ignored `angarion-data/`) + README с настройкой и
  troubleshooting'ом авторизации. Документация §17.9 (quick start к M3).
  Acceptance: пример воспроизводим по README; секреты и рантайм в репо
  не попадают [closed 2026-06-13, текущий PR].

- **T005** — **M3** — Telegram-адаптер: Telethon listener + catch-up (§9)
  + sender через plugin-контракт §12.11, мультиаккаунт, троттлинг, CLI
  (`run` / `migrate` / `login`) (`specs/T005-m3-telegram/spec.md`, ветка
  `T005-m3-telegram`). Реализован по 6 фазам (Q7): SessionStorePort +
  контракт плагина, граница Telethon + live-listener, catch-up §9.3,
  sender (token-bucket + FloodWait), CLI + composition root + graceful
  shutdown, acceptance. Acceptance (§16 M3) выполнен: e2e-тест «простой →
  правки/удаления → рестарт» через настоящий `build_app` (sqlite-персист
  + fake-граница Telethon) даёт корректные edited/deleted/new на
  рестарте; суточный прогон — ручной на тестовом аккаунте (N4); гайд
  бэкапа §17.6 в README [closed 2026-06-13, текущий PR].

- **T004** — Ревизия исходников tgcf (MIT, v1.1.8) перед M3 (§15.18,
  §16 M3): FloodWait/реконнекты, пагинация истории, резолв сущностей,
  session string (`specs/T004-tgcf-review/notes.md`, ветка
  `T004-tgcf-review`). Acceptance выполнен: конспект готов, решения
  «брать/не брать» зафиксированы (ADR 2026-06-13). Прямых заимствований
  кода нет — берём приёмы Telethon на уровне концепции; антипаттерны и
  вопросы к спеке M3 (T005) отмечены [closed 2026-06-13, текущий PR].

- **T003** — **M2** — персистентность: PersistQueue-адаптер;
  SQLAlchemy-адаптеры (dedup, registry, cursors, state, analytics,
  outbox, DLQ) + Alembic-миграции; публикация `angarion.testing`
  (контрактные наборы)
  (`specs/T003-m2-persistence/`, ветка `T003-m2-persistence`).
  Acceptance (§16 M2) выполнен: контрактные тесты зелёные на
  sqlite/persistqueue через `angarion.testing`; kill-тест — потерь
  нет, дубли только в остаточном окне C-9. По дороге закрыт крэш-фикс
  ingest (A-11, ADR 2026-06-12) [closed 2026-06-12, PR #6].

- **T002** — **M1** — ядро: домен (события, `thread_id`, открытый
  `Messenger`), порты (вкл. `AdapterCapabilities`, непрозрачный курсор,
  контракт `AdapterPlugin`/`Listener`), application (ingest с fan-out,
  worker, router, деградация по матрице), outbox исходящих +
  `DeliveryWorker` (C-9), реестр плагинов через entry points,
  InMemory-адаптеры как плагин («нулевой пациент»), конфиг
  (pydantic-settings), хелперы ключей (`domain/keys`)
  (`specs/T002-m1-core/`, ветка `T002-m1-core`).
  Acceptance (§16 M1) выполнен: сквозной тест new/edited/deleted +
  multicast in-memory; fail-fast/деградация по матрице покрыты;
  InMemory-плагин загружается через entry point
  [closed 2026-06-11, текущий PR].

- **T013** — Осмысленный README.md: статус pre-alpha, ключевые идеи из
  ТЗ, дорожная карта M1–M7, бейджи, гайд разработчика
  [closed 2026-06-11, PR #3].

- **T014** — Интеграционный контур Telegram (предтеча M6): `.secrets` +
  `scripts/tg_login.py` + маркер `integration` + первый интеграционный
  тест (авторизация, доступ к двум тестовым супергруппам,
  send/delete-roundtrip). Прогон зелёный на реальном аккаунте
  [closed 2026-06-11, текущий PR].
