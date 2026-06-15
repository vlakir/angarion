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

- **T008** — **M5** — Web API + Web UI + Auth + Admin (умбрелла этапа;
  `specs/T008-m5-web/`). [взят 2026-06-14] Spec/plan/analyze готовы
  (статус Analyzed, 🔴 нет). Дроблён на 4 группы (plan.md §1):
  **T022** Web core (+spec/plan, +свёрнутый T021 `config_model`) →
  **T023** Auth → **T024** Ops (динамика+outbox) → **T025** Viz+Docs.
  Каждая — своя ветка/PR/сессия; поднимается в `Doing` при взятии.
  При релизе 1.0.0 CHANGELOG содержит «T008 (вкл. бывш. T021),
  T022–T025» (уникальность T-ID).
## Done

- **T024** — **M5/C** — Ops: динамика + админ-операции (§12.8) +
  командный outbox + роли процессов (§12.9) (ветка `T024-ops`). [closed
  2026-06-15, PR #18] Самая тяжёлая группа, четыре фазы:
  (1) `RuntimeConfigPort` (`DynamicSettings`, миграция 0004) →
  (2) `CommandOutboxPort` (`OutboxCommandRow`, миграция 0005,
  атомарный `take` `pending`→`taken`) →
  (3) применение на лету + пауза-без-потерь (worker defer-to-tail,
  sender-троттлинг, log level через `SettingsNotifier`) →
  (4) админ-операции (`/api/v1/admin` + `/ui/settings` + `/ui/dlq`:
  pause/resume, requeue `attempt=0`, restart/catchup, аудит `admin_op`,
  viewer→403), `OutboxConsumer` (notify/catchup/restart_pipeline),
  неблокирующее уведомление о заявке (`[api.notify]`, закрыт долг T023),
  роли процессов `--role pipeline|api|combined` / `--with-api`
  (uvicorn-раннер), web composition root. Один squash-PR на группу.
  Acceptance spec §4 закрыт: контракты портов (InMemory+SQLAlchemy),
  пауза-без-потерь, requeue `attempt=0`, цикл notify, аудит `admin_op`,
  viewer→403. ADR 2026-06-15 (§3.1 busy_timeout два писателя; per-write
  ретрай → BACKLOG T028; reaper `taken` → BACKLOG T027). Дальше **T025**
  Viz+Docs — последняя группа M5.

- **T023** — **M5/B** — Auth (ветка `T023-auth`). [closed 2026-06-15,
  PR #17] fastapi-users (+`-db-sqlalchemy`), §12.7. Все три фазы:
  (1) identity data layer — `[api]`/`ApiConfig`, `UserRow` ORM +
  миграция 0003 → (2) fastapi-users core — JWT-логин, свой register-
  эндпоинт, `CurrentUser`/`AdminUser` (auth на уровне роутеров),
  `auth="none"` синтетический админ, bootstrap + fail-fast → (3)
  регистрация-одобрение, лимит `max_pending_registrations`, JSON-ручки
  `/api/v1/users` (admin-only), cookie-вход `/ui/login`/`/ui/logout`/
  `/ui/register`, админка `/ui/users` (htmx). Один squash-PR на группу.
  Acceptance spec §4 закрыт: auth-матрица, цикл регистрация→одобрение→
  вход, bootstrap, лимит pending, cookie login/logout. **Notify-заявки —
  в T024** (outbox). Дальше **T025** Viz+Docs (последняя группа M5).

- **T022** — **M5/A** — Web core (ветка `T022-web-core`). [closed
  2026-06-14, PR #16] Все три фазы готовы: (1) T021 `config_model`
  fail-fast → (2) API core (`create_app`, `AngarionDeps`, DI, `/api/v1`,
  webhook-роутеры) → (3) UI core (SSR Jinja2+htmx+Pico, `/ui`,
  `/ui/events`, `/ui/fragments/*`, офлайн-ассеты, контракт страниц
  `Page`/`create_app(pages=…)`). Один squash-PR на всю группу.
  Acceptance spec §4 в части A закрыт (bootstrap fail-fast
  `config_model`; auth-матрица и реестр — в T023). Дальше **T023** Auth.

<!-- Закрытые задачи, ждущие переноса в CHANGELOG.md при следующем
     релизе или значимой точке. После переноса — очищаем. -->

<!-- T001, T011 перенесены в CHANGELOG.md [0.1.0] — 2026-06-11. -->

<!-- T002, T003, T004, T005, T007, T013, T014, T018, T019 перенесены
     в CHANGELOG.md [0.2.0] — 2026-06-14 (milestone M4 закрыт). -->
