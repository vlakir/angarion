# Plan: M5 — Web API + Web UI + Auth + Admin (T008)

**Дата:** 2026-06-14
**Спека:** `specs/T008-m5-web/spec.md`
**Решения дробления (Clarify 2026-06-14):** дробим **крупнее, ~4 группы**;
spec/plan/analyze едут **с первой фазой** (без отдельного umbrella-PR).

---

## 1. Декомпозиция на под-задачи (4 группы)

Каждая группа — свой `T<NNN>`, ветка `T<NNN>-<slug>`, один PR (один
коммит), одна сессия (через `/clear`). T-ID: max существующих = T021
(свёрнут) → новые **T022–T025**. Умбрелла этапа — **T008** (в `BOARD →
Doing`); под-задачи трекаются здесь и поднимаются в `BOARD` при взятии.

| Группа | T-ID | Состав (§) | Зависит от |
|---|---|---|---|
| **A. Web core** | **T022** | T021 (`config_model` fail-fast) + API core (§12.5) + UI core (§12.6 без `/ui/pipelines`). **Сюда же spec/plan/analyze.** | — (порты M1–M4) |
| **B. Auth** | **T023** | fastapi-users, `UserRow`+миграция, JWT+cookie, роли, регистрация/одобрение, `/ui/users`, bootstrap-админ, FR-0 api-secret (§12.7) | A |
| **C. Ops** | **T024** | Динамика+админ-операции (§12.8) + командный outbox + роли процессов (§12.9) | A, B |
| **D. Viz + Docs** | **T025** | `/ui/pipelines` (серверный SVG) + документация этапа §17.9 | A, C (статус пауз) |

**Почему такие границы.** A — фундамент (фабрика `create_app`, DI поверх
портов, SSR-каркас): без него ни auth, ни админка не на чем монтировать.
B — авторизация навешивается на роутеры A, поэтому строго после A. C —
самая тяжёлая группа: динамика (§12.8) и outbox (§12.9) связаны (catchup/
restart-операции **порождают** команды outbox, notify-заявки — тоже), их
дробление дало бы взаимные заглушки; держим вместе. D — `/ui/pipelines`
читает статус паузы (C) и пользовательский layout (A), docs — deliverable
всего этапа, логично закрывать последними.

**Подстраховка по объёму:** если C при реализации раздувается — режем по
шву §12.8 (settings+admin) / §12.9 (outbox+роли) на T024/T026. Решаем по
факту в сессии C, не превентивно.

## 2. Новые порты и сущности (по группам)

- **A:** `AngarionDeps` (контейнер портов в `app.state`); DI-deps
  `AnalyticsDep/RegistryDep/StateDep/QueueDep/CursorsDep`; `Page`
  (дескриптор UI-страницы); хук `config_model` в `ProcessorPort` (T021).
- **B:** `UserRow` (ORM, миграция Alembic); auth-deps `CurrentUser`,
  `AdminUser`; два backend'а fastapi-users (JWT + cookie) над одним store.
- **C:** `RuntimeConfigPort` (+ `DynamicSettings` DTO, `AppSettingRow`
  ORM, InMemory); `CommandOutboxPort` (+ `OutboxCommandRow` ORM,
  InMemory); событие аналитики `admin_op`, `notify_failed`, `deferred`.
- **D:** только шаблоны/SVG-рендер, без новых портов.

Все новые порты — с **контрактным набором** (`angarion.testing`),
прогоняемым по InMemory + SQLAlchemy (требование ТЗ §12.11).

## 3. Технические развязки (входят в Analyze спеки)

### 3.1. Два писателя в `app.db` (раздельный режим) — 🟡
api-процесс пишет users/settings/audit, pipeline-процесс — analytics/
registry/outbox-статусы. SQLite = один писатель. **Решение:** `PRAGMA
busy_timeout` (уже есть WAL) + ретрай записи на `database is locked` в
storage-слое; записи api-процесса редкие (админ-действия). Не вводим
второй движок БД (вне scope, §16 M5 — SQLite). Зафиксировать в
`DECISIONS.md` (ADR при реализации C).

### 3.2. `restart` во встроенном режиме (`--with-api`) — 🟢
Один процесс → restart гасит и pipeline. By design (graceful shutdown,
подъём — супервизор). Документируется в deploy-гайде (D). В раздельном —
`restart_pipeline` через outbox (C).

### 3.3. `auth="none"` и UI/admin-операции — 🟢 (решено дефолтом)
При `auth="none"` все роутеры открыты; `CurrentUser`/`AdminUser`
резолвятся в синтетического локального админа (admin-страницы/операции
доступны для dev/локали). При bind ≠ `127.0.0.1` — громкое предупреждение
в лог при старте (§12.7). Вшито в spec FR-3.

### 3.4. Порядок миграций Alembic — 🟢
B добавляет `UserRow`, C — `AppSettingRow`/`OutboxCommandRow`. Каждая
группа = своя ревизия, линейно поверх M2-схемы. `auto_migrate=false`
(fail-fast при расхождении) сохраняется.

## 4. Тест-стратегия (TDD, общая для групп)

- ASGI через `httpx.AsyncClient(ASGITransport(create_app(...)))` на
  **InMemory-портах** — без сети/БД/Telegram.
- Каждый новый порт — контрактный набор (InMemory + SQLAlchemy).
- Acceptance §16 M5 (см. spec §4) распределяются по группам: auth-матрица
  и цикл регистрации → B; пауза-без-потерь, requeue, контракт
  `RuntimeConfigPort`, аудит → C; цикл notify, контракт
  `CommandOutboxPort` → C; bootstrap fail-fast → B (api-secret/admin) + A
  (T021 `config_model`).
- Coverage ≥ 90% на `src/` держим **в каждой** группе (порог жёсткий).

## 5. Документация (D, §17.9 — без неё этап не закрыт)

- Deploy-гайд: systemd (`Restart=always`), бэкапы (`app.db`+ключи),
  reverse proxy + TLS, режимы `--with-api` / `--role`.
- Гайд автора плагина: walkthrough по `angarion.testing` + пример
  пользовательской ручки/страницы (`register_page`, `AnalyticsDep`).
- Справочник публичного API: пополнить новым web-API (`create_app`, deps,
  `CurrentUser/AdminUser`, `RuntimeConfigPort`, `CommandOutboxPort`).
- **Примеры (`examples/`, договорённость 2026-06-13):** M5 user-facing →
  нужен пример (запуск с `--with-api`, кастомная ручка/страница). Завести
  отдельной задачей при закрытии D (или сразу в D).

## 6. Релиз

Все 4 PR'а вливаются в `main` инкрементально (`[Unreleased]`
накапливает). По завершении D — milestone M5 закрыт → версия **1.0.0**
(§16: 1.0.0 по завершении M5; гарантии стабильности §12.11 в полном
объёме). CHANGELOG-запись 1.0.0 перечисляет T008 (вкл. бывш. T021),
T022–T025; ретроспектива milestone — секцией `### Retrospective`.

## 7. Что НЕ делаем в M5 (из spec §7, напоминание)

Редактор топологии через UI, RBAC тоньше admin/viewer, OAuth/SSO,
графические JS-библиотеки, медиа и второй мессенджер (M7),
горизонтальное масштабирование.
