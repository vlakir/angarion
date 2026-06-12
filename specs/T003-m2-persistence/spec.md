# Spec: T003 — M2: персистентность (persist-queue, SQLAlchemy + Alembic, angarion.testing)

**Статус:** In Progress (clarify C-1…C-7 и analyze A-1…A-11 закрыты
2026-06-12; фаза 1 — `angarion.testing`; фаза 2 — persistqueue-очередь,
FR-1/FR-2; фаза 3 — SQLAlchemy/SQLite-хранилище + Alembic, FR-3…FR-8 и
FR-11; фаза 4 — крэш-фикс ingest, A-11; фаза 5 — kill-тест, FR-12.
Все FR реализованы — осталось: CHANGELOG, BOARD → Done, squash, PR)
**Дата создания:** 2026-06-11
**Связанные документы:** `CONCEPT.md` (ТЗ v2.1) — §§7.4, 12.2–12.3,
12.11 (angarion.testing), 13, 16 (M2), 17.3; `DECISIONS.md` (ADR C-8,
C-9 от 2026-06-11); `specs/T002-m1-core/` (границы M1); `BOARD.md` (T003)

> Как и в T002: ТЗ v2.1 — первоисточник, спека фиксирует **границу
> этапа M2** и уточнения clarify. При расхождении приоритет у ТЗ;
> отступления (унаследованные от C-9 T002 и новые) фиксируются здесь
> и в `DECISIONS.md`.

---

## 1. Overview

Второй этап реализации angarion: конвейер M1 переживает рестарт и
падение процесса. Очередь становится персистентной (persist-queue,
`queue.db`), все storage-порты получают SQLAlchemy/SQLite-реализации
(`app.db`) со схемой под управлением Alembic, DLQ хранится в БД.
Контрактные наборы тестов M1 оформляются в публичный пакет
`angarion.testing` — сертификационный инструмент для авторов сторонних
адаптеров (§12.11) и одновременно механизм проверки новых
персистентных реализаций. Ядро (домен, application) не меняется —
M2 по построению является чистым добавлением адаптеров.

## 2. Сценарии использования

- Как **оператор приложения на angarion**, я хочу, чтобы рестарт или
  падение процесса (вплоть до kill -9) не теряло и не дублировало
  доставку событий, чтобы конвейер можно было эксплуатировать
  непрерывно.
- Как **разработчик приложения**, я хочу включить персистентность
  декларативно (`[queue] backend = "persistqueue"`, `[storage]
  backend = "sqlite"`), чтобы не писать инфраструктурный код.
- Как **автор стороннего адаптера**, я хочу импортировать
  `angarion.testing` и прогнать контрактные наборы по своей
  реализации, чтобы зелёный прогон гарантировал соответствие контракту
  портов.
- Как **разработчики angarion (мы)**, я хочу, чтобы те же контрактные
  наборы, что проверяли InMemory, проверяли SQLAlchemy- и
  persistqueue-адаптеры — «настоящая» реализация обязана вести себя
  неотличимо от эталонной.

## 3. Functional Requirements

Нумерация FR-* — продолжение ссылочной схемы T002 (свой счётчик
в каждой спеке).

### Очередь

- **FR-1** ДОЛЖНА: адаптер `EventQueuePort` на
  `persistqueue.SQLiteAckQueue` (§12.2): файл `queue.db`;
  сериализация envelope — `model_dump_json()` (строки, не pickle);
  синхронные вызовы — через `asyncio.to_thread()`; блокирующий
  `get()` — поллинг с коротким таймаутом, чтобы задача оставалась
  отменяемой (A-7); `recover()` → `resume_unack_tasks()`; `depth()` —
  pending/unacked для §17.5; `QueueItem.receipt` — `pqid`
  persistqueue (непрозрачность уже в контракте). Семантика C-8
  (`not_before`, defer-to-tail) обслуживается существующей механикой
  worker'а — нативной задержки от очереди не требуется.
- **FR-2** ДОЛЖНА: регистрация очереди как `QueueBackend` в entry
  point `angarion.queues` под именем `persistqueue`; `[queue]
  backend = "persistqueue"` + `path` резолвятся бутстрапом по реестру
  (механизм M1, FR-12 T002). Зависимость persist-queue — extra
  `angarion[persistqueue]` (C-1).

### Хранилище SQLAlchemy + Alembic

- **FR-3** ДОЛЖНА: storage-бэкенд на SQLAlchemy 2.0 async (§12.3):
  `create_async_engine("sqlite+aiosqlite:///<path>")`; `PRAGMA
  journal_mode=WAL` и `foreign_keys=ON` — event-хук `connect`;
  декларативный стиль 2.0 (`DeclarativeBase`, `Mapped` /
  `mapped_column`), полная типизация; каждый адаптер получает
  `async_sessionmaker` через конструктор, транзакции — `async with
  session.begin()`, никаких глобальных сессий. Зависимости
  (SQLAlchemy + Alembic + aiosqlite) — extra `angarion[sqlite]`
  (§14.9). Все временные колонки — UTC, в БД — ISO 8601 с явным
  смещением через общий `TypeDecorator` (§17.4; A-4).
- **FR-4** ДОЛЖНА: ORM-сущности (приватные для адаптера, §3.1) —
  объём M2: `AnalyticsEventRow`, `InboundDedupRow` (+ колонка
  `marked_at` для prune — A-6), `MessageRow`, `MessageVersionRow`,
  `SourceCursorRow`, `ProcessorStateRow`, `DeadLetterRow`,
  `OutboundRow` (журнал outbox исходящих — C-9 T002; в таблице §12.3
  отсутствует, добавление подтверждено Владимиром 2026-06-12).
  `DeliveredDedupRow` из §12.3 НЕ создаётся (упразднён C-9: выходная
  идемпотентность — первичный ключ outbox). `UserRow`,
  `AppSettingRow`, `OutboxCommandRow` — M5, приходят со своими
  миграциями (C-2).
- **FR-5** ДОЛЖНА: SQLAlchemy-реализации **всех** портов
  `StorageBundle`: dedup (`insert ... on_conflict_do_nothing` +
  `rowcount` — атомарное «отметить, если не было», §7.4), registry
  (история версий, staleness-guard, `known_ids` с числовым
  сравнением десятичных id — фильтрация на стороне Python для
  паритета с InMemory, A-5), cursors, state, analytics, dead_letters,
  outbox (insert-if-absent по `idempotency_key`,
  due/mark_sent/reschedule/mark_failed/get). Все методы `prune()` —
  реализованы (фоновый запуск — M3, A-7 T002).
- **FR-6** ДОЛЖНА: миграции Alembic (async-шаблон): каталог
  `src/angarion/migrations/` (внутри пакета — едет в wheel, C-5),
  первая ревизия — полная схема M2 и только она (C-2); любое
  последующее изменение ORM — новой ревизией (автогенерация с
  обязательной ручной ревизией). `migrations/` исключён из ruff /
  mypy / coverage (паттерны конфига уже покрывают).
- **FR-7** ДОЛЖНА: применение миграций — программное
  (`alembic upgrade head` средствами библиотеки, sync-вызов в
  `asyncio.to_thread()` — A-8) при сборке sqlite-бэкенда в bootstrap;
  конфигурируемо: `[storage] auto_migrate = true` (default, C-3);
  при `false` и расхождении схемы — fail-fast с внятным сообщением
  «примени миграции». CLI `angarion migrate` — M3.
- **FR-8** ДОЛЖНА: регистрация бэкенда как `StorageBackend` в entry
  point `angarion.storages` под именем `sqlite`; `[storage]
  backend = "sqlite"` + `path` резолвятся бутстрапом.

### angarion.testing

- **FR-9** ДОЛЖНА: публичный пакет `angarion.testing` (§12.11):
  контрактные наборы тестов всех портов M1/M2 + фабрики тестовых
  DTO, импортируемые сторонним пакетом. Состав публичной поверхности
  — контрактные классы (по одному на порт) и фабрики; прогон у
  автора адаптера: параметризация классов своей реализацией через
  fixture-override (как в `tests/contracts/test_memory.py`).
  Зависимости pytest + pytest-asyncio — extra `angarion[testing]`
  (C-1, C-6, A-3); без extra импорт падает честным ImportError.
  Контрактные классы несут `pytestmark = pytest.mark.asyncio`
  (классовый атрибут наследуется) — наборы работают и при
  `asyncio_mode = "strict"` у потребителя (A-3). Assert-rewriting
  для внятных сообщений у потребителя — `pytest11` entry point
  с `pytest.register_assert_rewrite("angarion.testing")` (A-2).
- **FR-10** ДОЛЖНА: `tests/contracts/` библиотеки переводится на
  импорт из `angarion.testing` (сами наборы живут в `src/`,
  в `tests/` — только параметризация реализациями). Контракты
  пополняются недостающими инвариантами, выявленными при реализации
  персистентных адаптеров (например, переживание переоткрытия
  хранилища), — расширения применяются ко всем реализациям,
  включая InMemory.

### Тесты этапа

- **FR-11** ДОЛЖНА: тесты SQLAlchemy-адаптеров — на временной
  SQLite-базе, схема разворачивается **миграциями Alembic** (§13.2:
  это заодно проверяет сами миграции), не `create_all()`.
- **FR-12** ДОЛЖНА: kill-тест (§16 M2): конвейер на персистентных
  адаптерах в дочернем процессе, `SIGKILL` в произвольный/заданный
  момент, рестарт (`recover()` + повторная обработка) — доставка
  без потерь; без дублей вне остаточного окна C-9 (трактовка — C-4).
  В обязательном CI-наборе, бюджет ~30 с: детерминированные
  kill-точки + рандомизированные итерации по бюджету (C-7);
  POSIX-only (`SIGKILL`), на не-POSIX — skip с маркировкой (A-10).
- **FR-13** НЕ ДОЛЖНА: менять домен, application-слой и контракты
  портов M1 (кроме аддитивных уточнений контрактных тестов FR-10 и
  крэш-фикса A-11 — аддитивный `DedupStorePort.seen()` + перенос
  отметки дедупа в ingest после fan-out; одобрено Владимиром
  2026-06-12); не тянуть инфраструктурные зависимости в ядро —
  SQLAlchemy / Alembic / aiosqlite / persist-queue / pytest
  подключаются только через extras (C-1).

## 4. Success Criteria

Acceptance §16 M2 дословно:

- **SC-1**: контрактные тесты зелёные на реальных реализациях
  (SQLAlchemy-бэкенд целиком, persistqueue-очередь) — **через
  `angarion.testing`**.
- **SC-2**: kill -9 в произвольный момент не теряет и не дублирует
  доставку (трактовка дублей — с поправкой C-9, см. C-4: потерь нет
  всегда; дублей нет вне остаточного окна «send → mark_sent»; при
  kill внутри окна — максимум один дубль).

Плюс качество (ТЗ §13–14 + проектный CLAUDE.md):

- **SC-3**: TDD (тест до кода), coverage ≥ 90%, `mypy --strict`
  на пакет (включая `angarion.testing`, исключая `migrations/`),
  ruff чистый; 4 обязательные проверки перед каждым push.
- **SC-4**: InMemory-реализации проходят расширенные контракты FR-10
  без правок поведения (если правки потребовались — это баг InMemory,
  фиксится с воспроизводящим тестом).
- **SC-5**: чужой пакет может выполнить `from angarion.testing
  import ...` и собрать прогон без доступа к `tests/` библиотеки
  (проверяется тестом на импортируемость публичной поверхности).

## 5. Key Entities

Полные определения — ТЗ §12.2–12.3. Новых доменных DTO и портов M2
не вводит (порты `OutboxPort`/`DeadLetterPort` созданы в M1). Новое:

- **ORM-сущности** (приватные, `adapters/storage/...`): состав FR-4.
- **`OutboundRow`** — персистентная форма `OutboundRecord` M1
  (idempotency_key PK, pipeline, event_uid, payload сообщения,
  status, attempts, next_attempt_at, finished_at, receipt/error).
- **`angarion.testing`** — публичный пакет: контрактные классы
  портов + фабрики DTO.
- **Структура пакета**: `adapters/queue/persistqueue_.py`,
  `adapters/storage/` (orm.py, engine.py, адаптеры портов),
  `src/angarion/migrations/` (C-5), `src/angarion/testing/`.

## 6. Assumptions & Constraints

- Ядро без новых обязательных зависимостей; extras после M2:
  `angarion[sqlite]` (SQLAlchemy + Alembic + aiosqlite),
  `angarion[persistqueue]` (persist-queue), `angarion[testing]`
  (pytest + pytest-asyncio) — C-1.
- Релиз на PyPI после M2 не публикуется: `[Unreleased]` копит
  M1 + M2 до M3 (C-7 T002).
- Очередь (persist-queue) — вне SQLAlchemy и вне Alembic: отдельный
  файл со своей схемой (§12.3).
- `DeadLetterPort` без автоочистки — разбор ручной (§17.3); prune
  DLQ не реализуется.
- Время — строго UTC; в БД — ISO 8601 с явным смещением (§17.4).
- Процесс: T003 = один PR на ветке `T003-m2-persistence`; фазы —
  коммиты на ветке, squash перед merge (паттерн C-5 T002).
- Python ≥ 3.12; CI-матрица 3.12–3.14 — новые зависимости
  (включая greenlet, транзитивную для SQLAlchemy async) обязаны
  на ней работать (A-9).

## 7. Out of Scope (T003 / M2)

- Telegram-адаптер, catch-up §9.3, CLI (`angarion run` / `migrate`),
  фоновый запуск prune/TTL-задач, tenacity-политика sender'а — **M3**
  (T005); ревизия tgcf — **T004**.
- LLM-процессор, `template`/Jinja2 — **M4** (T007).
- Web API / UI / auth / `RuntimeConfigPort` / `CommandOutboxPort` и
  их ORM-сущности (`UserRow`, `AppSettingRow`, `OutboxCommandRow`),
  requeue DLQ через админ-ручки — **M5** (T008).
- Интеграционные сценарии на реальном аккаунте — **M6** (T009).
- Медиа, второй мессенджер — **M7** (T010).
- Альтернативные СУБД (PostgreSQL и т.п.): схема и адаптеры пишутся
  под `sqlite+aiosqlite` (§12.3); диалект-нейтральность сверх
  бесплатной — не цель этапа.
- Документация-сайт (mkdocs, автосправочник API «с M2» §17.9) —
  отдельная задача T006, не в этом PR.

---

## Clarify (заполняется Claude)

### Open questions

- (нет — clarify закрыт 2026-06-12)

### Resolved (ответы Владимира 2026-06-12: «всё ок» по C-1…C-7 и по
замене `DeliveredDedupRow` → `OutboundRow` в FR-4)

- **C-1. Состав extras** → три независимых extra:
  `angarion[persistqueue]`, `angarion[testing]` (в дополнение к
  `angarion[sqlite]` из §14.9); зонтичный `[persistence]` отвергнут
  (очередь и БД — разные подсистемы, §12.3). Вшито в FR-2, FR-3,
  FR-9, FR-13, Assumptions.
- **C-2. Объём первой миграции** → только сущности M2 (FR-4);
  таблицы M5 приходят своими ревизиями в M5 (миграции аддитивны).
  «Вся схема ТЗ сразу» отвергнута: мёртвые таблицы без потребителей
  (анти-паттерн C-1 T002). Вшито в FR-4, FR-6.
- **C-3. Автоприменение миграций** → `[storage] auto_migrate = true`
  (default): bootstrap применяет `upgrade head` программно; при
  `false` и расхождении схемы — fail-fast. Отложить применение к M3
  отвергнуто: бэкенд M2 не собрать без ручного alembic-вызова.
  Вшито в FR-7.
- **C-4. Формулировка «не дублирует» в kill-тесте** → kill-тест
  проверяет: (а) отсутствие потерь всегда, (б) отсутствие дублей при
  kill вне остаточного окна C-9 «send выполнен, mark_sent не
  записан», (в) при kill внутри окна — максимум один дубль, потери
  нет (допустимо at-least-once §7.1). Вшито в FR-12, SC-2.
- **C-5. Размещение `migrations/`** → `src/angarion/migrations/`
  (внутри пакета, как §3.2 ТЗ): миграции едут в wheel, `upgrade
  head` работает у пользователя библиотеки (нужно для FR-7).
  Исключения ruff/mypy/coverage уже покрывают паттерном
  `**/migrations`. Вшито в FR-6, Key Entities.
- **C-6. Зависимость pytest у `angarion.testing`** → pytest
  импортируется в шапке модулей (без условных импортов), попадает в
  extra `angarion[testing]`; без extra — честный ImportError.
  Вшито в FR-9.
- **C-7. Бюджет и место kill-теста** → в обязательном CI-наборе
  (это acceptance этапа), бюджет ~30 с: детерминированные kill-точки
  + рандомизированные итерации по бюджету. Маркер `slow` вне
  обязательного набора отвергнут. Вшито в FR-12.

---

## Analyze (заполняется Claude)

Проход 2026-06-12 (spec v. clarified против ТЗ v2.1, кода M1 и
эмпирической проверки ruff на перенесённых контрактах).

- 🔴 **A-1. Переезд контрактов в `src/` ловится ruff: 162 ошибки.**
  Замер (копия `tests/contracts/*_contract.py` + `factories.py` под
  `src/angarion/testing/`, проектный конфиг): **138 × S101** (голый
  `assert`), 10 × INP001, 9 × TC001, 5 × PLR2004. INP001 снимается
  `__init__.py`, TC001 — autofix, PLR2004 — именованными константами
  (правим код, не глушим). Остаётся S101: `assert` — природа
  тестового кода, замена на `raise` ломает pytest-репортинг и
  идиому. Предложение: **per-file-ignores
  `"src/angarion/testing/**" = ["S101"]`** с
  комментарием-обоснованием — требует явного согласия Владимира
  (правило «не расширять ignore без обсуждения»). Альтернативы
  отвергнуты: `raise AssertionError` вручную — теряем introspection
  и читаемость 138 проверок; оставить наборы в `tests/` — ломает
  саму цель FR-9 (публичный импортируемый пакет).
  **Resolved 2026-06-12: подтверждено Владимиром** — per-file-ignore
  S101 добавлен (только каталог `src/angarion/testing/**`);
  INP001 / TC001 / PLR2004 починены кодом, не подавлением.
  ADR — в `DECISIONS.md`. Реализационное уточнение: pytest11-модуль —
  `angarion._testing_plugin` **вне** пакета testing (entry point
  внутрь пакета импортировал бы `__init__` с подмодулями раньше
  регистрации rewrite).
- 🟡 **A-2. Pytest assertion rewriting не работает для импортируемых
  пакетов.** pytest переписывает `assert` только в собираемых
  тестовых модулях — у потребителя `angarion.testing` сообщения
  провалов будут пустыми (`AssertionError` без diff). Фикс принят в
  FR-9: entry point `pytest11` с модулем-плагином, вызывающим
  `pytest.register_assert_rewrite('angarion.testing')` (стандартный
  паттерн библиотек контрактных тестов).
- 🟡 **A-3. Контрактные наборы — async, режим pytest-asyncio
  потребителя неизвестен.** Наш `asyncio_mode = "auto"` — деталь
  нашего конфига; у стороннего автора default `strict` молча не
  соберёт ни одного теста. Фикс принят в FR-9: классовый
  `pytestmark = pytest.mark.asyncio` (наследуется подклассами) +
  pytest-asyncio в extra `angarion[testing]`.
- 🟡 **A-4. SQLite не хранит timezone в DateTime.** §17.4 требует
  «ISO 8601 с явным смещением», дефолтный `Mapped[datetime]` на
  SQLite пишет naive. Фикс принят в FR-3: общий `TypeDecorator`
  (TEXT, ISO 8601 с offset, нормализация в UTC) для всех временных
  колонок.
- 🟡 **A-5. `known_ids` с числовым сравнением десятичных id в SQL
  нетривиален** (CAST ломается на смешанных форматах). Фикс принят
  в FR-5: выборка id источника + фильтрация в Python — точный
  паритет с InMemory-реализацией; объём ограничен окном реестра.
- 🟢 **A-6. `InboundDedupRow` в таблице §12.3 — только PK, а
  `prune(older_than)` требует временной отметки.** Аддитивное
  уточнение схемы: колонка `marked_at`. Вшито в FR-4.
- 🟢 **A-7. Блокирующий `get()` persistqueue в `to_thread` не
  отменяем штатно** (cancel задачи не прерывает поток). Вшито в
  FR-1: поллинг с коротким таймаутом вместо вечной блокировки —
  контрактный `test_get_waits_for_put` и graceful shutdown
  работают. `QueueItem.receipt` = `pqid`.
- 🟢 **A-8. Alembic — sync API.** Программный `upgrade head` из
  async-бутстрапа — `asyncio.to_thread()` (env.py async-шаблона
  поддерживает оба пути). Вшито в FR-7.
- 🟢 **A-9. greenlet — транзитивная зависимость SQLAlchemy async** —
  обязана собираться на CI-матрице 3.12–3.14. Риск низкий
  (актуальные релизы покрывают), проверится CI. Вшито в Assumptions.
- 🟢 **A-10. `SIGKILL` — POSIX-only.** Kill-тест на не-POSIX
  платформах скипается с маркировкой; CI — Linux, acceptance
  гоняется всегда. Вшито в FR-12.
- 🔴 **A-11. Крэш-окно ingest: `mark_inbound` до `queue.put` терял
  событие безвозвратно.** Найдено при проектировании kill-теста
  (фаза 4): отметка дедупа коммитилась в `app.db` до постановки
  envelope в `queue.db`; kill -9 между ними + ре-эмит после рестарта
  (catch-up §9) → «дубль» → событие потеряно — нарушение at-least-once
  §7.1 и SC-2. Worker/delivery устроены правильно (outbox строго до
  ack — C-9); дыра только на входе.
  **Resolved 2026-06-12: подтверждено Владимиром** — фикс в M2
  (отступление от FR-13 вшито туда же): аддитивный
  `DedupStorePort.seen()` (чистое чтение), ingest проверяет `seen()`
  на входе, отметку `mark_inbound()` пишет строго после fan-out
  (и на unrouted-ветке). Kill в новом окне «put → отметка» даёт
  повторную постановку envelope при ре-эмите: реестр переписывается
  идемпотентно (`unchanged`), дубль обработки гасится outbox'ом
  (insert-if-absent) — дубль вместо потери. Контракт
  `DedupStoreContract` дополнен `seen()` (FR-10), реализации
  Memory/Sqlite — аддитивно. ADR — в `DECISIONS.md`.
