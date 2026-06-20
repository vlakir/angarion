# angarion

[![PyPI](https://img.shields.io/pypi/v/angarion)](https://pypi.org/project/angarion/)
[![CI](https://github.com/vlakir/angarion/actions/workflows/ci.yml/badge.svg)](https://github.com/vlakir/angarion/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/angarion)](https://pypi.org/project/angarion/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Универсальный Python-движок **мультиформатных конвейеров данных**:
подключись к чему угодно, обработай как угодно, отдай куда угодно — за
единым унифицированным контрактом вход/выход и гарантией at-least-once.

```
N подключаемых источников любого формата
        → нормализация в унифицированную запись → очередь → обработка
        → M подключаемых приёмников любого формата
                                            ↘ аналитика в БД
```

Запись — первоклассная сущность домена. Для транспортов класса
«мессенджер» она несёт событийную семантику: **новое сообщение** (включая
ответы), **редактирование**, **удаление** — надёжно отслеживаемые, в том
числе за время простоя приложения (catch-up по реестру). Для прочих
транспортов отслеживание правок — опциональная capability адаптера.

> **Статус: pre-alpha.** Реализован первый класс транспорта — **мессенджеры**
> (Telegram, Matrix/E2EE): дорожная карта M1–M7 пройдена (ядро,
> персистентность, LLM/`template`-процессоры, Web API/UI + auth,
> интеграционный контур). **Видение смещается к универсальному
> мультиформатному движку** (брокеры, email, ручной/веб-вход, цепочки
> пайплайнов) — см. [`concepts/v2-multiformat-pipelines.md`](concepts/v2-multiformat-pipelines.md)
> и ADR от 2026-06-19 в [DECISIONS.md](DECISIONS.md). Публичный API
> нестабилен до 1.0 (обобщение модели до `Record`/`transport` — ломающее) —
> следите за [CHANGELOG](CHANGELOG.md).

## Ключевые идеи

- **Гексагональная архитектура (ports & adapters).** Ядро не зависит от
  Telegram, очереди, СУБД и веб-фреймворка. Замена мессенджера, очереди
  или хранилища — добавление адаптера, без изменения ядра.
- **Гарантия at-least-once + идемпотентность** на входе (дедупликация
  событий) и на выходе (идемпотентные ключи доставки) — дубли гасятся,
  доставка не теряется, включая рестарты и повторные catch-up.
- **Catch-up после простоя**: новые сообщения, правки и удаления за время
  оффлайна восстанавливаются по курсорам и реестру сообщений с историей
  версий. Catch-up — это сверка истории источника с реестром по
  хэшам содержимого, поэтому он же служит **детерминированным
  backstop'ом** к неидеальным live-апдейтам мессенджера: что live
  проморгал (особенно правки/удаления), catch-up догоняет. Для боевой
  надёжности рекомендуется включить **периодический** фоновый catch-up —
  `[catchup] interval = <секунды>` (по умолчанию выключен, работает
  только прогон на старте). Глубокий catch-up пере-сканирует историю и на
  больших группах дорог, поэтому есть **лёгкий поллинг недавнего окна**
  (T032): частая дешёвая сверка только последних N сообщений (не старше M
  минут) как backstop правок/удалений — включается per-pipeline
  `[pipelines.*].recent_poll = true`, параметры окна/частоты — в
  `[catchup] recent_*` (Telegram и Matrix).
- **Multicast-пайплайны**: один источник может питать несколько
  независимых конвейеров с изолированными ретраями и DLQ.
- **Процессоры-плагины**: пользовательская логика (включая LLM) с
  опциональным персистентным состоянием; регистрация через entry points.
- **Web API + Web UI** (FastAPI, SSR на Jinja2 + htmx): встроенная
  диагностика, журнал аналитики, административные операции; расширяется
  пользовательскими ручками и страницами через DI поверх портов.
- **Адаптеры мессенджеров — Telegram и Matrix.** Telegram (Telethon,
  пользовательские аккаунты, MTProto): закрытые группы, мультиаккаунт,
  троттлинг, FloodWait. Matrix (`matrix-nio`): комнаты/треды, правки
  (`m.replace`), удаления (redactions), **E2EE-комнаты** (olm/megolm) —
  доказательство переносимости портов на втором боевом адаптере.

Документы видения: [CONCEPT.md](CONCEPT.md) — **v1-baseline** (immutable,
ТЗ конвейеров событий сообщений); актуальное видение —
[`concepts/v2-multiformat-pipelines.md`](concepts/v2-multiformat-pipelines.md)
(мультиформатные конвейеры, pivot 2026-06-19). Принятые архитектурные
решения — [DECISIONS.md](DECISIONS.md).

## Установка

```bash
pip install angarion        # или: uv add angarion
```

Telegram-адаптер требует extra: `uv add "angarion[telegram,sqlite]"`.

Matrix-адаптер: `uv add "angarion[matrix,sqlite]"`. E2EE-комнаты
(olm/megolm) тянут `python-olm`, которому нужна **системная libolm** —
поставь её до установки extra:

```bash
# Debian/Ubuntu
sudo apt install libolm-dev
# Fedora
sudo dnf install libolm-devel
# macOS (Homebrew)
brew install libolm
```

Без libolm пакет `python-olm` не соберётся. E2EE key-store
(olm/megolm) Matrix-адаптер держит на ФС в `[matrix].store_dir`
(по умолчанию `data/matrix-e2e`, git-ignored); токен и `device_id`
аккаунта — в `app.db`, как у Telegram.

Быстрый старт (этап M3):

```bash
export ANGARION_SESSION_KEY=...           # ключ шифрования сессий (Fernet)
uv run angarion migrate --config app.toml # применить миграции БД
uv run angarion login --config app.toml --account main  # авторизовать аккаунт
uv run angarion run --config app.toml     # боевой запуск конвейера
```

`app.toml` — секции `[accounts.*]` / `[storage]` / `[queue]` /
`[pipelines.*]` (пример полей — в [CONCEPT.md](CONCEPT.md) §11);
`api_id`/`api_hash`/`ANGARION_SESSION_KEY` подаются через env, не в TOML.

**Dev/CI без login (T042).** Если у вас уже есть авторизованная
`StringSession`, её можно отдать рантайму прямо из окружения —
`ANGARION_ACCOUNTS__<NAME>__SESSION` (для аккаунта `main` —
`ANGARION_ACCOUNTS__MAIN__SESSION`). Тогда не нужны ни `angarion login`,
ни ключ шифрования: env-сессия подключается напрямую, в обход `app.db`
(приоритетнее сохранённой). Это **dev/CI-путь** — строка сессии это
полноценные учётные данные аккаунта в открытом виде, поэтому держите её
вне TOML (только env, git-ignored) и не используйте как prod-дефолт;
боевое хранение остаётся зашифрованным в `app.db`. Примеры в `examples/`
подхватывают такую сессию автоматически из git-ignored `.secrets`.

## Дорожная карта

| Этап | Содержимое | Статус |
|---|---|---|
| M1 | Домен, порты, application-слой, InMemory-адаптеры, конфиг | ✅ готов |
| M2 | Персистентность: persist-queue, SQLAlchemy + Alembic, DLQ, `angarion.testing` | ✅ готов |
| M3 | Telegram-адаптер (Telethon): live + catch-up + sender, CLI | ✅ готов |
| M4 | LLM-процессор + `template`-процессор | ✅ готов |
| M5 | Web API + Web UI + Auth + админ-операции | ✅ готов |
| M6 | Интеграционный тестовый контур на реальном аккаунте | ✅ готов |
| M7 | Медиа; второй мессенджер (Matrix, E2EE) | ✅ готов |

Текущая очередь задач — [BOARD.md](BOARD.md) и [BACKLOG.md](BACKLOG.md).

## Ограничения платформ (§17.9)

Реестр платформенных ограничений — то, что обусловлено самой платформой,
а не реализацией адаптера:

- **Matrix, E2EE-история до входа устройства (UTD).** Сообщения в
  зашифрованной комнате, отправленные **до** первого `angarion login`
  этого устройства, расшифровать нельзя — ключи megolm-сессии устройству
  не выдавались. Это фундаментальное свойство Matrix E2EE, не баг адаптера:
  такие события (`MegolmEvent` без ключа) пропускаются с пометкой
  `matrix_undecryptable` в аналитике, конвейер не падает. Catch-up по
  истории E2EE-комнаты видит только то, что устройство способно
  расшифровать.
- **Matrix, профиль возможностей** — полный (`user_account`/`edit_events`/
  `delete_events`/`history_fetch`/`threads`, `push_transport="client"`):
  правки через `m.replace`, удаления через redactions, история через
  sync/`/messages`, треды `m.thread`. Деградации по матрице возможностей
  (§12.10) Matrix не вызывает; механизм деградации проверяется на
  адаптере с урезанным профилем.
- **Telegram, удаления на chat-уровне** — update удаления не несёт топика
  (§9.4): топик-удаления live не детектируются.

## Встроенные процессоры

Процессор пайплайна задаётся в `[pipelines.*]`: `processor = "<имя>"`,
параметры — в `[pipelines.*.processor_config]`. Встроенные:

- **`passthrough`** — ретранслирует текст события как есть; `text=None`
  (удаление без восстановления) → drop.
- **`template`** — детерминированно переписывает событие Jinja2-шаблоном
  по его полям; `jinja2` в core, доступен из коробки. `processor_config`:
  `template` (базовый) + опц. `edited`/`deleted` (fallback на базовый).
  Пустой результат рендера → drop. Контекст шаблона — поля события
  (`text`, `previous_text`, `kind`, `origin`, `sender_name`, `external_id`,
  `event_at`, вложенные `source.address` и др.; полный список полей —
  `CONCEPT.md` §4, модель `Record`); `None` → пустая строка, без
  HTML-экранирования.
- **`llm`** — обрабатывает текст через OpenAI-совместимую модель
  (Ollama / LM Studio / vLLM / облако). Требует extra:
  `uv add "angarion[llm]"`. `processor_config`:

  ```toml
  [pipelines.summarize.processor_config]
  base_url = "http://localhost:11434/v1"   # OpenAI-совместимый endpoint
  model = "qwen2.5:3b"
  system_prompt = "Ты — редактор. Делай краткую выжимку."
  user_prompt = "Сократи до одного абзаца:\n\n{{ text }}"
  # user_prompt_edited / user_prompt_deleted — опц. пер-видовые промпты
  # api_key_env = "OPENAI_API_KEY"  # ИМЯ env-переменной с ключом (не сам ключ);
  #                                 # без поля — запрос без авторизации (локальные модели)
  timeout_s = 60
  max_attempts = 3
  # temperature / max_tokens — опц.
  ```

  Промпты — те же Jinja2-поля события, что у `template`. Ключ берётся из
  env по имени `api_key_env` — **в TOML секрет не пишется** (§17.7).
  Сеть/`5xx`/`429` ретраятся (уважая `Retry-After`); `text=None` и пустой
  ответ модели → drop. Под at-least-once повторная обработка может вызвать
  модель повторно (иной ответ, расход токенов) — дублирующую доставку
  гасит идемпотентность выхода.

Пользовательские процессоры регистрируются через entry point
`angarion.processors` (та же механика, что у встроенных).

## Медиа (M7, часть A)

Вложения событий — первоклассные: `Record.media: list[MediaRef]`
(тип/имя/размер/mime/размерности), `has_media` — производное от `media`.
`passthrough` переносит вложения транзитом, поэтому **пересылка медиа
работает из коробки** (текст + фото/видео/документ доходят в цель).

Доставка вложения на send-стороне — два пути:

- **Fast-path по ссылке (по умолчанию):** Telegram-sender переотправляет
  медиа, рефетча исходное сообщение тем же аккаунтом (`file_id` обновляется
  при рефетче) — **диск не используется**, лишнего скачивания нет.
- **Скачивание (opt-in):** при `[media] download = true` принимающий аккаунт
  скачивает подходящие вложения при ingest в git-ignored каталог и проставляет
  `MediaRef.local_path`. Это нужно для **кросс-аккаунт/кросс-платформа**
  доставки (когда рефетч недоступен) и даёт **процессорам доступ к контенту**
  по локальному пути.

Политика — глобальная секция `[media]` (всё опционально, по умолчанию не
скачиваем):

```toml
[media]
download = true                  # opt-in; по умолчанию false (только метаданные)
allowed_kinds = ["photo", "video", "document"]  # пусто = все виды
max_size = 52428800              # байт; 0 = без лимита
storage_dir = "angarion-data/media"  # git-ignored рантайм-каталог
retention_days = 7               # 0 = бессрочно; чистится фоновой prune-задачей
```

Скачивание best-effort: сбой границы платформы деградирует до «только
метаданные», событие не теряется. Переопределение пересылки медиа
**по пайплайну** — отдельная задача (BACKLOG T033).

## Цепочки пайплайнов (внутренний провод)

Пайплайны соединяются в цепочку — выход одного становится входом другого —
через **внутренний транспорт `internal`**, без служебной группы-посредника
на реальной платформе. Объявляется обычным аккаунтом с `transport =
"internal"`; ребро цепочки — совпадение `(transport=internal, address)` у
`target` одного пайплайна и `source` другого. Канал (`address`) —
произвольное имя.

```toml
[accounts.wire]
transport = "internal"           # синтетический транспорт, без секретов/сессии

[chains]
max_hops = 10                    # рантайм-лимит прыжков по внутренним рёбрам

[pipelines.stage1]               # звено 1: пишет во внутренний канал
processor = "template"
events = ["new"]
sources = [{ account = "main", address = "-100AAAAAAAAAA" }]
targets = [{ account = "wire", address = "stage1" }]

[pipelines.stage2]               # звено 2: слушает тот же канал
processor = "template"
events = ["new"]
sources = [{ account = "wire", address = "stage1" }]
targets = [{ account = "main", address = "-100BBBBBBBBBB" }]
```

Внутренний sink не доставляет наружу: он преобразует исходящую запись
обратно во входную (`Record(kind=new)`) и подаёт в штатный конвейер
(re-ingestion). Свойства:

- **At-least-once без дублей** — ключи стыка детерминированы из
  `idempotency_key`; повтор доставки ребра гасит дедуп на входе приёмника.
- **Защита от циклов двухуровневая** — циклическая цепочка (`stage1 →
  stage2 → stage1`) отклоняется на старте (fail-fast DAG-валидация); цикл
  через реальную платформу ловит рантайм-лимит `[chains] max_hops` (запись
  → DLQ при превышении).
- **Только `new`** — внутренний транспорт декларирует capability `new`;
  re-ingested запись всегда `new`. Подписка приёмника на `edited`/`deleted`
  отклоняется на старте (проброс правок сквозь ребро — вне MVP).
- **Сквозная трассировка** — `trace_id` (корень цепочки) пробрасывается
  через звенья; внутренние записи помечены `origin='internal'` (рядом с
  `live`/`catchup`), доступно в шаблонах/аналитике.
- **Split-деплой** — re-ingestion идёт через очередь, звенья могут жить в
  разных процессах (`--role api` / `pipeline`).

Топология цепочек видна в `/ui/pipelines`: внутреннее ребро рисуется
связью pipeline→pipeline, отличимой от обычных source/target. Fan-out
(выход → входы нескольких) и fan-in следуют из модели бесплатно
(`sources`/`targets` множественные). Полный пример — [`examples/chain/`](examples/chain/README.md).

## Разработка

Менеджер зависимостей и окружения — [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync                       # поставить зависимости (.venv)
```

Методика — **TDD** (тест пишется до кода), покрытие ≥ 90%. Перед каждым
push обязательны четыре проверки с нулём ошибок:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
```

Прямой push в `main` запрещён (есть `hooks/pre-push` — установи его:
`cp hooks/pre-push .git/hooks/ && chmod +x .git/hooks/pre-push`).
Изменения — только через PR (один PR = один squash-коммит).

### Интеграционные тесты (реальный Telegram)

По умолчанию пропускаются (`-m 'not integration'` в конфиге pytest).
Контур §13.2 ТЗ гоняет **весь** пайплайн на реальном аккаунте и **трёх**
тестовых группах: live new/edited/deleted, multicast, catch-up после
простоя, дедуп при повторном catch-up, рестарт с непустой очередью,
петлевой guard `source == target`. Тесты прибирают за собой.

Нужны реквизиты в git-ignored файле `.secrets` (образец —
`.secrets.example`: api_id/api_hash с <https://my.telegram.org>, три
тестовые группы — A источник, B/C цели) и одноразовая интерактивная
авторизация:

```bash
cp .secrets.example .secrets            # заполнить значениями (3 группы)
uv run python scripts/tg_login.py       # спросит код, создаст session
uv run pytest -m integration            # запуск интеграционного набора
```

Темп — с запасом под лимиты Telegram (троттлинг + ожидание доставки),
поэтому набор идёт ощутимо дольше unit-тестов. Рекомендованы супергруппы
(id вида `-100…`) — целевой кейс (§9.4) и условие работы петлевого guard
(сверка `address` по числовому id).

Session-файл — полноценные учётные данные аккаунта: живёт в `sessions/`
(git-ignored), никогда не коммитится и не копируется в открытые места.

### Публикация на PyPI

Релиз собирается и выкладывается через `scripts/publish.sh` (`uv build`
→ `twine check` → `uv publish`). Токен берётся из git-ignored `.secrets`
(`PYPI_TOKEN`; образец — `.secrets.example`):

```bash
cp .secrets.example .secrets            # заполнить PYPI_TOKEN
scripts/publish.sh                      # сборка + проверка + загрузка на PyPI
scripts/publish.sh --test               # то же на TestPyPI (PYPI_TEST_TOKEN)
```

Токен — project-scoped (scope «Project: angarion»); первая публикация
требует account-scoped, после неё переключаемся на project-scoped.

## Бэкапы

Гайд §17.6 ТЗ с поправкой на хранение сессий в БД (ADR 2026-06-13 в
[DECISIONS.md](DECISIONS.md)). Бэкапить:

- **`app.db`** — горячая копия через SQLite backup API / `VACUUM INTO`,
  **не** файловым копированием под нагрузкой (для непрерывной репликации
  — Litestream). Содержит реестр сообщений, курсоры, аналитику, outbox
  **и зашифрованные сессии аккаунтов** (`StringSession`).
- **`ANGARION_SESSION_KEY`** — ключ шифрования сессий; хранить и
  бэкапить **отдельно** от `app.db`. Ключ рядом с БД обесценивает
  шифрование; потеря ключа = бэкап сессий бесполезен (потребуется
  повторный `angarion login` для каждого аккаунта).
- **TOML-конфиг** — параметры пайплайнов и аккаунтов (без секретов:
  `api_id`/`api_hash`/`ANGARION_SESSION_KEY` — из env).
- **`queue.db`** — по выбору: потеря очереди при живом catch-up
  восстановима для сообщений в окне реестра (`registry_window_days`).
- **E2EE key-store Matrix** (`[matrix].store_dir`, по умолчанию
  `data/matrix-e2e/`) — **только** для Matrix-аккаунтов: отдельный sqlite
  с olm/megolm-ключами устройства, который ведёт `matrix-nio` (единственное
  отступление от «вся сессия в `app.db`», ADR 2026-06-18). Бэкапить вместе с
  `app.db`; при потере — re-login (`angarion login`) перевыпустит устройство,
  но история E2EE-комнат, зашифрованная на старое устройство, станет
  недоступна (UTD). Каталог git-ignored.

Файловых session-файлов у боевого адаптера нет — сессия живёт в `app.db`
(отступление от §11/§12.1/§17.6 ТЗ, ADR 2026-06-13). Файл
`sessions/*.session` остаётся только у интеграционного контура тестов
(см. ниже) и в бэкап боевого деплоя не входит.

## Структура проекта

- `src/angarion/` — корень исходников пакета.
- `tests/` — тесты (unit/контрактные; интеграционные — по маркеру).
- `CONCEPT.md` — техническое задание (immutable, точка опоры).
- `DECISIONS.md` — архитектурные решения с обоснованиями (ADR-Lite).
- `BOARD.md` / `BACKLOG.md` — kanban-доска и парковка идей.
- `CHANGELOG.md` — журнал изменений (Keep a Changelog).
- `specs/` — спецификации крупных фич.
- `CLAUDE.md` — проектные правила для Claude (Claude Code).

## Методика работы

Проект создан из шаблона
[vlakir/dreamteam](https://github.com/vlakir/dreamteam): scope
discipline, ритуал spec/clarify/analyze для крупных фич, обязательный
code review каждого PR, pre-push контроль качества. Подробности — в
репозитории шаблона.

## Лицензия

[MIT](LICENSE).
