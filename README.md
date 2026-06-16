# angarion

[![PyPI](https://img.shields.io/pypi/v/angarion)](https://pypi.org/project/angarion/)
[![CI](https://github.com/vlakir/angarion/actions/workflows/ci.yml/badge.svg)](https://github.com/vlakir/angarion/actions/workflows/ci.yml)
[![Python](https://img.shields.io/pypi/pyversions/angarion)](https://pypi.org/project/angarion/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Универсальная Python-библиотека для построения конвейеров обработки
**событий сообщений**:

```
N источников → нормализация в события → очередь → обработка → M получателей
                                            ↘ аналитика в БД
```

Событие — первоклассная сущность: **новое сообщение** (включая ответы),
**редактирование**, **удаление**. Редактирования и удаления отслеживаются
надёжно — в том числе произошедшие за время простоя приложения
(catch-up по реестру сообщений).

> **Статус: pre-alpha.** Версия 0.1.0 — каркас, опубликована для
> резервирования имени. Ядро (M1) и персистентность (M2) готовы;
> следующий этап — Telegram-адаптер (M3). Публичный API нестабилен
> до 1.0 — следите за [CHANGELOG](CHANGELOG.md).

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
  только прогон на старте).
- **Multicast-пайплайны**: один источник может питать несколько
  независимых конвейеров с изолированными ретраями и DLQ.
- **Процессоры-плагины**: пользовательская логика (включая LLM) с
  опциональным персистентным состоянием; регистрация через entry points.
- **Web API + Web UI** (FastAPI, SSR на Jinja2 + htmx): встроенная
  диагностика, журнал аналитики, административные операции; расширяется
  пользовательскими ручками и страницами через DI поверх портов.
- **Первый адаптер — Telegram** (Telethon, пользовательские аккаунты,
  MTProto): закрытые группы, мультиаккаунт, троттлинг, FloodWait.

Полное техническое задание — [CONCEPT.md](CONCEPT.md) (immutable,
v2.1); принятые архитектурные решения — [DECISIONS.md](DECISIONS.md).

## Установка

```bash
pip install angarion        # или: uv add angarion
```

Telegram-адаптер требует extra: `uv add "angarion[telegram,sqlite]"`.

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

## Дорожная карта

| Этап | Содержимое | Статус |
|---|---|---|
| M1 | Домен, порты, application-слой, InMemory-адаптеры, конфиг | ✅ готов |
| M2 | Персистентность: persist-queue, SQLAlchemy + Alembic, DLQ, `angarion.testing` | ✅ готов |
| M3 | Telegram-адаптер (Telethon): live + catch-up + sender, CLI | ✅ готов |
| M4 | LLM-процессор + `template`-процессор | ✅ готов |
| M5 | Web API + Web UI + Auth + админ-операции | ✅ готов |
| M6 | Интеграционный тестовый контур на реальном аккаунте | ✅ готов |
| M7 | Медиа; второй мессенджер (Matrix) | ⏳ |

Текущая очередь задач — [BOARD.md](BOARD.md) и [BACKLOG.md](BACKLOG.md).

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
  `event_at`, вложенные `source.chat_id` и др.; полный список полей —
  `CONCEPT.md` §4, модель `InboundEvent`); `None` → пустая строка, без
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
(сверка `chat_id` по числовому id).

Session-файл — полноценные учётные данные аккаунта: живёт в `sessions/`
(git-ignored), никогда не коммитится и не копируется в открытые места.

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
