# Быстрый старт

## Установка

```bash
pip install angarion        # или: uv add angarion
```

Адаптеры и хранилища подключаются через extra. Для боевого Telegram-
конвейера с SQLite-хранилищем:

```bash
uv add "angarion[telegram,sqlite]"
```

Доступные extra: `telegram`, `sqlite`, `persistqueue`, `llm`, `web`,
`testing` (см. [pyproject](https://github.com/vlakir/angarion/blob/main/pyproject.toml)).

## Конфигурация

Конвейер описывается TOML-файлом — секции `[accounts.*]` / `[storage]` /
`[queue]` / `[pipelines.*]` (полный список полей — `CONCEPT.md` §11):

```toml
[storage]
backend = "sqlite"
dsn = "app.db"

[queue]
backend = "persistqueue"
path = "queue.db"

[accounts.main]
messenger = "telegram"
# api_id / api_hash — через env, не в TOML (§17.7)

[pipelines.relay]
source = { account = "main", chat_id = -1001234567890 }
processor = "passthrough"
sink = { account = "main", chat_id = -1009876543210 }
```

!!! danger "Секреты — только через env"
    `api_id` / `api_hash` / `ANGARION_SESSION_KEY` и ключи LLM подаются
    переменными окружения, **никогда** в TOML. Подробнее — в
    [гайде по деплою](guides/deploy.md).

## Первый запуск

```bash
export ANGARION_SESSION_KEY=...           # ключ шифрования сессий (Fernet)
uv run angarion migrate --config app.toml # применить миграции БД
uv run angarion login --config app.toml --account main  # авторизовать аккаунт
uv run angarion run --config app.toml     # боевой запуск конвейера
```

`ANGARION_SESSION_KEY` — Fernet-ключ шифрования `StringSession`
аккаунтов at-rest. Храните и бэкапьте его **отдельно** от `app.db`
(см. [деплой → бэкапы](guides/deploy.md#бэкапы)).

## Встроенные процессоры

Процессор пайплайна задаётся в `[pipelines.*]`: `processor = "<имя>"`,
параметры — в `[pipelines.*.processor_config]`.

- **`passthrough`** — ретранслирует текст события как есть; `text=None`
  (удаление без восстановления) → drop.
- **`template`** — детерминированно переписывает событие Jinja2-шаблоном
  по его полям (`jinja2` в core, доступен из коробки). `processor_config`:
  `template` (базовый) + опц. `edited` / `deleted`. Пустой результат
  рендера → drop.
- **`llm`** — обрабатывает текст OpenAI-совместимой моделью (Ollama /
  LM Studio / vLLM / облако). Требует `uv add "angarion[llm]"`.

Свой процессор регистрируется через entry point `angarion.processors` —
см. [гайд автору плагина](guides/plugin-authoring.md).

## Что дальше

- Запуск со встроенным Web API и UI — `--with-api` / `--role`, см.
  [Web API и UI](guides/web-api.md).
- Прод-развёртывание (systemd, reverse proxy, бэкапы) —
  [Деплой](guides/deploy.md).
- Рабочие примеры — каталог
  [`examples/`](https://github.com/vlakir/angarion/tree/main/examples)
  репозитория.
