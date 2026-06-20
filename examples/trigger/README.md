# Пример: ручной запуск пайплайна (T038)

Тот же passthrough A→B, что в [`../forward`](../forward/README.md), но
обработку инициирует **не живое сообщение** в группе A, а **ручной
триггер**. Три поверхности одной фичи:

1. **Программный API** — `AngarionApp.submit_event` / `run_pipeline`
   (встраиваем библиотеку в свой код, [`inject.py`](inject.py)).
2. **HTTP-ручка** — `POST /api/v1/trigger` (событие) и
   `POST /api/v1/run/{pipeline}` (прямой запуск) под отдельным API-ключом
   (для служб/CI; curl ниже).
3. **UI-кнопка** — форма `/ui/trigger` под admin-сессией (для оператора).

Все ручные события помечаются `origin='manual'` — видно в `/ui/events`.

## Два пути обработки

- **event** (`submit_event` / `POST /api/v1/trigger`) — запись идёт
  штатным путём: маршрутизация по `source` → dedup → реестр → fan-out.
  Событие попадает в пайплайн, у которого этот `source` в `sources`.
  Опциональный `idempotency_key` гасит повтор (штатный dedup).
- **pipeline** (`run_pipeline` / `POST /api/v1/run/{pipeline}`) — запись
  ставится в **именованный** пайплайн напрямую, **минуя** маршрутизацию и
  dedup. Изолированный прогон конкретного пайплайна; идемпотентность здесь
  не гарантируется.

## Что нужно

- Установленный проект с extra:
  `uv sync --extra telegram --extra sqlite --extra web`
  (или `uv add "angarion[telegram,sqlite,web]"` во внешнем проекте).
- Telegram-аккаунт, **состоящий в обеих группах** A и B.
- `api_id` / `api_hash` — с <https://my.telegram.org> → API development
  tools.
- `id` групп (вида `-100…`) — например, переслав сообщение из группы боту
  `@username_to_id_bot`, либо из клиента.

## Настройка

1. Подставь `id` групп A/B в [`app.toml`](app.toml) (`sources` / `targets`).
2. Задай секреты в окружении (не в файлах — не должны попасть в git):

   ```bash
   export ANGARION_ACCOUNTS__MAIN__API_ID=12345
   export ANGARION_ACCOUNTS__MAIN__API_HASH=0123456789abcdef0123456789abcdef
   # API-ключ HTTP-ручки триггера (run.sh подставит demo-токен, если не задан):
   export ANGARION_API__TRIGGER_TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(24))")
   ```

> Dev (есть git-ignored `.secrets` в корне репо): креды, группы A/B и
> сессия подхватываются автоматически — ни export, ни `login` не нужны.

## Запуск

### A. Combined-сервер (HTTP-ручка + UI)

```bash
examples/trigger/run.sh
```

Поднимает конвейер + Web-адаптер в одном процессе (`angarion run
--with-api`). Дальше — из **другого терминала** триггери обработку.

**HTTP — впрыск события** (маршрутизируется по `source`; подставь `address`
группы A):

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/trigger \
  -H "X-API-Key: $ANGARION_API__TRIGGER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"event": {"source": {"transport": "telegram", "address": "-100AAAAAAAAAA"},
                 "text": "Привет из HTTP-триггера (event)"}}'
```

**HTTP — прямой запуск пайплайна** (минуя маршрутизацию; `source` не важен):

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/run/forward \
  -H "X-API-Key: $ANGARION_API__TRIGGER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"event": {"source": {"transport": "telegram", "address": "-100AAAAAAAAAA"},
                 "text": "Привет из HTTP-триггера (run forward)"}}'
```

Ответ — `202` с `record_uid` и `mode` (`ingested` / `queued` / `staged`).
Без заголовка `X-API-Key` → `401`, с неверным → `403`, при пустом
`trigger_token` ручки выключены → `503`.

**Идемпотентность** (только event-путь): повтор с тем же
`idempotency_key` не порождает второй обработки —

```bash
-d '{"event": {"source": {"transport": "telegram", "address": "-100AAAAAAAAAA"},
               "text": "однажды", "idempotency_key": "demo-key-1"}}'
```

**UI** — открой <http://127.0.0.1:8000/ui/trigger>: форма (source, text,
kind, выбор пайплайна) триггерит ту же обработку под admin-сессией. В этом
примере `auth = "none"` (dev) — UI открыт под синтетическим локальным
админом; в бою (`auth = "users"`) нужна admin-сессия.

### B. Программный впрыск (встраивание в свой код)

```bash
examples/trigger/run.sh inject
```

Запускает [`inject.py`](inject.py): собирает конвейер из `app.toml`,
поднимает его, делает `submit_event` (event) и `run_pipeline` (direct),
ждёт доставки и выходит. Оба сообщения прилетают в группу B. Это
самодостаточный вариант запуска — **не** одновременно с сервером (у каждого
свой in-memory-конвейер).

Суть программного API — пара строк:

```python
from angarion import ManualEvent
from angarion.domain.models import Endpoint

source = Endpoint(transport="telegram", address="-100AAAAAAAAAA")
await app.submit_event(ManualEvent(source=source, text="привет"))   # по source
await app.run_pipeline("forward", ManualEvent(source=source, text="прямо"))
```

## Combined vs split

В этом примере — **combined** (`--with-api`): HTTP-ручка зовёт
`ingest`/очередь напрямую. В **split** (`--role api`) тот же event-эндпойнт
кладёт команду `INJECT` в `CommandOutbox`, а исполняет её pipeline-процесс
(у api-процесса нет конвейера). Прямой запуск пайплайна (`/run/{pipeline}`)
идёт через общую очередь в обоих режимах. Подробно —
[`docs/guides/web-api.md`](../../docs/guides/web-api.md) → «Ручной триггер».

## Гигиена

Секреты (`api_id`/`api_hash`, `trigger_token`) — только в окружении/
`.secrets`, никогда в `app.toml` и не в git. Рантайм (`angarion-data/`,
ключ сессии) — git-ignored.
