# Web API и UI

angarion включает FastAPI как второй driving-адаптер (§12.5/§12.6): REST
API, серверный (SSR) Web UI на Jinja2 + htmx, граф топологии, журнал
аналитики и админ-операции. Расширяется пользовательскими ручками и
страницами через DI поверх портов — пользователь дотягивается до системы
только через порты, не зная про ORM/сессии/адаптеры.

Пакет — в extra `angarion[web]`; ядро остаётся fastapi-free (§14.9).

## Запуск

Web поднимается ролью процесса (см. [Деплой → роли](deploy.md#роли-процессов)):

```bash
uv add "angarion[web,sqlite]"
uv run angarion run --config app.toml --with-api   # конвейер + API в одном процессе
# либо отдельный web-процесс:
uv run angarion run --config app.toml --role api
```

Хост/порт/аутентификация — секция `[api]` конфига:

```toml
[api]
host = "127.0.0.1"
port = 8000
auth = "users"          # "none" — открыто (dev/localhost); "users" — fastapi-users
# secret подаётся через env, не в TOML
```

## Встроенные эндпоинты

| Путь | Назначение | Доступ |
|---|---|---|
| `GET /api/v1/health` | liveness | публичный |
| `GET /api/v1/diagnostics` | состояние конвейера, курсоры | защищённый |
| `GET /api/v1/events` | журнал аналитики (JSON) | защищённый |
| `GET /ui` | дашборд | защищённый |
| `GET /ui/pipelines` | граф топологии «источники → пайплайны → получатели» (серверный SVG) | защищённый |
| `GET /ui/events` | журнал аналитики (SSR) | защищённый |
| `/api/v1/admin/*`, `/ui/settings`, `/ui/dlq`, `/ui/users` | админ-операции (пауза/resume, requeue DLQ, динамика, пользователи) | admin-only |
| `POST /api/v1/trigger`, `POST /api/v1/run/{pipeline}` | ручной триггер: впрыск события / прямой запуск пайплайна (write) | API-ключ |
| `GET/POST /ui/trigger` | UI-форма ручного триггера | admin-only |
| `/api/v1/auth/*`, `/ui/login`, `/ui/register` | аутентификация | публичный |

Граф `/ui/pipelines` рендерится **на сервере** (Jinja, без JS-библиотек):
цвет узла = статус (активен / пауза из `runtime_config` / failed за час),
аннотации delivered/depth; клик по узлу (admin) — htmx pause/resume.
Цепочки пайплайнов через внутренний провод (транспорт `internal`,
см. README → «Цепочки пайплайнов») рисуются ребром pipeline→pipeline —
внутренний канал схлопнут в одну связь и визуально отличим от обычных
рёбер источник/получатель.

## Своя JSON-ручка

Соберите `APIRouter` и попросите нужный порт типизированной зависимостью
(`AnalyticsDep`, `RegistryDep`, `StateDep`, …) — FastAPI подставит его из
`app.state`. Роутер передаётся в `create_app(routers=[...])` и
монтируется **под `CurrentUser`** (как встроенный `/api/v1`).

```python
from fastapi import APIRouter
from angarion.adapters.http.deps import AnalyticsDep

router = APIRouter(prefix="/api/v1/ext", tags=["ext"])


@router.get("/event-count")
async def event_count(analytics: AnalyticsDep) -> dict[str, int]:
    rows = await analytics.recent(limit=1000)
    return {"count": len(rows)}
```

Доступные зависимости — [`angarion.adapters.http.deps`](../reference/web.md#типизированные-di-зависимости):
`AnalyticsDep`, `RegistryDep`, `StateDep`, `QueueDep`, `CursorsDep`,
`RuntimeConfigDep`, `CommandOutboxDep`, `DeadLettersDep`, `NotifierDep`.

## Своя UI-страница

Страница — дескриптор [`Page`](../reference/web.md) (роутер + пункт
навигации). Шаблон наследует `angarion/base.html` и получает навигацию,
стили и htmx-автообновление бесплатно; рендер — через
`request.app.state.templates`.

```python
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from angarion.adapters.http import Page
from angarion.adapters.http.deps import AnalyticsDep

router = APIRouter()


@router.get("/ui/ext", response_class=HTMLResponse)
async def ext_page(request: Request, analytics: AnalyticsDep) -> HTMLResponse:
    rows = await analytics.recent(limit=20)
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request, "ext/page.html", {"rows": rows}
    )


from pathlib import Path

ext_page_descriptor = Page(
    title="Расширение",
    path="/ui/ext",
    router=router,
    template_dirs=(Path("templates"),),
)
```

```jinja
{# templates/ext/page.html #}
{% extends "angarion/base.html" %}
{% block content %}
  <h1>Моё расширение</h1>
  <ul>{% for r in rows %}<li>{{ r }}</li>{% endfor %}</ul>
{% endblock %}
```

Соберите приложение, передав страницу:

```python
from angarion.adapters.http import create_app

app = create_app(deps, pages=[ext_page_descriptor])
```

`title` / `path` автоматически появляются в шапке навигации. Каталог
шаблонов страница несёт сама в `Page.template_dirs` (T036) — `create_app`
подмешивает его в общий `ChoiceLoader`. Альтернатива — общий для нескольких
страниц каталог через `create_app(template_dirs=[...])`. Каталоги
пользователя ищутся раньше встроенных — любой встроенный шаблон можно
переопределить.

### Через entry point — без своего лаунчера (T029)

Если расширение — только страница (без своей JSON-ручки), её не обязательно
монтировать собственным `create_app`. Зарегистрируйте `Page` в entry-point-
группе `angarion.pages` (симметрично `angarion.processors` /
`angarion.adapters`), и `angarion run --with-api` (а также `--role api`)
подхватит её сам:

```toml
# pyproject.toml вашего пакета-расширения
[project.entry-points."angarion.pages"]
ext = "my_ext.pages:ext_page_descriptor"   # значение — объект Page
```

Раннер резолвит зарегистрированные `Page` (`load_pages()`), сортирует по
`path` для стабильной навигации и передаёт в `create_app`. Каждый entry
point обязан загружаться в `Page` — иначе `ConfigError`; недоступный extra
(C-1) пропускается с предупреждением, не роняя запуск.

> **Шаблоны (T036).** Каталог шаблонов страница несёт сама в
> `Page.template_dirs` — раннер передаёт его в `create_app`, и
> entry-point-страница наследует `angarion/base.html` через общий
> `request.app.state.templates` без собственного лаунчера:
>
> ```python
> from pathlib import Path
> ext_page_descriptor = Page(
>     title="Расширение",
>     path="/ui/ext",
>     router=router,
>     template_dirs=(Path(__file__).parent / "templates",),
> )
> ```
>
> Каталог из `template_dirs` ищется раньше встроенных (`ChoiceLoader`).
> Без `template_dirs` страница обязана рендерить самодостаточно
> (`HTMLResponse` напрямую либо собственный `Jinja2Templates`).

## Аутентификация

- **`auth = "none"`** — все роутеры открыты; `CurrentUser` / `AdminUser`
  резолвятся в синтетического локального админа. Режим для dev/localhost;
  при bind не на `127.0.0.1` — громкое предупреждение в лог.
- **`auth = "users"`** — fastapi-users (§12.7): JWT/cookie-вход, роли
  `admin` / `viewer`, регистрация с одобрением админом. Требует
  `[api].secret` (env) и user store (`app.db`). Пользовательские ручки и
  страницы по умолчанию закрыты `CurrentUser`.

Страницу можно осознанно открыть — `Page(..., public=True)` (например,
своя страница входа). И ручки из `routers=[...]`, и страницы наследуют
выбранный режим `auth`: при `auth = "none"` они открыты (локальный
синтетический админ), при `auth = "users"` — требуют аутентификации
(`public=True` оставляет страницу открытой и в этом режиме).

## Ручной триггер (T038)

Первое штатное **write**-расширение встроенного API помимо admin-
управления (§12.5): ручной запуск обработки без живого события источника.
Две ручки под `/api/v1`, обе принимают тело `{"event": …}` (упрощённый
payload) **или** `{"record": …}` (готовый `Record`) — ровно одно из двух:

| Путь | Семантика | Доступ |
|---|---|---|
| `POST /api/v1/trigger` | **event** — впрыск через `IngestService` → router/dedup/реестр/fan-out | API-ключ |
| `POST /api/v1/run/{pipeline}` | **pipeline** — сырой `QueueEnvelope` в очередь, минуя router/dedup | API-ключ |

Ручные события помечаются `origin='manual'` (аналитика/трасса, `/ui/events`).
Ответ — `202` с `record_uid` и `mode`: `ingested` (combined event), `queued`
(split event через outbox) или `staged` (прямой запуск пайплайна).

### Авторизация — отдельный API-ключ

Write-ручка защищена **не** admin-сессией fastapi-users, а отдельным
**API-ключом** — машинный путь для служб/CI (ключ переиспользуем, в отличие
от сессии). Ключ — секрет `[api].trigger_token`, подаётся через env
`ANGARION_API__TRIGGER_TOKEN` (не в TOML), проверяется на каждый запрос в
заголовке `X-API-Key` (сравнение постоянного времени):

- пустой `trigger_token` → ручки выключены (`503`);
- нет заголовка → `401`; неверный ключ → `403`.

Авторизация ключом **не зависит** от режима `[api].auth` — машинная ручка
работает и при `auth = "none"`, и при `auth = "users"`.

```bash
curl -sS -X POST http://127.0.0.1:8000/api/v1/trigger \
  -H "X-API-Key: $ANGARION_API__TRIGGER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"event": {"source": {"transport": "telegram", "address": "-100…"},
                 "text": "manual event"}}'
```

### Combined vs split

- **combined** (`run --with-api`): event-ручка зовёт `ingest` напрямую
  (меньше латентность), прямой запуск — `queue.put`.
- **split** (`run --role api`): у api-процесса нет конвейера, поэтому
  event-ручка кладёт команду `INJECT` в `CommandOutbox` — исполняет
  consumer pipeline-процесса (мост §12.9, как `restart`/`catchup`). Прямой
  запуск пайплайна идёт через **общую очередь** в обоих режимах (api-процесс
  уже пишет в общий `queue.db`, как requeue из DLQ) — отдельной команды не
  требует. Инвариант «api-процесс без конвейера» сохранён.

### Идемпотентность

Опциональный клиентский `idempotency_key` в payload → детерминированный
`dedup_key`: повтор гасится штатным `dedup.seen()`. Это **свойство
event-пути** — прямой запуск пайплайна минует dedup, там повтор всегда
обрабатывается заново. Без ключа каждый вызов уникален (свежий `uid`).

### UI-форма

Под admin-сессией доступна форма `/ui/trigger` (htmx): source, текст, kind,
выбор пайплайна (пусто = впрыск события по source; имя = прямой запуск). В
отличие от машинной ручки, UI-кнопка завязана на admin-сессию, поэтому
требует `auth = "users"` (либо `auth = "none"` для dev — синтетический
локальный админ). Программный путь и ключ-ручка от режима auth независимы.

### Программный API

Те же два пути — без HTTP, прямо в коде, на `AngarionApp`:

```python
from angarion import ManualEvent
from angarion.domain.models import Endpoint

source = Endpoint(transport="telegram", address="-100…")
await app.submit_event(ManualEvent(source=source, text="event"))      # по source
await app.run_pipeline("forward", ManualEvent(source=source, text="direct"))
```

Публичная фабрика `build_manual_record(ManualEvent(...))` строит валидный
`Record` (ключи, `origin='manual'`) без ручной сборки; экспортируется из
пакета `angarion` вместе с `ManualEvent`. Рабочий пример обеих поверхностей
— каталог [`examples/trigger/`](https://github.com/vlakir/angarion/tree/main/examples).

## Контейнер портов

`create_app` принимает [`AngarionDeps`](../reference/web.md) — контейнер
driven-портов из composition root. Собирается из хранилища и очереди
фабрикой `build_web_deps`:

```python
from angarion.adapters.http import build_web_deps, build_settings_notifier, create_app

deps = build_web_deps(
    settings,
    storage,
    queue,
    notifier=build_settings_notifier(),
)
app = create_app(deps, routers=[router], pages=[ext_page_descriptor])
```

Рабочий пример запуска с кастомной ручкой и страницей — каталог
[`examples/web/`](https://github.com/vlakir/angarion/tree/main/examples)
репозитория.
