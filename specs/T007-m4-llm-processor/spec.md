# Spec: M4 — LLM-процессор + `template`-процессор

**Статус:** Implemented (3 фазы, T007-m4-llm-processor)
**Дата создания:** 2026-06-13
**Связанные документы:**
- `CONCEPT.md` §10 (процессоры, plugin API), §10.2 (встроенные v1:
  `passthrough`, `template`, LLM — M4), §10.3 (stateful-процессоры),
  §4.4 (контекст процессора), §8 (ошибки/ретраи), §11 (конфиг,
  `processor_config`), §16 (этап M4), §17.7 (маскирование секретов)
- `DECISIONS.md` — ADR «Telegram-runtime конфиг под `[telegram]`»
  (паттерн секретов через env), C-3 (откладывание `template` к M4 —
  docstring `application/processors.py`)
- `specs/T005-m3-telegram/spec.md` — образец тонкой тест-границы над
  внешним I/O (`TelegramClientPort` + fake), tenacity-политика sender'а

---

## 1. Overview

M4 даёт `angarion` первые **трансформирующие** процессоры. Главный —
`llm`: обработка текста события через LLM по OpenAI-совместимому
HTTP-endpoint (`/v1/chat/completions`). Целевой кейс — локальная модель
(Ollama / LM Studio / vLLM / llama.cpp-server), но контракт
OpenAI-совместимый, поэтому работает и с облачными провайдерами. До M4
ядро умело только ретранслировать (`passthrough`); теперь пайплайн
может суммаризировать, переводить, фильтровать и переписывать сообщения
на лету.

Попутно закрывается долг C-3 из M2 — встроенный процессор `template`
(Jinja2 по полям события, отдельные шаблоны для new/edited/deleted),
числящийся в §10.2 как встроенный v1. Оба процессора используют общий
Jinja2-движок рендеринга, поэтому реализуются вместе.

Stateful-процессор «дайджест» (§10.3) и user-facing примеры (`examples/`)
— **отдельная задача T019** (решение Q5; по правилу «примеры как часть
крупной фичи», как T018 для M3). Здесь — только библиотечные процессоры
в `src/`.

## 2. Сценарии использования

- Как **оператор пайплайна**, я хочу указать процессор `llm` с адресом
  локальной модели и промптом, чтобы входящие сообщения
  переписывались/суммаризировались перед отправкой в целевые группы.
- Как **оператор**, я хочу задавать API-ключ модели **не в TOML**, а
  через секрет окружения (имя env-переменной в конфиге) — чтобы конфиг
  можно было хранить в репозитории.
- Как **оператор**, я хочу процессор `template` для простого
  детерминированного переписывания (Jinja2 по полям события), в т.ч.
  своими шаблонами на правки и удаления — без обращения к LLM.
- Как **разработчик библиотеки**, я хочу прогонять логику `llm` и
  `template` в CI **без сети и реального LLM** (HTTP-транспорт под
  тестовым двойником).

## 3. Functional Requirements

**Общее (Jinja2-рендеринг)**
- ДОЛЖНА: общий хелпер рендеринга Jinja2-шаблона по полям `InboundEvent`
  (контекст — поля события: `text`, `previous_text`, `kind`,
  `sender_name`, `sender_id`, `origin`, `external_id`, `event_at` и др.).
- ДОЛЖНА: `None`-поля рендерятся пустой строкой (`finalize`), не строкой
  `"None"`; рендеринг — plain text (без HTML-autoescape).

**`template`-процессор**
- ДОЛЖНА: регистрироваться как встроенный процессор (entry point
  `angarion.processors`), как `passthrough`.
- ДОЛЖНА: брать из `ctx.settings` шаблон(ы): базовый `template` плюс
  опциональные пер-видовые `edited`/`deleted` (fallback на базовый, если
  пер-видовой не задан). Валидируется Pydantic-моделью.
- ДОЛЖНА: рендерить шаблон для вида события → `OutboundMessage` на каждую
  цель; пустой результат рендера → DROP.

**`llm`-процессор**
- ДОЛЖНА: регистрироваться как встроенный процессор (entry point
  `angarion.processors`).
- ДОЛЖНА: читать конфигурацию из `ctx.settings` Pydantic-моделью
  `LlmProcessorConfig`: `base_url`, `model`, `system_prompt`,
  `user_prompt` (+ опциональные пер-видовые `user_prompt` на
  edited/deleted), `api_key_env`, `timeout_s` (default 60),
  параметры генерации (`temperature`, `max_tokens`), `max_attempts`.
- ДОЛЖНА: формировать `messages` (`system` + `user`) рендерингом
  промптов общим Jinja2-хелпером; вызывать
  `POST {base_url}/chat/completions` через `httpx.AsyncClient`; парсить
  `choices[0].message.content` типизированной Pydantic-моделью ответа.
- ДОЛЖНА: ключ авторизации — из env-переменной с именем `api_key_env`
  (Q2); при отсутствии `api_key_env` запрос идёт **без** заголовка
  `Authorization` (локальные модели без auth). Секрет в TOML не пишется.
- ДОЛЖНА (Q7): при `text=None` (DELETED без восстановления) — DROP.
- ДОЛЖНА (Q6): на сетевых ошибках / `5xx` / `429` — короткий ретрай
  внутри процессора (`tenacity`, `max_attempts`, exp-backoff), уважая
  `Retry-After` при `429`; при исчерпании попыток → `ProcessingError`
  (worker делает re-enqueue по §8). `4xx` кроме `429` (например `400`
  плохой запрос, `401` auth) — **не ретраятся**, сразу `ProcessingError`.
- ДОЛЖНА: успешный непустой ответ → `OutboundMessage` на каждую цель
  (idempotency-ключи через `svc.make_idempotency_key`).
- НЕ ДОЛЖНА: писать секреты/ключи в TOML или логи (§17.7).

**Тестируемость**
- ДОЛЖНА: HTTP-вызов LLM идёт через узкую границу (тонкий
  Protocol/класс над `httpx.AsyncClient`), подменяемую в тестах двойником
  или `httpx.MockTransport` — unit-тесты `llm` без сети (образец —
  `TelegramClientPort` из M3).

## 4. Success Criteria

- Acceptance (§16 M4): обработка события через локальную
  OpenAI-совместимую модель работает end-to-end (ручной прогон —
  фиксируется отдельно, как суточный прогон в M3; e2e-проверка примера —
  в T019).
- В CI без сети: unit-тест `llm` под тестовым транспортом даёт корректный
  `OutboundMessage`; таймаут/`5xx`/`429` → ретрай → при исчерпании
  `ProcessingError`; `400`/`401` → `ProcessingError` без ретраев;
  `text=None` → DROP. Unit-тест `template` рендерит new/edited/deleted
  своими шаблонами, пустой рендер → DROP.
- coverage ≥ 90% на новом коде, `mypy --strict` чистый, 4 проверки
  зелёные.

## 5. Key Entities

- **`TemplateProcessorConfig`** — Pydantic-модель `processor_config` для
  `template`: `template` (базовый) + опц. `edited`/`deleted`.
- **`LlmProcessorConfig`** — Pydantic-модель `processor_config` для `llm`
  (поля — см. FR). Парсится и валидируется внутри процессора (см. W1).
- **`ChatMessage` / `ChatCompletionResponse`** — типизированные модели
  запроса/ответа OpenAI-совместимого API (для `mypy --strict` поверх
  `response.json()`).
- HTTP-граница к LLM — тонкий слой над `httpx.AsyncClient`, подменяемый
  в тестах.

## 6. Assumptions & Constraints

- Endpoint — OpenAI-совместимый (`/chat/completions`, схема
  `messages`/`choices`). Несовместимые провайдеры — вне scope.
- Worker конкурентен = 1 и событийно-управляем (§6.3): процессор
  вызывается только на приходящее событие. Долгий honored `Retry-After`
  на `429` блокирует единственный worker — как honored FloodWait у
  sender'а M3; принимается осознанно (см. W5).
- Конфиг оператора доверенный: Jinja2-шаблоны исполняются из конфига
  (см. W4).
- `mypy --strict` поверх `httpx` (типизирован).
- Шаблоны (`jinja2`) и HTTP-клиент (`httpx`) — новые runtime-зависимости.

## 7. Out of Scope

- **Дайджест и примеры** (`examples/llm`, stateful-дайджест) — задача
  **T019** (Q5).
- Стриминг ответа (`stream=true`), tool/function-calling.
- Эмбеддинги/RAG, мультимодальность (vision) — медиа в M7.
- Фоновый шедулер сброса по «настенному» времени без события.
- Балансировка/пул нескольких LLM-endpoint'ов.
- Валидация `processor_config` на старте через хук контракта процессора
  (см. W1) — парковка в BACKLOG, не в M4.

---

## Clarify (заполняется Claude)

### Open questions

- _(все вопросы закрыты — см. Resolved)_

### Resolved (с ответами)

- **Q1 (scope `template`) → включить в T007.** `template` (Jinja2) —
  встроенный v1 (§10.2), отложенный C-3; `llm` всё равно тянет Jinja2 для
  промптов, общий движок → реализуем вместе, закрываем долг C-3.
- **Q2 (секрет ключа) → `api_key_env`.** Поле `api_key_env` в
  `processor_config` называет env-переменную; процессор читает
  `os.environ`. Секрета в TOML нет; пропуск поля → запрос без auth
  (локальные модели). Консистентно с `ANGARION_SESSION_KEY` (M3).
- **Q3 (промпт) → Jinja2 `system_prompt` + `user_prompt`** по полям
  события; опц. пер-видовые `user_prompt` (edited/deleted). Тот же
  Jinja2-движок, что и `template`.
- **Q4 (сброс дайджеста) → оба, настраиваемо (N и/или max-age),** эмиссия
  в `ctx.targets`. Перенесено в **T019** (дайджест — пример). Зафиксировано
  здесь как вход для T019; max-age проверяется на приходящем событии
  (без фонового шедулера, §6.3).
- **Q5 (где дайджест/примеры) → только пример в `examples/`,** отдельной
  задачей **T019**. T007 даёт в `src/` лишь `llm` + `template`.
- **Q6 (ошибки LLM) → tenacity внутри процессора** (`max_attempts`,
  default 3) на сети/`5xx`/`429` с уважением `Retry-After`; исчерпание →
  `ProcessingError` → re-enqueue (§8). `4xx≠429` — без ретраев. Таймаут
  default 60 c. Двухслойность как у sender M3.
- **Q7 (`text=None`) → DROP** (как `passthrough`): суммаризировать
  нечего. (`template` при этом рендерит свой `deleted`-шаблон, если задан.)
- **Q8 (фазы) → 3 фазы, один PR (укрупнённо):** (1) Jinja2-хелпер +
  `template` + конфиг + dep `jinja2`; (2) `llm` (`LlmProcessorConfig`,
  HTTP-граница, OpenAI-вызов, tenacity + маппинг ошибок) + dep `httpx`;
  (3) регистрация обоих entry points в bootstrap + acceptance +
  `CHANGELOG`/`DECISIONS` (ADR) + `README`. Коммит = фаза, `/clear`
  между; merge одним PR. Примеры — отдельный PR (T019).

---

## Analyze (заполняется Claude)

### 🔴 Critical

- _(не выявлено: фича опирается на готовые порты M1/M2 и существующий
  контракт `ProcessorPort`; новых доменных портов не вводит.)_

### 🟡 Warning

- **W1. `processor_config` не валидируется на старте (расхождение с §11
  «fail-fast при старте»).** В `PipelineConfig.processor_config` —
  `dict[str, Any]`; bootstrap не знает, какой Pydantic-моделью его
  проверять (контракт `ProcessorPort` не несёт `config_model`). →
  **Решение для M4:** процессор парсит/валидирует `ctx.settings` лениво и
  кэширует разобранный конфиг по `ctx.pipeline` (worker конкурентен = 1,
  процессор-синглтон); ошибка конфига всплывёт на первом событии как
  `ConfigError`/`ProcessingError`, а не на старте. Startup-валидация
  через хук `config_model` в контракте процессора (по образцу
  `account_config_model` адаптера M3) — отдельной задачей, в BACKLOG
  (см. Out of Scope). Расширять `ProcessorPort` без ADR в рамках M4 не
  будем.
- **W2. Повторный LLM-вызов под at-least-once.** При re-enqueue события
  (ретрай/восстановление) `llm` вызовет модель повторно (расход токенов,
  иной ответ). Дублирующая **доставка** гасится `mark_delivered` (§6.3,
  §7.3): первый доставленный результат побеждает, перегенерированный
  отбрасывается на `mark_delivered`. Принять как осознанное следствие
  at-least-once (как и приблизительность счётчиков §8). → зафиксировать в
  docstring процессора и README.
- **W3. `mypy --strict` поверх `response.json()` (тип `Any`).** Парсить
  ответ типизированной Pydantic-моделью `ChatCompletionResponse`, не
  индексировать сырой dict — иначе strict-ошибки и хрупкость.
- **W4. Исполнение Jinja2-шаблонов из конфига.** Конфиг доверенный
  (оператор), но во избежание сюрпризов — `jinja2.Environment` с
  `autoescape=False` (plain text), `finalize` для `None`→`''`. Песочница
  (`SandboxedEnvironment`) — overkill для доверенного конфига; не вводим.
  Поведение при отсутствующем атрибуте — стандартный `Undefined` (рендер
  пусто через finalize), без `StrictUndefined`, чтобы `previous_text=None`
  у NEW не валил рендер.
- **W5. Долгий honored `Retry-After`/таймаут блокирует единственный
  worker.** Как honored FloodWait у sender'а M3 (§6.3, конкурентность=1).
  Принимается; для защиты — разумный потолок ожидания `Retry-After`
  (например cap = `timeout_s`) и явный `timeout_s` у `httpx`.

### 🟢 Note

- **N1. `template` и `text=None`.** В отличие от `llm` (DROP), `template`
  для DELETED рендерит `deleted`-шаблон (если задан); там поля
  восстановлены реестром (§4.2) либо пусты. Пустой рендер → DROP.
- **N2. `origin` (`live`/`catchup`) доступен в шаблоне/промпте** — автор
  конфига может, например, помечать догнанные события. Документируем в
  README как доступное поле, спец-логики в процессорах не вводим.
- **N3. Маскирование секретов (§17.7).** `api_key` берётся из env и
  кладётся только в заголовок `Authorization`; в логи/аналитику не
  попадает. Проверить, что structlog-маскирование (§17.7) покрывает
  `authorization` (уже в списке ключей маскировщика).
- **N4. Долг C-3.** Реализовав `template`, убрать из docstring
  `application/processors.py` фразу «`template` … отложен к M4 (C-3)».
- **N5. Локальная модель для acceptance/демо.** Ручной прогон §16 M4 и
  пример T019 — на **Ollama в docker** (OpenAI-совместимый
  `/v1/chat/completions`): `docker run -d -p 11434:11434 ollama/ollama`,
  `base_url = "http://localhost:11434/v1"`, `api_key_env` опущен (без
  auth). Модель по умолчанию — **`qwen2.5:3b`** (осмысленная сводка на
  русском, ~2–3 ГБ, CPU-ок); более лёгкая замена — `gemma2:2b`. Команды
  и troubleshooting — в README примера (T019).

### Итог

Critical нет. W1 (ленивая валидация + парковка startup-хука), W2 (фиксация
семантики повторного вызова), W3 (типизированная модель ответа), W4
(настройка Jinja2-окружения), W5 (cap ожидания) учтены в FR/решениях и
плане реализации. Спека готова к **Implement** по 3 фазам (Q8).
Параллельный спин-офф — **T019** (дайджест + примеры).
