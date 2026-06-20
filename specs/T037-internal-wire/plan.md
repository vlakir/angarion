# Plan: T037 «Внутренний провод»

**Статус:** Draft
**Дата:** 2026-06-20
**Спека:** `specs/T037-internal-wire/spec.md` (Clarified + Analyzed)

Технический план реализации поверх транспорт-агностичной модели T041. Решения
Clarify (Q1–Q6) и находки Analyze (A1–A10) — в спеке. Каждая фаза = отдельный
коммит на ветке `T037-internal-wire`, по возможности одна фаза за сессию
(`/clear` между фазами для чистого контекста).

---

## 1. Архитектура (итог)

`internal` — обычный транспорт-адаптер (`AdapterPlugin`, entry-point
`angarion.adapters`), **sink-only**: его sink вместо доставки наружу преобразует
`OutboundRecord` → `Record(kind=new)` и подаёт в `IngestService.ingest`
(re-ingestion). Дальше — штатный конвейер (ingest → очередь → worker → outbox →
delivery). Ребро цепочки = совпадение `(transport=internal, address=канал)` у
`target` одного пайплайна и `source` другого. Граф рёбер — производный от конфига
(валидация + viz), не доменная сущность.

Поток одного звена:
```
P1.processor → OutboundRecord{target=(internal,X), send_via=(internal,wire),
                              trace_id, hops}                       [worker стейджит]
  → outbox → DeliveryWorker → dispatch by send_via.transport=internal
  → InternalSink.send():
        Record{kind=new, source=(internal,X), external_id=f(idempotency_key),
               dedup_key=f(idempotency_key), trace_id, hops+1, origin='internal'}
        → ingest.ingest(record)
  → router.resolve(source=(internal,X)) → P2 → ...
```

## 2. Затрагиваемые компоненты (карта правок)

| Слой | Файл (примерно) | Правка |
|---|---|---|
| domain/models | `domain/models.py` | `Record`: `+trace_id`, `+hops`, `origin`+`'internal'`; `OutboundRecord`: `+trace_id`, `+hops` (носители, A3) |
| domain/keys | `domain/keys.py` | деривация `external_id`/`dedup_key` внутренней записи из `idempotency_key` (Q3) |
| adapters/internal | `adapters/internal/` (new) | `AdapterPlugin` (name=internal, caps={new}, sink-only), `InternalSink`, account-config-model |
| domain/plugin | `domain/plugin.py` | `make_listener: ListenerFactory \| None` (A2) — либо no-op listener в адаптере |
| application/loop_guard | `application/loop_guard.py` или `bootstrap` | исключить `transport=internal` из guarded-источников (A1) |
| application/worker | `application/worker.py` | копировать `trace_id`/`hops` входной `Record` в стейджимый `OutboundRecord` (A3) |
| application/ingest | `application/ingest.py` | проверка `hops>max_hops` → DLQ (`hop_limit_exceeded`); не требовать registry/catch-up для caps-неполного источника (A7/A8) |
| config | `config.py` | internal account model; `[chains] max_hops` (дефолт 10, A9); стартовая DAG-валидация рёбер (A2-cycle) |
| bootstrap | `bootstrap.py` | регистрация internal-адаптера; проброс `ingest` в `InternalSink`; порядок init; DAG-валидация на старте |
| adapters/http viz | `adapters/http/viz.py` + шаблоны | внутреннее ребро P1→P2 (схлопывание канала), визуально отличимо (A5) |
| persistence | сериализация очереди / analytics origin | новые поля через `QueueEnvelope`/`queue.db`; `origin='internal'` — миграция vs чистый разрыв (A4) |

## 3. Фазы

### Фаза 1 — Модель + конфиг + DAG-валидация (фундамент)

**Scope:** аддитивные поля модели, ключи стыка, конфиг внутреннего транспорта,
стартовая защита от циклов. Без re-ingestion-поведения — но всё тестируемо.

- `Record`: `trace_id: str | None = None` + `model_validator`, проставляющий
  `str(uid)` если не задан (корень цепочки; не трогаем все места создания
  `Record`), `hops: int = 0`, `origin` Literal += `'internal'`.
- `OutboundRecord`: `trace_id: str`, `hops: int = 0` (носители для проброса).
- `domain/keys.py`: функция деривации внутренних `external_id`/`dedup_key` из
  `idempotency_key` (инъективно, по образцу T043 escape).
- `config.py`: модель `[accounts.*]` для `plugin="internal"`; ключ
  `[chains] max_hops` (дефолт 10).
- Стартовая DAG-валидация: построить граф внутренних рёбер из `settings.pipelines`,
  topo-sort, цикл → `ConfigError` fail-fast (включая self-loop P→P).
- **Решить A4:** перечислить точки персистенции новых полей; pre-alpha → чистый
  разрыв (как T041/T043) или Alembic-миграция, если `origin` в БД.
- Тесты: поля round-trip (вкл. сериализацию `QueueEnvelope`); деривация ключей
  (инъективность, повтор → тот же ключ); DAG (ацикличный ок / цикл / self-loop /
  fan-in / fan-out); парс конфига internal-аккаунта и `max_hops`.

**Коммит:** `T037: доменная модель + конфиг + DAG-валидация внутренних рёбер`

### Фаза 2 — Внутренний адаптер + re-ingestion (ядро провода)

**Scope:** sink-only адаптер, преобразование и re-ingestion, проводка, защиты.

- `adapters/internal/plugin.py`: `AdapterPlugin(name='internal',
  capabilities={new})`, sink-only — закрыть A2 (рекомендация:
  `make_listener: …|None`; альтернатива — no-op listener; см. ADR §Открытые).
- `InternalSink(SinkPort)`: `OutboundRecord` → `Record(kind=new,
  source=(internal,address), external_id/dedup_key=f(idempotency_key),
  trace_id=outbound.trace_id, hops=outbound.hops+1, origin='internal',
  received_by=(internal,wire), content_hash/...)` → `await ingest.ingest(record)`
  → вернуть `DeliveryReceipt`.
- `worker.py`: при стейджинге копировать `trace_id`/`hops` входной `Record` в
  `OutboundRecord` (для не-internal целей поля просто едут «вхолостую»).
- `ingest.py`: **проверка** `hops>max_hops` → DLQ (`hop_limit_exceeded`)
  (инкремент `hops` делает `InternalSink` при построении записи, не ingest);
  убедиться, что для источника с caps `{new}` (нет registry/catch-up) путь не
  падает (A7/A8).
- `bootstrap.py`: зарегистрировать internal-адаптер; собрать `InternalSink` с
  ссылкой на `ingest` (порядок init: storage/queue → ingest → adapters/sinks →
  dispatcher → workers); **исключить `internal` из guarded-источников (A1)**.
- Тесты (обязательный набор, fake-/memory-стек): e2e P1→P2 без реального
  транспорта; детерминизм ключей; **at-least-once** (повторная доставка ребра →
  дедуп гасит, у P2 ровно одна запись); fan-out по двум внутренним каналам (A6);
  hop-limit → DLQ; loop-guard НЕ душит внутреннее ребро (A1-регресс).

**Коммит:** `T037: internal-адаптер, re-ingestion, защита циклов/петель`

### Фаза 3 — Web-viz топологии цепочек

**Scope:** граф `/ui/pipelines` рисует внутренние рёбра P1→P2 отличимо.

- `viz.py build_pipeline_graph`: распознавать `transport=internal`, схлопывать
  внутренний канал в ребро pipeline→pipeline (не два endpoint-узла), пометить
  отличимым стилем (цвет/пунктир).
- Шаблон/фрагмент `pipelines.html`: легенда/стиль внутреннего ребра.
- Тесты: граф из цепочечного конфига даёт pipeline→pipeline ребро; внутренний
  канал не висит «двумя коробками»; визуальный признак присутствует.

**Коммит:** `T037: визуализация цепочек в /ui/pipelines`

### Фаза 4 — Пример + документация (Update)

**Scope:** user-facing deliverable (договорённость 2026-06-13: внутренний провод —
способ конфигурации, пользователь взаимодействует напрямую → пример нужен).

- `examples/chain/` (new): два пайплайна, связанных внутренним каналом (напр.
  нормализатор → обогатитель → доставка в Telegram); `run.sh`, README, плейсхолдеры
  секретов (git-ignored).
- Сверить существующие `examples/` на актуальность (правок публичного API нет —
  поля аддитивные; ожидаемо без изменений).
- `CHANGELOG.md` `[Unreleased]`: фича + ломающие/аддитивные изменения модели.
- `DECISIONS.md`: ADR (см. §4).
- `README.md` + web-api гайд: внутренний транспорт, цепочки, `max_hops`.

**Коммит:** `T037: пример examples/chain + документация`

> Фазы 1–2 — «инфраструктурное» ядро, но фича в целом user-facing → пример в
> фазе 4 обязателен (критерий из проектного CLAUDE.md «Примеры как часть фичи»).

## 4. Черновик ADR (в `DECISIONS.md` на фазе 4)

**2026-06-?? — T037: внутренний провод цепочек пайплайнов**

- **Контекст:** связать пайплайны в цепочку можно было лишь через реальную
  группу-посредник (трафик, задержка, видимый служебный чат; межпайплайновый
  цикл loop-guard не ловит). Нужен прямой провод без выхода на платформу.
- **Решение:** chaining как **внутренний транспорт `internal`** (sink-only
  адаптер, sink замкнут на `ingest`), поверх транспорт-агностичной модели T041.
  - Ключи стыка детерминированы из `idempotency_key` (at-least-once без дублей).
  - Защита от циклов: fail-fast DAG-валидация внутренних рёбер на старте +
    рантайм `hops`-лимit (ловит и циклы через реальную платформу).
  - Capability `{new}` (брокер-подобный); edited/deleted сквозь ребро — вне MVP.
  - Сквозная трассировка: `trace_id` + `origin='internal'`.
  - **Инвариант:** `internal` исключён из loop-guard (на нём нет эха платформы;
    защиту даёт DAG+hops).
  - `make_listener` становится опциональным (sink-only адаптеры) — аддитивно.
- **Альтернативы:** явные рёбра `[[chains]]` (отдельный путь re-ingestion в обход
  диспетчера + «ребро» в домене) — отвергли как дублирующее и инвазивное к домену;
  свежие ключи на стыке — отвергли (ломают at-least-once); только стартовая
  DAG-проверка — отвергли (не ловит цикл через платформу).
- **Последствия:** ядро не получает понятие графа (граф производен от конфига);
  аддитивные поля `Record`/`OutboundRecord`; правка `viz`; пре-alpha — чистый
  разрыв сериализации, если затронут персист.

## 5. Открытые верификации (закрыть в начале соответствующей фазы)

- **A2-выбор:** `make_listener: …|None` (рекомендую — честный контракт sink-only)
  vs no-op listener (без правки контракта). Проверить все call-site `make_listener`
  на обработку `None` (Telegram/Matrix/memory) — фаза 2.
- **A4-персист:** где реально лежат `origin`/новые поля (analytics БД? только
  in-flight `QueueEnvelope`?) — определить миграцию vs чистый разрыв — фаза 1.
- **Проводка ingest в sink:** подтвердить порядок init в `bootstrap.py` (ingest
  до сборки dispatcher/sinks) — фаза 2.
- **ingest для caps-`{new}` источника:** убедиться, что `_maintain_registry`/
  catch-up не активируются для `internal` (нет listener — вероятно бесплатно) —
  фаза 2.

## 6. Риски

- **Сериализация очереди** новых полей `Record` — при несовместимости старые
  записи в `queue.db` не десериализуются; пре-alpha → допустимо очистить (нет
  релиза с цепочками). Зафиксировать в CHANGELOG.
- **Двойной dedup-mark** (ingest шаг 5 + бывший loop-guard) после исключения
  internal из guard — устранён A1; проверить регрессом.
- **viz-объём** (фаза 3) недооценим — раскладка графа с рёбрами pipeline→pipeline
  может потребовать перелопатить координатную логику; держать в отдельной фазе.

## 7. Следующий шаг

Повторный **Analyze** spec+plan до чистого прохода (проверить, что план не внёс
новых противоречий), затем **Implement фаза 1**.
