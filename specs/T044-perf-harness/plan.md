# Plan: T044 — нагрузочный гарнес

**Связь:** `spec.md` (статус Clarified). План разбивает зонтичную задачу
на фазы. Каждая фаза = отдельная сессия (`/clear`) + отдельный коммит/PR
с `T044` в заголовке.

## Стек и точки опоры

- Ядро гарнеса переиспользует существующий синтетический контур:
  `MemoryListener.emit` → `IngestService` → queue → `PipelineWorker` →
  `DeliveryWorker` → sink. То же крутят `tests/e2e/` и
  `tests/integration/harness.py`.
- Тривиальный процессор (`passthrough`) — измеряем накладные расходы
  фреймворка, не стоимость обработки.
- Тайминги: монотонный `perf_counter`; метка на `emit`, метка на
  доставке. `AnalyticsEvent.at` — кросс-проверка.
- Бэкенды через конфиг: `[queue].backend` = `memory` | `persistqueue`,
  `[storage]` = memory | sqlite. sqlite-прогоны — на `tmp_path`.
- Воркер для замеров: `poll_interval` уменьшить почти до нуля (иначе
  throughput упрётся в `sleep`), `max_retries` минимизировать.

## Фаза A — Ядро гарнеса + counting-sink + pytest-обвязка

- `src/angarion/testing/`: добавить counting/null-сток (считает
  доставленное, не накапливая список; опц. perf_counter-метка
  последней доставки) + контрактный тест.
- `pyproject.toml`: маркер `perf`, `addopts = -m "not perf"`, dev-dep
  `pyinstrument` (в dev-группу), исключить `tests/perf` из coverage.
- `tests/perf/_harness.py`: сборка `build_app` с настраиваемыми
  бэкендами/knobs; инжекция N записей; ожидание завершения по
  counting-sink; сбор throughput + перцентилей латентности + пиковой
  глубины очереди.
- Один smoke perf-тест (memory, малый N) — доказать гарнес.
- **ADR:** dev-зависимость `pyinstrument`.
- Acceptance: `uv run pytest -m perf tests/perf` зелёный; обычный
  `uv run pytest` perf-тесты не собирает; 4 pre-push проверки чисты.

## Фаза B — Матрица бэкендов + формы нагрузки + отчёт

- Параметризация по (memory, sqlite).
- Burst-форма: throughput + латентность p50/p95/p99.
- Sustained/ramp-форма: целевой темп подачи + профиль `queue.depth()`
  во времени → точка насыщения.
- Печать человекочитаемого отчёта + опц. JSON-артефакт; soft-порог
  `ANGARION_PERF_MIN_RPS` (по умолчанию выкл).
- **Update:** baseline-числа (memory vs sqlite, цена персистентности) в
  `CHANGELOG.md`.

## Фаза C — Изоляция узких мест

- Компонентные микро-бенчи портов в изоляции (queue.put/get,
  analytics.record, dedup/registry/outbox) → rec/s каждого.
- Атрибуция времени по этапам (ingest/processed/delivered) из
  perf_counter-меток.
- `pyinstrument`-профиль sqlite-прогона → назвать сериализующую точку с
  доказательством.
- **Update:** вывод про узкое место → запись-наблюдение в
  `DECISIONS.md`.

## Фаза D — Прототип конкурентности за флагом + ADR

- **Открытый вопрос фазы (решить в мини-спеке перед стартом D):** где
  живёт флаг — (i) harness-local параллельный драйвер (M consumer-тасков
  вокруг реальной очереди/процессора, `src/` не трогаем) либо (ii)
  реальный knob `[worker].concurrency` в `src/` с дефолтом 1. (i) чище
  по scope ядра, (ii) ближе к проду.
- Замер прироста throughput против baseline (фаза B).
- **ADR:** flag-gated конкурентность, отказ от строгого FIFO только за
  флагом, рассмотренные альтернативы, ожидаемый потолок.
- Acceptance: дефолтное поведение M1/FIFO не изменилось; прирост измерен
  и задокументирован.

## Порядок и зависимость

A → B → C независимы по риску (B/C опираются на A). D — последняя
(опирается на baseline из B и узкое место из C). Если D разрастётся —
выносим в отдельную задачу T0NN, в T044 остаётся A–C (измерение +
документирование).
