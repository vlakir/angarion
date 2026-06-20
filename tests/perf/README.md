# Нагрузочный гарнес angarion (T044)

Dev/infra-инструмент: прогоняет синтетический поток записей через
**настоящий** конвейер (`build_app`: ingest → queue → worker → delivery →
sink) без сети и реальных платформ и измеряет пропускную способность и
узкие места. Не часть публичного API библиотеки.

## Запуск

Perf-тесты помечены маркером `perf` и **исключены** из gated-прогона
(`addopts = -m 'not perf'`), чтобы машинозависимый абсолютный throughput
не делал четыре pre-push проверки flaky. Запуск — явным маркером и без
coverage-гейта (`--cov-fail-under=90` на perf-пути заведомо не наберётся):

```bash
uv run pytest -m perf --no-cov tests/perf
```

Человекочитаемый отчёт виден с `-s`:

```bash
uv run pytest -m perf --no-cov -s tests/perf
```

## Что внутри

- `_harness.py` — ядро (не собирается pytest'ом, префикс `_`):
  - `run_burst(...)` — closed-loop burst: подать N записей, дождаться
    доставки всех, замерить throughput (rec/s), перцентили латентности
    (p50/p95/p99) и профиль глубины очереди.
  - `build_perf_settings` / `build_perf_plugins` — сборка app с
    настраиваемыми бэкендами (`queue_backend` = `memory` |
    `persistqueue`, `storage_backend` = `memory` | `sqlite`) и
    `CountingSink` вместо `MemorySink`.
  - `PerfReport.render()` — печать чисел + условий прогона.
- `test_smoke.py` — smoke-доказательство гарнеса (memory, малый N).

Латентность считается меткой `perf_counter` в тексте записи —
`passthrough` ретранслирует её как есть, поэтому корреляция «доставлено →
эмит» не зависит от `OutboundRecord.record_uid` (его passthrough не
проставляет).

## Изоляция и воспроизводимость

- sqlite/persistqueue-прогоны пишут в изолированный `tmp_path` и чистятся
  между тестами — иначе конкуренция за write-lock «плывёт» между
  прогонами.
- Воркер настроен на минимальный `poll_interval` (иначе throughput
  упёрся бы в `asyncio.sleep`, а не в конвейер).
- Абсолютные числа машинозависимы (CPU, диск, FS): гарнес печатает числа
  **и** условия прогона; кросс-машинное сравнение не гарантируется.

## Фазы (см. `specs/T044-perf-harness/plan.md`)

- **A** (готово) — ядро гарнеса + `CountingSink` + pytest-обвязка.
- **B** — матрица бэкендов + sustained/ramp-форма + отчёт + soft-порог
  `ANGARION_PERF_MIN_RPS`.
- **C** — изоляция узких мест (микро-бенчи портов + `pyinstrument`).
- **D** — прототип конкурентного воркера за флагом + ADR.
