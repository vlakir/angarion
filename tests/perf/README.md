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
  - `run_sustained(...)` — open-loop: подавать с целевым темпом
    `target_rps` фиксированное время, сэмплируя `queue.depth()` →
    профиль глубины, МНК-наклон и вердикт насыщения (ingest-bound, если
    подача не достигает target; delivery-bound, если очередь растёт).
  - `BACKENDS` — матрица конфигураций: `('memory','memory')` (потолок) и
    `('persistqueue','sqlite')` (реалистичная персистентность).
  - `build_perf_settings` / `build_perf_plugins` — сборка app с
    настраиваемыми бэкендами и `CountingSink` вместо `MemorySink`.
  - `PerfReport` / `SustainedReport` — модели отчётов с `.render()`.
  - `run_stage_attribution(...)` — однопоточная атрибуция по этапам
    (фаза C, метод (b)): для каждой записи замеряет `perf_counter` вокруг
    ингеста (`emit`), обработки (`worker.process_one`) и доставки
    (`delivery.deliver_next`) → доля ingest/process/deliver во времени.
    `app.start()` НЕ вызывается — этапы гоняются вручную и строго
    последовательно, без фоновых циклов и конкуренции за write-lock.
  - `maybe_write_json(...)` / `maybe_assert_min_rps(...)` — опц. сохранение
    JSON-артефакта и soft-порог throughput (оба через env, по умолч. выкл).
- `_microbench.py` — компонентные микро-бенчи портов (фаза C, метод (a),
  не собирается pytest'ом):
  - `run_microbench(...)` — каждый driven-порт (queue put/get/ack, dedup,
    registry, outbox, analytics) гоняется **отдельно**, в тугом цикле,
    без остального конвейера → rec/s каждого. Локализует сериализующую
    точку количественно. Входные DTO пред-собираются до старта таймера.
  - `MicrobenchReport` — модель отчёта с `.render()` и `.slowest()`.
- `test_burst.py` — burst по матрице бэкендов.
- `test_sustained.py` — sustained/ramp по матрице бэкендов.
- `test_microbench.py` — компонентные микро-бенчи портов по матрице.
- `test_attribution.py` — атрибуция времени по этапам по матрице.
- `test_profile.py` — сэмплирующий профиль (`pyinstrument`) sqlite-burst:
  печатает call-tree, называющий сериализующую точку. С
  `ANGARION_PERF_JSON_DIR` сохраняет HTML-артефакт профиля.

### Env-knobs

- `ANGARION_PERF_BURST_N` — размер burst-пакета (по умолчанию 2000).
- `ANGARION_PERF_SUSTAINED_RPS` — целевой темп подачи (по умолчанию 500).
- `ANGARION_PERF_SUSTAINED_SECONDS` — длительность окна (по умолчанию 5).
- `ANGARION_PERF_MICROBENCH_ITERS` — итераций на порт в микро-бенче (2000).
- `ANGARION_PERF_ATTRIBUTION_N` — записей в атрибуции по этапам (500).
- `ANGARION_PERF_PROFILE_N` — размер burst под профилем (800; меньше
  обычного — сэмплирование добавляет накладные расходы).
- `ANGARION_PERF_MIN_RPS` — soft-порог throughput; если задан и измеренное
  значение ниже — perf-тест падает (regression-сигнал, по умолчанию выкл).
- `ANGARION_PERF_JSON_DIR` — каталог для JSON-артефактов отчётов
  (`<label>.json`, для сравнения прогонов; по умолчанию выкл).

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
- **B** (готово) — матрица бэкендов + sustained/ramp-форма + отчёт
  (human + JSON) + soft-порог `ANGARION_PERF_MIN_RPS`.
- **C** (готово) — изоляция узких мест: микро-бенчи портов (метод (a)) +
  атрибуция по этапам (метод (b)) + `pyinstrument`-профиль (метод (c)).
  Вывод про сериализующую точку SQLite — `DECISIONS.md` (2026-06-20).
- **D** — прототип конкурентного воркера за флагом + ADR.
