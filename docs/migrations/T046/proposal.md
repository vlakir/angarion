# T046 — предложение ручной миграции dreamteam 2.0.0

Статус: **предложение получило independent PRE PASS после исправлений;
ручная реализация выполнена; итоговые проверки — outcome.md/checks.json**.
Ниже сохранены план и rationale, по которым ревьюер разрешил деактивацию.

## Границы и происхождение

- Точный base: `1f1430fc91d874d8ec91cbc8b1c241ee84a5c60e` (T045 поверх T044).
  Свежий writer начал с этого HEAD и пустого `git status --short`.
- Задача Ensemble: `task_mvcj1jkc_014o_vefzll`; команда `project-dt200-wip`;
  writer `devops-t046-r1`. Продолжение интеграции — `T046-dreamteam-200-wip`.
- `main` = `bfc5bf48260c10fd326a6c05de5450fcad31d154` — только сравнение:
  `ls-remote` подтвердил SHA; T046 не встречается в его BOARD/BACKLOG/CHANGELOG,
  удалённых веток `T046*` нет. Rebase/merge main не выполнялись.
- `dreamteam --version` = 2.0.0. `dreamteam init` с defaults, language=ru,
  package_manager=uv, project_name=angarion создал **отдельный** игнорируемый
  `.venv/t046-render`, rc=0. Это не обновление исходного проекта.
- Первый настоящий `dreamteam update . --dry-run`: **rc=2**, stdout 0 строк,
  stderr 1 строка, 0 маркеров конфликтов. Raw subprocess output оставался только
  в RAM, наружу выведены whitelist-счётчики/известные пути. Причина rc=2 не
  доказана; CLI migration success **не заявляется**. --force не использован.
- Чтение installed package source отклонено permissions; отказ не обходился.
  Контракт версии проверен через CLI help и реально сгенерированные артефакты.

## Оригиналы до любых деактиваций

Durable private archive:
`~/.local/share/dreamteam/migration-20261016/angarion-T046-devops-r1-49316e9d4bf94a0fa88d573f134387f1/`.
Новый уникальный каталог, parent и вложенные каталоги 0700, файлы 0600;
`manifest.json` содержит base, исходные режимы, размеры и SHA256.
27 файлов: все actual tracked root-файлы + native methodology subtree.
Копирование opaque bytes, значения settings/env/ключей не выводились.
Untracked, symlinks, gitlinks и чужие worktree не обходились/не копировались.
`CONCEPT.md` сохраняется **целиком**, SHA256
`aa075e5e4be29cf145b99f399ba4c8409823b12a1e0a3a98ecdbb59f0aaf8463`.

## Полный managed target inventory (реальный render: 28)

| Target | Actual на base | План / обоснование |
|---|---|---|
| .copier-answers.yml | есть, 1.5.1 | После PASS: truthful manual target2.0.0 metadata, не claim CLI success |
| .gitignore | есть | Preserve, локальные данные и worktree не добавлять |
| .opencode/agents/architect.md | есть, = render | Preserve wildcard deny, реальные read-only tools |
| .opencode/agents/backend.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/agents/designer.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/agents/devops.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/agents/frontend.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/agents/mobile.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/agents/reviewer.md | есть, = render | Preserve wildcard deny, реальные read-only tools |
| .opencode/agents/tech-lead.md | есть, отличается | Добавить question:allow из render; остальной текст preserve |
| .opencode/agents/tester.md | есть, = render | Add local control override question:deny; subagent preserve |
| .opencode/ensemble.json | есть, = render | Preserve mergeOnCleanup:false |
| .opencode/team-roles.md | есть, = render | Уточнить one existing server, lead-only questions и no restart active team |
| AGENTS.md | есть, custom | Полный проектный контекст preserve, native wording; 80→90, protected WIP |
| BACKLOG.md | есть, custom | Preserve bytes, не второй backlog |
| BOARD.md | есть, custom | Добавить только T046 с acceptance, T044/T045 остаются Doing |
| CHANGELOG.md | есть, custom | Preserve bytes, не менять выпуск/версии библиотеки |
| CONCEPT.md | есть, immutable | Preserve exact bytes, без template/preamble |
| DECISIONS.md | есть, custom | Preserve exact bytes, включая T044/T045 ADR |
| README.md | есть, custom | Только активный канон AGENTS/native workflow и ссылка на evidence |
| hooks/pre-push | есть, custom | Preserve bytes/поведение |
| opencode.json | есть, = render | Preserve schema/plugin/config, без providers/models/MCP changes |
| pyproject.toml | есть, custom | Preserve bytes: >=3.12, strict, cov90, все deps |
| scripts/pytest-guard.sh | отсутствует | Не ставить: меняет tooling/gates; serialization при проверке внешним flock |
| specs/design-brief-template.md | отсутствует | Добавить буквальный rendered native template |
| specs/spec-template.md | есть | Diff с render: только Claude→OpenCode в Clarify/Analyze; применить эти 4 wording-замены |
| src/main.py | отсутствует | Не добавлять generic business seed в существующую библиотеку |
| tests/test_main.py | отсутствует | Не добавлять generic seed/no-op тест |

Это inventory 28 **targets**, не утверждение о 28 legacy-файлах.
Actual tracked `.claude/**`: **0**; extra tracked legacy `CLAUDE.md`: **1**.
Исторический preserved[] не используется как inventory. Ни один отсутствующий
legacy/custom файл не восстанавливается по догадке.

## Source → meaningful native mapping

| Источник / поведение | Native результат и проверка |
|---|---|
| CLAUDE.md project instructions | AGENTS.md уже содержит полный проектный текст; проверить отличие только runtime wording/новый migration блок, затем archive+delete CLAUDE.md после PASS |
| Глобальные ссылки ~/.claude/CLAUDE.md | ~/.config/opencode/AGENTS.md; пользовательская конфигурация не меняется |
| Agent/SendMessage/AskUserQuestion исторические имена | Реальные Ensemble team_* и native question; primary tech-lead question:allow из render, шесть writer-role получают LOCAL question:deny (в installed render его НЕТ), architect/reviewer уже wildcard deny |
| Архитектор/reviewer read-only | Существующий wildcard `"*":deny` с whitelist read/glob/grep/list и team communications; не выдавать им worktree/write grants |
| Spec/clarify/analyze и custom project rules | AGENTS.md + specs/spec-template.md; не подменять общим Python3.14/cov80 template |
| Commands/skills/hooks/settings/MCP legacy | Actual tracked отсутствуют, кроме hooks/pre-push (preserve); не придумывать команды/пустые заглушки и не переносить секретные значения |
| T044 perf/T045 SQLite WIP | BOARD Doing, все specs/ADR/бизнес-файлы exact; AnalyticsPort/FIFO/at-least-once/pause и отвергнутые меры неизменны |
| Knowledge/канон | README→AGENTS, immutable CONCEPT + DECISIONS/specs; приватная память только необязательный дубль |
| Runtime continuation | Текущая own T046 integration поверх exact base; existing server4096, без restart/server spawn/global changes; source activation pending ownercoord |

Ничего уникального из CLAUDE.md не удаляется без сохранённого native эквивалента.
Фактический `git diff --no-index CLAUDE.md AGENTS.md` подтвердил: содержательное
тело уже идентично; различаются только runtime/global paths и добавленный блок
миграции. Проектных правил, существующих лишь в CLAUDE.md, нет.
Provenance финализируется только после независимого reviewer PASS этого плана.

## Ordinary repeat и native stability — разные проверки

Обычный `dreamteam update --dry-run` оценивается **полностью**, со всеми 28
targets, включая README/BOARD/CONCEPT/deps/seeds. Возможные MODIFY этих областей
не считаются «no-op»: без переноса custom semantics не применять. Baseline dryrun
rc2 не даёт full ordinary diff; повтор после ручной работы будет зафиксирован с
реальным rc, без подавления/force. Это не CLI migration acceptance и не изменение
канонического T171 AC10.

### Semantic loss assessment полной template-замены (не CLI dryrun diff)

Реальный render даёт материал для оценки, но rc2 НЕ даёт ordinary update plan.
Ниже **сравнение фактических артефактов**, не выдаваемое за вывод CLI:

- **HIGH — AGENTS.md MODIFY**: template Python3.14/cov80/лёгкий pytest и новый
  mutex gate (render строки63–89) ослабили бы проектный Python3.12/TDD/cov90 и
  заменили действующий canonical pytest/addopts; потерялись бы требования к
  примерам, секретам и custom контекст. Решение: preserve полный project текст,
  runtime wording/контроли обновить адресно, stale80→90, gates/deps не менять.
- **HIGH — pyproject.toml MODIFY**: template version0.1/dependencies[]/Python3.14
  вместо библиотеки0.4/Python3.12/реальных extras/entrypoints/dev dependencies;
  потеря runtime и совместимости. Решение: exact byte preserve + uv.lock preserve.
- **HIGH — CONCEPT.md MODIFY**: placeholder вместо106427-byte immutable ТЗ;
  потеря архитектуры/контрактов/исходного видения. Exact byte preserve, не preamble.
- **HIGH — BOARD/BACKLOG/DECISIONS/README/CHANGELOG MODIFY**: шаблонные пустые
  журналы/описания затёрли бы WIP T044/T045, принятую аналитику/SQLite/отвергнутые
  оптимизации, очередь T017/T020/T039/T040, историю релизов и API/quickstart.
  Preserve bytes, кроме адресной записи T046 в BOARD и канон-ссылки в README.
- **MEDIUM — .gitignore/hooks/pre-push MODIFY**: generic replacement мог бы
  потерять project secret/runtime exclusions и custom main-push guard; preserve.
- **MEDIUM — native writer roles MODIFY при literal sync**: template не содержит
  question:deny шести writer ролей; literal sync удалил бы local lead-only control.
  Оставляем явные local overrides, не притворяемся byte equality с template.
- **LOW — spec-template MODIFY**: только четыре Claude→OpenCode wording-замены,
  смыслы Clarify/Analyze сохранены. Design brief template — новое методическое
  дополнение без бизнес-seed. src/main.py/tests/test_main.py — **не добавлять**:
  в существующем package это посторонние business seeds, не успешный no-op.
- pytest-guard candidate — absent, исключён из установки: не менять runtime/gates
  ради миграционной проверки. Внешний flock при local gate не меняет CI behavior.

Private supplemental archive для spec-template/hook/CI:
`~/.local/share/dreamteam/migration-20261016/angarion-T046-supplement-d7845da1ceed4867a20bfa8c5de9c190/`.
Оба архива проверены по byte hashes/реальным режимам всех dirs0700/files0600;
публичный inventory.json содержит28targets и264protectedhashes.

Отдельная узкая проверка native/control stability: все девять ролей, permissions,
Ensemble config, opencode.json, AGENTS custom invariants и SHA защищённых файлов
сверяются дважды; повтор ничего не пишет. Она доказывает только локальную
стабильность этих контролей, не произвольный replay/автоматическое восстановление.

## Acceptance и гейты

1. Full actual inventory + byte originals; independent review до деактивации.
2. Nine roles/lead-only questions/реальные R/O; project context не теряется.
3. Нет изменений src/tests/deps/lock/CI/business/CONCEPT/ADR/T044/T045 цепочки.
4. Truthful manual outcome + full ordinary repeat status + narrow native check.
5. До/после: `ruff check .`, `ruff format --check .`, `mypy src`, `pytest` с cov90
   из own locked uv env. Baseline: все четыре PASS; 1229 passed,20 deselected,
   coverage93.91%, Python3.14.8. Минимальная Python3.12 не меняется; CI matrix
   3.12/3.13/3.14 сохраняется, локальный один interpreter не выдается за CI matrix.
6. Clean checkpoint SHA передан лиду. Без push/PRmain/source activation; acceptance
   интеграции и eventual remote proof остаются у лида.
