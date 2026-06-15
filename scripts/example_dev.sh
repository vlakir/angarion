#!/usr/bin/env bash
# Dev-удобство для запуска примеров: подхватывает постоянные тестовые
# реквизиты из git-ignored `.secrets` (корень репо), чтобы не экспортировать
# их руками перед каждым прогоном. Подключается через `source`:
#
#   ROOT=...                       # корень репозитория
#   source "$ROOT/scripts/example_dev.sh"
#
# Семантика — как у scripts/tg_login.py: уже заданные ANGARION_*-переменные
# окружения приоритетнее (.secrets их не перетирает). Если `.secrets` нет
# (внешний пользователь) — helper ничего не делает, примеры работают по
# README (явный export + интерактивный login). Секреты в репозиторий не
# попадают: `.secrets` и `sessions/` в .gitignore.
#
# Маппит:
#   TG_API_ID/TG_API_HASH → ANGARION_ACCOUNTS__MAIN__API_ID/API_HASH
# и экспортит TG_TEST_GROUP_A/TG_TEST_GROUP_B как есть (вызывающий
# run.sh прокидывает их в источники/цели своего пайплайна).

_angarion_secrets="${ANGARION_DEV_SECRETS:-$ROOT/.secrets}"

if [[ -f "$_angarion_secrets" ]]; then
  # Загружаем пары KEY=VALUE (без перетирания уже заданного окружения).
  while IFS='=' read -r _k _v; do
    _k="${_k// /}"
    [[ -z "$_k" || "$_k" == \#* ]] && continue
    # Только валидные имена переменных: кривая строка с set -e иначе уронит
    # launcher на indirect expansion / export.
    [[ "$_k" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    [[ -z "${!_k:-}" ]] && export "$_k=${_v# }"
  done <"$_angarion_secrets"

  # Креды аккаунта main: .secrets-имена → ANGARION-имена (env приоритетен).
  if [[ -n "${TG_API_ID:-}" ]]; then
    export ANGARION_ACCOUNTS__MAIN__API_ID="${ANGARION_ACCOUNTS__MAIN__API_ID:-$TG_API_ID}"
  fi
  if [[ -n "${TG_API_HASH:-}" ]]; then
    export ANGARION_ACCOUNTS__MAIN__API_HASH="${ANGARION_ACCOUNTS__MAIN__API_HASH:-$TG_API_HASH}"
  fi
  echo "→ dev: реквизиты подхвачены из $_angarion_secrets (для боевого запуска задай свои)."
fi

unset _angarion_secrets _k _v
