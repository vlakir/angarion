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

  # Telegram-креды аккаунта main: .secrets-имена → ANGARION-имена (env
  # приоритетен). Маппинг применяется, только если аккаунт main в примере —
  # Telegram (дефолт): иначе чужие поля (api_id/api_hash/session) поломают
  # схему другого транспорта (MatrixAccountConfig — extra='forbid'). Не-TG
  # примеры выставляют ANGARION_DEV_MAIN_TRANSPORT перед `source` (см.
  # examples/matrix/run.sh), чтобы opt-out из этого маппинга.
  if [[ "${ANGARION_DEV_MAIN_TRANSPORT:-telegram}" == 'telegram' ]]; then
    if [[ -n "${TG_API_ID:-}" ]]; then
      export ANGARION_ACCOUNTS__MAIN__API_ID="${ANGARION_ACCOUNTS__MAIN__API_ID:-$TG_API_ID}"
    fi
    if [[ -n "${TG_API_HASH:-}" ]]; then
      export ANGARION_ACCOUNTS__MAIN__API_HASH="${ANGARION_ACCOUNTS__MAIN__API_HASH:-$TG_API_HASH}"
    fi

    # T042 (ADR 2026-06-20): dev-сессия из env. Авторизованный Telethon-файл
    # (TG_SESSION) на лету конвертируем в StringSession и кладём в
    # ANGARION_ACCOUNTS__MAIN__SESSION — тогда примеры стартуют без login,
    # seed и ключа шифрования (env-сессия приоритетнее app.db). Файл —
    # единственный источник правды, дрейфа нет. Заданная env-сессия
    # приоритетнее.
    if [[ -z "${ANGARION_ACCOUNTS__MAIN__SESSION:-}" && -n "${TG_SESSION:-}" ]]; then
      _tg_session_file="$ROOT/${TG_SESSION}.session"
      if [[ -f "$_tg_session_file" ]]; then
        _string_session="$(
          uv run --project "$ROOT" python - "$ROOT/$TG_SESSION" <<'PY'
import sys

from telethon.sessions import SQLiteSession, StringSession

session = SQLiteSession(sys.argv[1])
try:
    print(StringSession.save(session))
finally:
    session.close()
PY
        )"
        if [[ -n "$_string_session" ]]; then
          export ANGARION_ACCOUNTS__MAIN__SESSION="$_string_session"
          echo "→ dev: StringSession из $_tg_session_file подхвачена в env (login/seed не нужны)."
        fi
      fi
    fi
  fi

  echo "→ dev: реквизиты подхвачены из $_angarion_secrets (для боевого запуска задай свои)."
fi

unset _angarion_secrets _k _v _tg_session_file _string_session
