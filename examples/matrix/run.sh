#!/usr/bin/env bash
# Пример angarion «всё в одном»: зеркало сообщений между двумя Matrix-комнатами.
#
# Запуск из любого места: examples/matrix/run.sh
#
# Что нужно заранее:
#   1. Matrix-аккаунт на homeserver'е; впиши homeserver/user_id в app.toml,
#      а source/target комнаты (room_id !... или alias #...) — в [pipelines.mirror].
#   2. Пароль аккаунта в окружении (секрет, не в репо):
#        export ANGARION_MATRIX_PASSWORD=...
#      (в dev с git-ignored `.secrets` в корне репо подхватится MATRIX_PASSWORD).
#   3. Extra с E2EE: `uv sync --extra matrix` (тянет nio[e2e] → системная
#      libolm; см. README репозитория, раздел «Установка»).
#
# Первый запуск выполнит `angarion login` (пароль → токен+device в
# зашифрованную сессию app.db), последующие — сразу запуск конвейера.
# Рантайм (БД, ключ сессии, E2EE-стор) — в ./angarion-data/ (git-ignored).
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"

# Dev-удобство: подхватить реквизиты из `.secrets` (если есть).
# shellcheck source=../../scripts/example_dev.sh
source "$ROOT/scripts/example_dev.sh"

CONFIG=app.toml
DATA_DIR=angarion-data
KEY_FILE="$DATA_DIR/session.key"
mkdir -p "$DATA_DIR"

# Пароль аккаунта (секрет): из ANGARION_MATRIX_PASSWORD либо dev-`.secrets`
# (MATRIX_PASSWORD). Без него login не пройдёт.
export ANGARION_MATRIX_PASSWORD="${ANGARION_MATRIX_PASSWORD:-${MATRIX_PASSWORD:?задай export ANGARION_MATRIX_PASSWORD=...}}"

# Ключ шифрования сессии: генерим один раз, дальше переиспользуем
# (новый ключ обесценил бы уже сохранённую сессию → re-login).
if [[ ! -f $KEY_FILE ]]; then
  uv run python -c \
    "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" \
    >"$KEY_FILE"
  echo "→ Сгенерён ключ шифрования сессии: $KEY_FILE (храни отдельно от app.db!)"
fi
chmod 600 "$KEY_FILE"
ANGARION_SESSION_KEY=$(cat "$KEY_FILE")
export ANGARION_SESSION_KEY

# БД + миграции (идемпотентно)
uv run angarion migrate --config "$CONFIG"

# Авторизация — только если сессии аккаунта ещё нет (идемпотентно).
if uv run python - "$CONFIG" <<'PY'
import asyncio
import sys

from angarion.bootstrap import build_storage
from angarion.config import load_settings


async def has_session() -> int:
    storage = build_storage(load_settings(sys.argv[1]))
    try:
        ids = await storage.session.account_ids()
    finally:
        dispose = getattr(storage, "dispose", None)
        if callable(dispose):
            await dispose()
    return 0 if "main" in ids else 1


raise SystemExit(asyncio.run(has_session()))
PY
then
  echo "→ Сессия аккаунта 'main' уже есть — авторизация не нужна."
else
  echo "→ Сессия не найдена. Парольный login (ANGARION_MATRIX_PASSWORD):"
  uv run angarion login --config "$CONFIG" --account main
fi

# Запуск конвейера
echo "→ Запуск. Пиши в комнату-источник — текст/вложения прилетят в цель. Ctrl+C — стоп."
uv run angarion run --config "$CONFIG"
