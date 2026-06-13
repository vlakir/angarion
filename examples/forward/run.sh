#!/usr/bin/env bash
# Пример angarion «всё в одном»: пересылка новых сообщений из группы A в B.
#
# Запуск из любого места: examples/forward/run.sh
# Перед первым запуском задай в окружении api-реквизиты (см. README.md):
#   export ANGARION_ACCOUNTS__MAIN__API_ID=...
#   export ANGARION_ACCOUNTS__MAIN__API_HASH=...
# и подставь id групп в app.toml.
#
# Первый запуск попросит авторизацию (телефон → код → 2FA), последующие —
# сразу запуск конвейера. Рантайм (БД, ключ сессии) — в ./angarion-data/
# (git-ignored), никаких секретов в репозиторий не попадает.
set -euo pipefail

cd "$(dirname "$0")"  # каталог примера (uv найдёт проект выше по дереву)

CONFIG=app.toml
DATA_DIR=angarion-data
KEY_FILE="$DATA_DIR/session.key"
mkdir -p "$DATA_DIR"

# 1. api_id / api_hash — обязаны быть в окружении (секреты, не в репо)
: "${ANGARION_ACCOUNTS__MAIN__API_ID:?задай export ANGARION_ACCOUNTS__MAIN__API_ID=...}"
: "${ANGARION_ACCOUNTS__MAIN__API_HASH:?задай export ANGARION_ACCOUNTS__MAIN__API_HASH=...}"

# 2. ключ шифрования сессии: генерим один раз, дальше переиспользуем
#    (новый ключ обесценил бы уже сохранённую сессию → re-login)
if [[ ! -f $KEY_FILE ]]; then
  uv run python -c \
    "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" \
    >"$KEY_FILE"
  chmod 600 "$KEY_FILE"
  echo "→ Сгенерён ключ шифрования сессии: $KEY_FILE (храни отдельно от app.db!)"
fi
ANGARION_SESSION_KEY=$(cat "$KEY_FILE")
export ANGARION_SESSION_KEY

# 3. БД + миграции (идемпотентно)
uv run angarion migrate --config "$CONFIG"

# 4. авторизация — только если сессии аккаунта ещё нет (идемпотентно)
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
  echo "→ Сессия не найдена. Авторизация (телефон → код из Telegram → 2FA):"
  uv run angarion login --config "$CONFIG" --account main
fi

# 5. запуск конвейера
echo "→ Запуск. Пиши в группу A — текст прилетит в B. Ctrl+C — остановка."
uv run angarion run --config "$CONFIG"
