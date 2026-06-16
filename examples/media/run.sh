#!/usr/bin/env bash
# Пример angarion «всё в одном»: зеркало с медиа из группы A в B + аннотация
# скачанного файла процессором.
#
# Запуск из любого места: examples/media/run.sh
#
# Dev (есть git-ignored `.secrets` в корне репо): api-реквизиты, группы A/B
# и сессия подхватываются автоматически — ни export, ни login не нужны.
#
# Внешний пользователь (без `.secrets`) — задай api-реквизиты в окружении
# (см. README.md) и подставь id групп в app.toml:
#   export ANGARION_ACCOUNTS__MAIN__API_ID=...
#   export ANGARION_ACCOUNTS__MAIN__API_HASH=...
#
# Первый запуск попросит авторизацию (телефон → код → 2FA), последующие —
# сразу запуск конвейера. Рантайм (БД, ключ сессии, скачанные файлы) — в
# ./angarion-data/ (git-ignored), никаких секретов в репозиторий не попадает.
set -euo pipefail

cd "$(dirname "$0")"  # каталог примера (uv найдёт проект выше по дереву)
ROOT="$(cd ../.. && pwd)"  # корень репо (subshell — cwd примера не меняется)

# Dev-удобство: подхватить постоянные тестовые реквизиты из `.secrets`
# (если есть), чтобы не экспортировать руками. Внешний пользователь без
# `.secrets` идёт обычным путём (export + login по README).
# shellcheck source=../../scripts/example_dev.sh
source "$ROOT/scripts/example_dev.sh"

# dev: подставить тестовые группы в источник/цель пайплайна через env
# (env приоритетнее app.toml — сам app.toml с плейсхолдерами не трогаем).
if [[ -n "${TG_TEST_GROUP_A:-}" && -z "${ANGARION_PIPELINES__MEDIA__SOURCES:-}" ]]; then
  export ANGARION_PIPELINES__MEDIA__SOURCES="[{\"account\":\"main\",\"chat_id\":\"$TG_TEST_GROUP_A\"}]"
fi
if [[ -n "${TG_TEST_GROUP_B:-}" && -z "${ANGARION_PIPELINES__MEDIA__TARGETS:-}" ]]; then
  export ANGARION_PIPELINES__MEDIA__TARGETS="[{\"account\":\"main\",\"chat_id\":\"$TG_TEST_GROUP_B\"}]"
fi

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
  echo "→ Сгенерён ключ шифрования сессии: $KEY_FILE (храни отдельно от app.db!)"
fi
chmod 600 "$KEY_FILE"  # безусловно: права не должны «дрейфовать» при повторных запусках
ANGARION_SESSION_KEY=$(cat "$KEY_FILE")
export ANGARION_SESSION_KEY

# 3. БД + миграции (идемпотентно)
uv run angarion migrate --config "$CONFIG"

# 4. dev: засеять сессию тестового аккаунта из sessions/integration.session
#    (если есть `.secrets` и файл) — обходит интерактивный login. Без них
#    скрипт ничего не делает, и ниже сработает обычная авторизация.
uv run python "$ROOT/scripts/seed_session.py" "$CONFIG" main

# 5. авторизация — только если сессии аккаунта ещё нет (идемпотентно)
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

# 6. запуск конвейера через лаунчер (он регистрирует кастомный процессор)
echo "→ Запуск зеркала с медиа. Пришли в группу A фото/видео/документ —"
echo "  оно прилетит в B с аннотацией скачанного файла. Ctrl+C — стоп."
uv run python run.py
