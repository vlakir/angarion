"""
Интеграционные тесты: реальный Telegram-аккаунт (§13.2 ТЗ).

Реквизиты — из переменных окружения или локального git-ignored файла
`.secrets` (KEY=VALUE, образец — `.secrets.example`). Если реквизитов
нет — тесты пропускаются. Session-файл создаётся одноразово скриптом
`scripts/tg_login.py`.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from telethon import TelegramClient

ROOT = Path(__file__).resolve().parents[2]
SECRETS_FILE = ROOT / '.secrets'
REQUIRED_VARS = (
    'TG_API_ID',
    'TG_API_HASH',
    'TG_SESSION',
    'TG_TEST_GROUP_A',
    'TG_TEST_GROUP_B',
)


def _load_secrets_file() -> None:
    """Экспортирует пары KEY=VALUE из .secrets в окружение (env приоритетнее)."""
    if not SECRETS_FILE.exists():
        return
    for raw_line in SECRETS_FILE.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        os.environ.setdefault(key.strip(), value.strip())


@dataclass(frozen=True)
class TgEnv:
    """Реквизиты интеграционного контура."""

    api_id: int
    api_hash: str
    session: str
    group_a: int
    group_b: int

    @property
    def groups(self) -> tuple[int, int]:
        """Обе тестовые группы."""
        return (self.group_a, self.group_b)


@pytest.fixture
def tg_env() -> TgEnv:
    """Реквизиты из env/.secrets; skip, если не заданы."""
    _load_secrets_file()
    missing = [name for name in REQUIRED_VARS if not os.environ.get(name)]
    if missing:
        pytest.skip(
            f'нет реквизитов Telegram: {", ".join(missing)} '
            f'(заполни .secrets, см. .secrets.example)',
        )
    return TgEnv(
        api_id=int(os.environ['TG_API_ID']),
        api_hash=os.environ['TG_API_HASH'],
        session=str(ROOT / os.environ['TG_SESSION']),
        group_a=int(os.environ['TG_TEST_GROUP_A']),
        group_b=int(os.environ['TG_TEST_GROUP_B']),
    )


@pytest.fixture
async def tg_client(tg_env: TgEnv) -> AsyncIterator[TelegramClient]:
    """Подключённый авторизованный клиент; skip, если сессии нет."""
    client = TelegramClient(tg_env.session, tg_env.api_id, tg_env.api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        pytest.skip(
            'сессия не авторизована — выполни: uv run python scripts/tg_login.py',
        )
    yield client
    await client.disconnect()
