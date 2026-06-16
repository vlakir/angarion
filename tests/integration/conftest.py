"""
Интеграционные тесты: реальный Telegram-аккаунт (§13.2 ТЗ).

Реквизиты — из переменных окружения или локального git-ignored файла
`.secrets` (KEY=VALUE, образец — `.secrets.example`). Если реквизитов
нет — тесты пропускаются. Session-файл создаётся одноразово скриптом
`scripts/tg_login.py`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from telethon import TelegramClient
from telethon.sessions import SQLiteSession, StringSession

from angarion.application import processors
from angarion.application.processors import FunctionProcessor

from harness import ECHO_PROCESSOR, echo_processor

ROOT = Path(__file__).resolve().parents[2]
SECRETS_FILE = ROOT / '.secrets'
REQUIRED_VARS = (
    'TG_API_ID',
    'TG_API_HASH',
    'TG_SESSION',
    'TG_TEST_GROUP_A',
    'TG_TEST_GROUP_B',
    'TG_TEST_GROUP_C',
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
    group_c: int

    @property
    def groups(self) -> tuple[int, int, int]:
        """Все три тестовые группы."""
        return (self.group_a, self.group_b, self.group_c)


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
        group_c=int(os.environ['TG_TEST_GROUP_C']),
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


@pytest.fixture
def tg_session_string(tg_env: TgEnv) -> str:
    """
    Сессия аккаунта как ``StringSession`` (из файлового session). Из неё
    оснастка поднимает клиент пула; разные клиенты на одном auth-key
    используются только последовательно (live — клиент пула, idle —
    отдельный), не одновременно. Skip, если сессия не авторизована.
    """
    sqlite_session = SQLiteSession(tg_env.session)
    try:
        session_string = StringSession.save(sqlite_session)
    finally:
        sqlite_session.close()
    if not session_string:
        pytest.skip(
            'сессия не авторизована — выполни: uv run python scripts/tg_login.py',
        )
    return str(session_string)


@pytest.fixture
def nonce() -> str:
    """Уникальный маркер прогона — для адресной самоочистки сообщений."""
    return f'angarion-it-{uuid.uuid4().hex[:12]}'


@pytest.fixture
def echo_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Регистрирует ``integration_echo`` в реестре процессоров на время теста.

    Изолированная копия реестра (тестовое имя не утекает между тестами,
    monkeypatch восстанавливает оригинал); ``build_app`` подхватывает его
    при сборке пайплайнов.
    """
    monkeypatch.setattr(processors, '_registry', dict(processors.registered()))
    processors.register(FunctionProcessor(name=ECHO_PROCESSOR, fn=echo_processor))
