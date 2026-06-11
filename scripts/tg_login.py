#!/usr/bin/env python3
"""
Одноразовая интерактивная авторизация Telegram для интеграционных тестов.

Читает реквизиты из `.secrets` (см. `.secrets.example`), создаёт
session-файл по пути TG_SESSION и выводит список доступных групп
с их id — для заполнения TG_TEST_GROUP_A / TG_TEST_GROUP_B.

Запуск: `uv run python scripts/tg_login.py` (спросит код подтверждения,
который Telegram пришлёт в приложение / по SMS).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from telethon import TelegramClient

ROOT = Path(__file__).resolve().parents[1]
SECRETS_FILE = ROOT / '.secrets'

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('tg_login')


def load_secrets() -> None:
    """Экспортирует пары KEY=VALUE из .secrets в окружение (env приоритетнее)."""
    if not SECRETS_FILE.exists():
        return
    for raw_line in SECRETS_FILE.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        os.environ.setdefault(key.strip(), value.strip())


async def run() -> int:
    """Авторизуется в Telegram и выводит доступные группы с id."""
    load_secrets()
    api_id = os.environ.get('TG_API_ID', '')
    api_hash = os.environ.get('TG_API_HASH', '')
    phone = os.environ.get('TG_PHONE') or None
    session = os.environ.get('TG_SESSION', 'sessions/integration')

    if not api_id.isdigit() or not api_hash:
        logger.error(
            'Заполни TG_API_ID и TG_API_HASH в %s (см. .secrets.example)',
            SECRETS_FILE,
        )
        return 1

    session_path = ROOT / session
    session_path.parent.mkdir(parents=True, exist_ok=True)

    client = TelegramClient(str(session_path), int(api_id), api_hash)
    await client.start(phone=phone)
    me = await client.get_me()
    logger.info(
        'Авторизован: %s (id=%s)',
        me.username or me.first_name,
        me.id,
    )
    logger.info('Session-файл: %s.session', session_path)
    logger.info('')
    logger.info('Доступные группы/каналы (кандидаты в TG_TEST_GROUP_A/B):')
    async for dialog in client.iter_dialogs():
        if dialog.is_group or dialog.is_channel:
            logger.info('  %-15s — %s', dialog.id, dialog.title)
    await client.disconnect()
    return 0


if __name__ == '__main__':
    sys.exit(asyncio.run(run()))
