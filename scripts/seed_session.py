#!/usr/bin/env python3
"""
Засев dev-сессии Telegram в ``app.db`` примера — обход интерактивного
``angarion login`` при локальной разработке.

Боевой адаптер хранит сессию аккаунта как зашифрованный ``StringSession``
в ``app.db`` (ADR 2026-06-13), а ``scripts/tg_login.py`` для интеграционных
тестов держит её Telethon-файлом (``sessions/integration.session``). Этот
скрипт конвертирует уже авторизованный файл в ``StringSession`` и кладёт
его в ``app.db`` примера — один и тот же тестовый аккаунт переиспользуется
во всех прогонах примеров, без повторной авторизации.

Идемпотентно и безопасно для внешнего пользователя: если ``.secrets`` нет,
session-файл отсутствует или сессия аккаунта в ``app.db`` уже есть —
скрипт ничего не делает и подсказывает, что нужен обычный ``login``.

Запуск: ``uv run python scripts/seed_session.py <config.toml> [account]``
(``account`` по умолчанию ``main``). ``ANGARION_SESSION_KEY`` — в окружении
(тем же ключом примет сессию боевой раннер).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from telethon.sessions import SQLiteSession, StringSession

from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.bootstrap import build_storage
from angarion.config import load_settings

ROOT = Path(__file__).resolve().parents[1]
SECRETS_FILE = ROOT / '.secrets'

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('seed_session')


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


async def seed(config_path: str, account: str) -> int:
    """Перенести Telethon session-файл в ``app.db`` примера как StringSession."""
    load_secrets()
    tg_session = os.environ.get('TG_SESSION')
    if not SECRETS_FILE.exists() or not tg_session:
        logger.info('dev-сессия недоступна (.secrets/TG_SESSION нет) — нужен login.')
        return 0

    session_file = (ROOT / tg_session).with_suffix('.session')
    if not session_file.exists():
        logger.info('session-файл %s не найден — нужен login.', session_file)
        return 0

    settings = load_settings(config_path)
    storage = build_storage(settings)
    try:
        # Soft-fail: повреждённая/залоченная Telethon-БД или сбой шифрования/
        # записи не должны ронять засев — run.sh (`set -e`) иначе оборвётся до
        # fallback на интерактивный login. Логируем и отдаём 0.
        try:
            if account in await storage.session.account_ids():
                logger.info('Сессия %r уже в app.db — засев не нужен.', account)
                return 0

            sqlite_session = SQLiteSession(str(ROOT / tg_session))
            try:
                session_string = StringSession.save(sqlite_session)
            finally:
                sqlite_session.close()
            if not session_string:
                logger.info('Telethon session пуст (не авторизован) — нужен login.')
                return 0

            store = EncryptedSessionStore(storage.session, settings.session_key)
            await store.save(account, session_string)
            logger.info('→ Сессия %r засеяна из %s в app.db.', account, session_file)
        except Exception:
            logger.exception('dev-засев сессии не удался — fallback на login.')
            return 0
    finally:
        dispose = getattr(storage, 'dispose', None)
        if callable(dispose):
            await dispose()
    return 0


def main() -> int:
    """CLI: ``seed_session.py <config.toml> [account]``."""
    args = sys.argv[1:]
    config_path = args[0] if args else 'app.toml'
    account = args[1] if args[1:] else 'main'
    return asyncio.run(seed(config_path, account))


if __name__ == '__main__':
    sys.exit(main())
