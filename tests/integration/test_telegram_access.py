"""
Первый интеграционный тест (T014): доступ к аккаунту и тестовым группам.

Проверяет предпосылки будущего контура §13.2 ТЗ: авторизация
пользовательского аккаунта, видимость обеих тестовых групп, право
отправки. Тесты прибирают за собой (отправленное удаляется).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from telethon import TelegramClient

    from .conftest import TgEnv

pytestmark = pytest.mark.integration

SMOKE_TEXT = 'angarion: интеграционный smoke T014 — сообщение будет удалено'


async def test_account_authorized(tg_client: TelegramClient) -> None:
    """Аккаунт авторизован, get_me возвращает пользователя."""
    me = await tg_client.get_me()
    assert me is not None
    assert me.id > 0


async def test_groups_accessible(tg_client: TelegramClient, tg_env: TgEnv) -> None:
    """Обе тестовые группы видимы аккаунту (entity резолвится)."""
    for chat_id in tg_env.groups:
        entity = await tg_client.get_entity(chat_id)
        assert entity is not None, f'группа {chat_id} недоступна'


async def test_send_and_delete_roundtrip(
    tg_client: TelegramClient,
    tg_env: TgEnv,
) -> None:
    """В обе группы можно отправить сообщение; после теста оно удаляется."""
    for chat_id in tg_env.groups:
        message = await tg_client.send_message(chat_id, SMOKE_TEXT)
        try:
            assert message.id > 0
        finally:
            await tg_client.delete_messages(chat_id, message.id)
