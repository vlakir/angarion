"""
Граница Telethon: тонкий Protocol-порт + нормализованные raw-DTO
(Q6 спеки T005, M3, фаза 2).

`TelegramClientPort` скрывает реальные вызовы Telethon
(`get_dialogs`/`get_peer_id`/подписки); живущий поверх него listener,
резолвер и mapping — чистые и тестируются на fake-реализации без сети.
Сырое событие Telethon реальная обёртка (`realclient.py`) раскладывает
в pydantic-DTO ниже, поэтому mapping не зависит от Telethon (W1: вся
логика — над портом, обёртка бритвенно-тонкая).

Порт растёт по фазам: catch-up (`fetch_history`) — фаза 3, отправка
(`send_message`) — фаза 4.

Ошибки отправки нормализованы в telethon-free исключения (``FloodWait`` /
``TransientSendError``): реальная обёртка транслирует в них ошибки
Telethon, а sender (над портом) обрабатывает их без зависимости от
Telethon — FloodWait честно пережидается, transient ретраится (§8, FR
«Sender»).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Final, Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict

from angarion.domain.models import EventKind

MESSENGER: Final = 'telegram'
"""Идентификатор платформы Telegram-адаптера."""


def as_peer(chat_id: str) -> int | str:
    """
    Числовой chat_id (в т.ч. ``-100…``) → ``int``, иначе строка (username/
    link/phone).

    Telethon на **строке** числового id падает ``Cannot find any entity``
    — для надёжного резолва/отправки по id нужен ``int`` (gotcha T004 §3).
    Используется и резолвером (``realclient.resolve_peer``), и sender'ом.
    """
    body = chat_id.removeprefix('-')
    return int(chat_id) if body.isdecimal() else chat_id


class FloodWaitError(Exception):
    """
    Telegram требует подождать ``seconds`` (telethon ``FloodWaitError``).
    Не сбой: то же действие повторяется после паузы (FR «Sender», не как
    пропуск в tgcf). Port-вариант telethon-ошибки — sender обрабатывает
    его, не завися от Telethon.
    """

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        super().__init__(f'flood wait {seconds}s')


class TransientSendError(Exception):
    """Временный сбой отправки (сеть/таймаут/5xx) — повторяемый (§8)."""


class RawMedia(BaseModel):
    """
    Нормализованное сырое вложение Telethon до маппинга (M7 A2).

    Раскладывается из ``message.file`` обёрткой (``realclient``) в плоские
    поля; ``mapping`` переводит в доменный ``MediaRef`` без логики ключей.
    ``kind`` — производное от типа сообщения (photo/video/voice/…), метаданные
    — из ``File`` Telethon. ``ref`` (платформенная ссылка для пересылки без
    скачивания) заполняется на sender-фазе A2.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    kind: str
    ref: str | None = None
    mime_type: str | None = None
    file_name: str | None = None
    size: int | None = None
    width: int | None = None
    height: int | None = None
    duration: int | None = None


class RawTelegramMessage(BaseModel):
    """
    Нормализованное сырое сообщение Telethon (NEW/EDITED) до маппинга.

    Числовые id — как их отдаёт Telethon (знаковый chat_id для
    супергрупп ``-100…``); строковыми их делает уже ``mapping`` под
    доменный контракт. ``is_service`` — ``MessageService`` (вступления,
    пины), отсеивается на входе адаптера (FR «Маппинг»). ``media`` — кортеж
    вложений (0..1 на сообщение в MTProto; кортеж — под доменный
    ``list[MediaRef]`` и будущие альбомы/платформы).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    kind: EventKind
    chat_id: int
    message_id: int
    thread_id: int | None = None
    text: str | None = None
    sender_id: int | None = None
    sender_name: str | None = None
    reply_to_message_id: int | None = None
    media: tuple[RawMedia, ...] = ()
    is_service: bool = False
    event_at: AwareDatetime


class RawTelegramDeletion(BaseModel):
    """
    Сырое событие удаления Telethon: список id + chat (для супергрупп).

    ``chat_id=None`` — legacy-группы (§9.4.2): Telegram не передаёт chat
    в update удаления; целевой кейс библиотеки — супергруппы, поэтому
    такие удаления адаптер обрабатывает best-effort/пропускает.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    chat_id: int | None = None
    message_ids: tuple[int, ...]
    deleted_at: AwareDatetime


RawMessageHandler = Callable[[RawTelegramMessage], Awaitable[None]]
"""Колбэк live-сообщения (NEW/EDITED)."""

RawDeletionHandler = Callable[[RawTelegramDeletion], Awaitable[None]]
"""Колбэк live-удаления."""


@runtime_checkable
class TelegramClientPort(Protocol):
    """Узкий интерфейс реальных вызовов Telethon (Q6); fake — для тестов."""

    async def warm_entity_cache(self) -> None:
        """Прогреть кэш сущностей (`get_dialogs`, Q5) — резолв по id не падал."""

    async def resolve_peer(self, peer: str) -> int:
        """Резолв username/link/phone/int → стабильный знаковый chat id."""

    def fetch_history(
        self,
        chat_id: int,
        *,
        limit: int,
        thread_id: int | None = None,
        min_id: int = 0,
    ) -> AsyncIterator[RawTelegramMessage]:
        """
        История чата (или форум-топика ``thread_id``) от **новых к
        старым**, только id > ``min_id``, не более ``limit`` сообщений
        (catch-up §9.3). Пагинацию и троттлинг прячет реализация.
        """

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        *,
        reply_to: int | None = None,
        parse_mode: str | None = None,
        silent: bool = False,
        link_preview: bool = True,
    ) -> int:
        """
        Отправить текст в чат/топик (``reply_to`` — id топика); вернуть
        id отправленного сообщения. ``FloodWaitError`` /
        ``TransientSendError`` транслируются обёрткой из ошибок Telethon
        (FR «Sender»). ``reply_to`` — таргетинг топика, не перенос
        reply-связи (§7 out of scope).
        """

    def on_new_message(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на новые сообщения (`events.NewMessage`)."""

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на правки (`events.MessageEdited`)."""

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        """Подписать колбэк на удаления (`events.MessageDeleted`)."""
