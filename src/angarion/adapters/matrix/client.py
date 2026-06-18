"""
Граница matrix-nio: тонкий Protocol-порт + нормализованные raw-DTO
(M7 B2, T010; зеркало telegram ``client.py``, Q6 спеки T005).

``MatrixClientPort`` скрывает реальные вызовы ``matrix-nio``
(``restore_login``/``sync_forever``/``add_event_callback``); живущие
поверх него listener и ``mapping`` — чистые и тестируются на
fake-реализации без сети и без nio (W1: вся логика над портом, обёртка
бритвенно-тонкая, ``realclient.py``).

Особенности Matrix против Telegram:

- **Правки.** Matrix-правка — это **новое** событие с
  ``m.relates_to: {rel_type: "m.replace", event_id: <оригинал>}`` и
  ``m.new_content``. Обёртка раскрывает это в ``RawMatrixMessage`` с
  ``kind=MESSAGE_EDITED`` и ``event_id`` = id **оригинала** (а не самой
  правки): так ``external_id`` совпадает с записью реестра исходного
  сообщения, ``previous_text`` достаётся из реестра в ``IngestService``
  (как у Telegram).
- **Удаления.** Redaction несёт ``redacts: <event_id>`` — одно
  ``MESSAGE_DELETED`` на событие (не список, в отличие от Telegram).
- **UTD (unable-to-decrypt).** Зашифрованное событие без ключа сессии
  приходит как ``MegolmEvent``; обёртка отдаёт его отдельным колбэком —
  listener помечает и пропускает (platform limitation §17.9, не падение).
- **Курсор.** Позиция sync — непрозрачный ``next_batch`` (account-level);
  listener сохраняет его в ``CursorStorePort`` через ``on_sync``. Глубокий
  catch-up по ``/messages`` (per-room ``prev_batch``) — фаза B3.

Модуль без ``from __future__ import annotations``: аннотации
pydantic-моделей вычисляются в runtime.
"""

from collections.abc import Awaitable, Callable
from typing import Final, Protocol, runtime_checkable

from pydantic import AwareDatetime, BaseModel, ConfigDict

from angarion.domain.models import EventKind

MESSENGER: Final = 'matrix'
"""Идентификатор платформы Matrix-адаптера."""


class MatrixRateLimitError(Exception):
    """
    Homeserver требует подождать ``retry_after`` секунд (``M_LIMIT_EXCEEDED``).

    Не сбой: то же действие повторяется после паузы (аналог Telegram
    ``FloodWaitError``) — sender обрабатывает, не завися от nio.
    """

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(f'rate limited, retry after {retry_after}s')


class MatrixTransientError(Exception):
    """Временный сбой отправки/истории (сеть/таймаут/5xx) — повторяемый (§8)."""


class RawMatrixMedia(BaseModel):
    """
    Нормализованное сырое вложение Matrix (``mxc://``) до маппинга (M7 B2).

    ``kind`` — производное от ``msgtype`` события (``m.image``→photo,
    ``m.video``→video, ``m.audio``→audio, ``m.file``→document,
    ``m.sticker``→sticker). ``ref`` — ``mxc://``-URI вложения
    (платформенная ссылка для пересылки без скачивания, использует
    sender в B3). Метаданные — из ``content.info``.
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


class RawMatrixMessage(BaseModel):
    """
    Нормализованное сырое сообщение Matrix (NEW/EDITED) до маппинга.

    ``event_id`` — для NEW id самого события; для EDITED обёртка кладёт
    сюда id **оригинала** (цель ``m.replace``), а ``text`` — новый текст
    (``m.new_content``). ``thread_id`` — корневое событие ``m.thread``
    (capability ``threads=True``). ``media`` — кортеж вложений (0..1 на
    событие в Matrix; кортеж — под доменный ``list[MediaRef]``).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    kind: EventKind
    room_id: str
    event_id: str
    thread_id: str | None = None
    text: str | None = None
    sender_id: str | None = None
    sender_name: str | None = None
    reply_to_event_id: str | None = None
    media: tuple[RawMatrixMedia, ...] = ()
    event_at: AwareDatetime


class RawMatrixDeletion(BaseModel):
    """
    Сырое событие удаления Matrix (redaction): один ``redacts`` event_id.

    В отличие от Telegram (пачка id в одном update), Matrix-redaction
    адресует ровно одно событие — маппинг даёт один ``MESSAGE_DELETED``.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    room_id: str
    redacts_event_id: str
    redacted_at: AwareDatetime


class RawMatrixUndecryptable(BaseModel):
    """
    Нерасшифрованное событие (``MegolmEvent``): нет ключа сессии (UTD).

    Listener помечает его как platform limitation (§17.9) и пропускает —
    исторические шифрованные события **до** входа устройства недоступны
    по природе Matrix E2EE, это не баг адаптера (Analyze 🔴, спека T010).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    room_id: str
    event_id: str
    sender_id: str | None = None
    event_at: AwareDatetime


class MatrixHistoryPage(BaseModel):
    """
    Страница истории комнаты (``/messages``) для catch-up (M7 B3).

    ``messages`` — сообщения (NEW/EDITED, от **новых к старым**, как
    Telegram ``fetch_history``); ``redactions`` — удаления (redaction-
    события чанка): в Matrix история несёт redaction явным событием, а
    не отсутствием (надёжнее absence-детекции Telegram).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    messages: tuple[RawMatrixMessage, ...] = ()
    redactions: tuple[RawMatrixDeletion, ...] = ()


RawMessageHandler = Callable[[RawMatrixMessage], Awaitable[None]]
"""Колбэк live-сообщения (NEW/EDITED)."""

RawDeletionHandler = Callable[[RawMatrixDeletion], Awaitable[None]]
"""Колбэк live-удаления (redaction)."""

UndecryptableHandler = Callable[[RawMatrixUndecryptable], Awaitable[None]]
"""Колбэк нерасшифрованного события (UTD)."""

SyncHandler = Callable[[str], Awaitable[None]]
"""Колбэк завершённого sync: получает непрозрачный ``next_batch`` (§9.2)."""


@runtime_checkable
class MatrixClientPort(Protocol):
    """Узкий интерфейс реальных вызовов matrix-nio; fake — для тестов."""

    async def restore(self) -> None:
        """
        Восстановить сессию (``restore_login`` токеном + ``device_id``) и
        загрузить E2EE key-store (olm/megolm). Идемпотентно.
        """

    async def resolve_room(self, alias_or_id: str) -> str:
        """
        Резолв ``#alias:server`` или ``!roomid:server`` → стабильный
        ``room_id``. Числовой/``!``-id — passthrough; alias — запрос к
        homeserver. Недоступно/нет членства → ``ConfigError``.
        """

    async def sync_forever(self, *, since: str | None) -> None:
        """
        Запустить sync-loop: с позиции ``since`` (непрозрачный
        ``next_batch`` или ``None`` — с конца), расшифровывая E2EE и
        вызывая зарегистрированные колбэки. Блокирует до ``stop``.
        """

    async def stop(self) -> None:
        """Остановить sync-loop и закрыть соединение (graceful)."""

    async def send_text(
        self,
        room_id: str,
        body: str,
        *,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        """
        Отправить текст в комнату (опц. в тред ``thread_root`` / reply) →
        ``event_id``. В зашифрованную комнату nio шифрует сам (E2EE-стор
        загружен). ``MatrixRateLimitError``/``MatrixTransientError``
        транслируются из ошибок nio (FR «Sender»).
        """

    async def send_media(
        self,
        room_id: str,
        *,
        body: str,
        kind: str,
        mxc_ref: str | None = None,
        local_path: str | None = None,
        mime_type: str | None = None,
        file_name: str | None = None,
        thread_root: str | None = None,
        reply_to: str | None = None,
    ) -> str:
        """
        Отправить вложение в комнату/тред → ``event_id`` (M7 B3).

        ``local_path`` (скачано при ingest, A3) → заливка файла
        (``upload`` → ``mxc``), работает кросс-аккаунт/кросс-платформа.
        Иначе ``mxc_ref`` — переотправка по ссылке (reupload-by-reference,
        где homeserver позволяет). Ни того, ни другого → деградация до
        текстовой отправки ``body``.
        """

    async def fetch_history(self, room_id: str, *, limit: int) -> 'MatrixHistoryPage':
        """
        История комнаты от **новых к старым**, не более ``limit`` событий
        (catch-up §9.3, B3): сообщения + redaction-события чанка.
        Пагинацию ``/messages`` прячет реализация.
        """

    async def download_media(self, *, mxc: str, dest_dir: str) -> str | None:
        """
        Скачать вложение ``mxc://`` в ``dest_dir`` → локальный путь (A3).
        Недоступно/ошибка границы → ``None`` (метаданные остаются).
        """

    def on_new_message(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на новые сообщения (``RoomMessage*``)."""

    def on_message_edited(self, handler: RawMessageHandler) -> None:
        """Подписать колбэк на правки (``m.replace``)."""

    def on_message_deleted(self, handler: RawDeletionHandler) -> None:
        """Подписать колбэк на удаления (``RedactionEvent``)."""

    def on_undecryptable(self, handler: UndecryptableHandler) -> None:
        """Подписать колбэк на UTD (``MegolmEvent`` без ключа)."""

    def on_sync(self, handler: SyncHandler) -> None:
        """Подписать колбэк на завершённый sync (передаёт ``next_batch``)."""
