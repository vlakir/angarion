"""
Публичные хелперы ключей идемпотентности (§7.2–7.3 ТЗ).

Форматы ключей и правила нормализации текста — публичный контракт:
адаптеры (включая сторонние) обязаны использовать эти функции, а не
реализовывать формат самостоятельно. Изменение правил — только в
мажорной версии; контракт зафиксирован golden-тестами.
"""

from __future__ import annotations

import hashlib
import unicodedata

from angarion.domain.models import Address, EventKind, InboundEvent, MediaRef


def make_source_key(
    messenger: str,
    account_id: str,
    chat_id: str,
    thread_id: str | None = None,
) -> str:
    """Ключ источника: ``messenger:account_id:chat_id[:thread_id]``."""
    key = f'{messenger}:{account_id}:{chat_id}'
    if thread_id is not None:
        key = f'{key}:{thread_id}'
    return key


def normalize_and_hash(text: str) -> str:
    r"""
    SHA-256 (hex) нормализованного текста.

    Нормализация (публичный контракт, A-4): Unicode NFC; переводы
    строк ``\\r\\n`` и ``\\r`` → ``\\n``. Больше ничего — без trim,
    lower и схлопывания пробелов: агрессивная нормализация склеила бы
    содержательно разные версии текста.
    """
    normalized = unicodedata.normalize('NFC', text)
    normalized = normalized.replace('\r\n', '\n').replace('\r', '\n')
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def make_media_hash(media: list[MediaRef]) -> str | None:
    r"""
    Отпечаток вложений для edit-детекции (§7.2, M7 A3).

    ``None`` при отсутствии медиа; иначе SHA-256 (hex) канонической записи
    **опознающих** полей каждого вложения: kind / mime / имя / размер /
    размерности / длительность. Сознательно **без** ``ref`` и ``local_path``:
    ``ref`` постоянен в пределах сообщения (координаты источника — не меняются
    при правке), ``local_path`` — сторона доставки. Меняется при подмене файла,
    поэтому правка медиа при том же тексте различается ключом (Q5). Метаданные,
    не байты (fast-path без скачивания): два разных файла с идентичными
    метаданными неотличимы — приемлемый компромисс, см. ADR.
    """
    if not media:
        return None
    parts = [
        '|'.join(
            '' if value is None else str(value)
            for value in (
                m.kind,
                m.mime_type,
                m.file_name,
                m.size,
                m.width,
                m.height,
                m.duration,
            )
        )
        for m in media
    ]
    return hashlib.sha256('\n'.join(parts).encode('utf-8')).hexdigest()


def _edit_slot(content_hash: str | None, media_hash: str | None) -> str | None:
    """
    Слот версии для ключа ``MESSAGE_EDITED`` из текстового и медиа-хэшей.

    Текст без медиа (``media_hash is None``) → слот = ``content_hash``: ключ
    байт-в-байт прежний (golden-контракт §7.2 не меняется). Медиа-only
    (``content_hash is None``) → слот по медиа (правка медиа-без-подписи не
    падает). Текст + медиа → комбинированный хэш: подмена файла при том же
    тексте меняет ключ (Q5). ``None`` — нечего опознавать (нет ни текста, ни
    медиа).
    """
    if media_hash is None:
        return content_hash
    if content_hash is None:
        return f'media:{media_hash}'
    return hashlib.sha256(f'{content_hash}\x00{media_hash}'.encode()).hexdigest()


def make_dedup_key(
    kind: EventKind,
    source_key: str,
    external_id: str,
    content_hash: str | None = None,
    media_hash: str | None = None,
) -> str:
    """
    Ключ входящей дедупликации (§7.2).

    Для ``MESSAGE_EDITED`` в ключ входит хэш содержимого (не edit_date):
    гранулярность времени платформ — секунда, хэш различает версии надёжно и
    одинаково работает в live и catch-up. Следствие: правка, вернувшая прежний
    текст, — дубль (осознанно). С M7 версию-слот формирует ``_edit_slot`` из
    текстового и медиа-хэшей: текст-без-медиа даёт прежний ключ, медиа влияет
    аддитивно (правка вложения = новая версия; медиа-only больше не падает).
    """
    if kind is EventKind.MESSAGE_NEW:
        return f'{source_key}:{external_id}:new'
    if kind is EventKind.MESSAGE_EDITED:
        slot = _edit_slot(content_hash, media_hash)
        if slot is None:
            msg = (
                'content_hash или media_hash обязателен для ключа MESSAGE_EDITED (§7.2)'
            )
            raise ValueError(msg)
        return f'{source_key}:{external_id}:edit:{slot}'
    return f'{source_key}:{external_id}:del'


def make_idempotency_key(
    pipeline: str,
    event: InboundEvent,
    target: Address,
    n: int,
) -> str:
    """
    Ключ идемпотентности исходящего (§7.3).

    Имя пайплайна включено обязательно: при multicast два пайплайна
    могут слать в одну группу и не должны подавлять друг друга.
    Worker частично применяет функцию со своим пайплайном (A-9), у
    процессора остаётся сигнатура ``(event, target, n) -> str``.
    """
    return f'{event.dedup_key}->{pipeline}:{target.chat_id}:{n}'
