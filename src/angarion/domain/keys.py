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

from angarion.domain.models import Address, EventKind, InboundEvent


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


def make_dedup_key(
    kind: EventKind,
    source_key: str,
    external_id: str,
    content_hash: str | None = None,
) -> str:
    """
    Ключ входящей дедупликации (§7.2).

    Для ``MESSAGE_EDITED`` в ключ входит хэш содержимого (не
    edit_date): гранулярность времени платформ — секунда, хэш
    различает версии надёжно и одинаково работает в live и catch-up.
    Следствие: правка, вернувшая прежний текст, — дубль (осознанно).
    """
    if kind is EventKind.MESSAGE_NEW:
        return f'{source_key}:{external_id}:new'
    if kind is EventKind.MESSAGE_EDITED:
        if content_hash is None:
            msg = 'content_hash обязателен для ключа MESSAGE_EDITED (§7.2)'
            raise ValueError(msg)
        return f'{source_key}:{external_id}:edit:{content_hash}'
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
