"""
Публичные контрактные наборы тестов портов angarion (§12.11 ТЗ;
FR-9 спеки T003) — сертификационный инструмент автора стороннего
адаптера: параметризуйте классы своей реализацией (fixture-override)
и добейтесь зелёного прогона.

Пакет требует extra ``angarion[testing]`` (pytest + pytest-asyncio).
Assert-rewriting подмодулей регистрирует pytest11-плагин
``angarion._testing_plugin`` (A-2 спеки T003) — он загружается pytest'ом
до импорта этого пакета, поэтому здесь только re-export'ы.
"""

from angarion.testing.analytics_contract import AnalyticsContract
from angarion.testing.cursor_contract import CursorStoreContract
from angarion.testing.dead_letter_contract import DeadLetterContract
from angarion.testing.dedup_contract import DedupStoreContract
from angarion.testing.factories import (
    FAR_FUTURE,
    LONG_AGO,
    NOW,
    SOURCE_KEY,
    make_address,
    make_analytics_event,
    make_cursor,
    make_dead_letter,
    make_envelope,
    make_event,
    make_outbound,
    make_record,
)
from angarion.testing.outbox_contract import OutboxContract
from angarion.testing.queue_contract import EventQueueContract
from angarion.testing.registry_contract import MessageRegistryContract
from angarion.testing.session_contract import SessionStoreContract
from angarion.testing.sink_contract import MessageSinkContract
from angarion.testing.state_contract import StateStoreContract

__all__ = [
    'FAR_FUTURE',
    'LONG_AGO',
    'NOW',
    'SOURCE_KEY',
    'AnalyticsContract',
    'CursorStoreContract',
    'DeadLetterContract',
    'DedupStoreContract',
    'EventQueueContract',
    'MessageRegistryContract',
    'MessageSinkContract',
    'OutboxContract',
    'SessionStoreContract',
    'StateStoreContract',
    'make_address',
    'make_analytics_event',
    'make_cursor',
    'make_dead_letter',
    'make_envelope',
    'make_event',
    'make_outbound',
    'make_record',
]
