"""
SC-5 (T003): публичная поверхность ``angarion.testing`` импортируема
сторонним пакетом без доступа к ``tests/`` библиотеки; pytest11-плагин
зарегистрирован entry point'ом (A-2); контрактные классы несут
asyncio-маркер для потребителей со ``asyncio_mode = "strict"`` (A-3).
"""

from __future__ import annotations

from importlib.metadata import entry_points

import angarion.testing

CONTRACT_CLASSES = [
    'AnalyticsContract',
    'CursorStoreContract',
    'DeadLetterContract',
    'DedupStoreContract',
    'EventQueueContract',
    'MessageRegistryContract',
    'MessageSinkContract',
    'OutboxContract',
    'StateStoreContract',
]

FACTORIES = [
    'FAR_FUTURE',
    'LONG_AGO',
    'NOW',
    'SOURCE_KEY',
    'make_address',
    'make_analytics_event',
    'make_cursor',
    'make_dead_letter',
    'make_envelope',
    'make_event',
    'make_outbound',
    'make_record',
]


def test_public_surface_is_importable() -> None:
    for name in CONTRACT_CLASSES + FACTORIES:
        assert hasattr(angarion.testing, name), name


def test_all_declares_exactly_the_public_surface() -> None:
    assert sorted(angarion.testing.__all__) == sorted(CONTRACT_CLASSES + FACTORIES)


def test_contract_classes_carry_asyncio_marker() -> None:
    """A-3: наборы собираются и при strict-режиме pytest-asyncio."""
    for name in CONTRACT_CLASSES:
        cls = getattr(angarion.testing, name)
        marks = getattr(cls, 'pytestmark', None)
        assert marks is not None, name
        marks = marks if isinstance(marks, list | tuple) else [marks]
        assert any(m.name == 'asyncio' for m in marks), name


def test_pytest11_plugin_registered_via_entry_point() -> None:
    """A-2: assert-rewriting регистрируется до импорта подмодулей."""
    (ep,) = entry_points(group='pytest11', name='angarion_testing')
    assert ep.value == 'angarion._testing_plugin'
