"""
SettingsNotifier + применение уровня лога на лету (§12.8, FR-4; T024,
M5/C, фаза 3).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
import structlog

from angarion.application.settings import (
    SettingsNotifier,
    apply_log_level,
    apply_log_level_on_change,
)
from angarion.domain.models import DynamicSettings

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def _restore_structlog() -> Iterator[None]:
    """Глобальная конфигурация structlog мутируется — откатываем после теста."""
    yield
    structlog.reset_defaults()


def test_apply_log_level_sets_filtering_wrapper() -> None:
    apply_log_level('WARNING')
    expected = structlog.make_filtering_bound_logger(logging.WARNING)
    assert structlog.get_config()['wrapper_class'] is expected


def test_apply_log_level_is_case_insensitive() -> None:
    apply_log_level('debug')
    expected = structlog.make_filtering_bound_logger(logging.DEBUG)
    assert structlog.get_config()['wrapper_class'] is expected


def test_apply_log_level_rejects_unknown() -> None:
    with pytest.raises(ValueError, match='неизвестный уровень лога'):
        apply_log_level('LOUD')


class TestNotifier:
    pytestmark = pytest.mark.asyncio

    async def test_subscriber_receives_settings(self) -> None:
        seen: list[DynamicSettings] = []

        async def remember(settings: DynamicSettings) -> None:
            seen.append(settings)

        notifier = SettingsNotifier()
        notifier.subscribe(remember)
        payload = DynamicSettings(log_level='ERROR')
        await notifier.notify(payload)
        assert seen == [payload]

    async def test_log_level_subscriber_applies_level(self) -> None:
        notifier = SettingsNotifier()
        notifier.subscribe(apply_log_level_on_change)
        await notifier.notify(DynamicSettings(log_level='ERROR'))
        expected = structlog.make_filtering_bound_logger(logging.ERROR)
        assert structlog.get_config()['wrapper_class'] is expected

    async def test_none_log_level_is_noop(self) -> None:
        before = structlog.get_config()['wrapper_class']
        await apply_log_level_on_change(DynamicSettings(paused_pipelines=frozenset()))
        assert structlog.get_config()['wrapper_class'] is before

    async def test_failing_subscriber_does_not_block_others(self) -> None:
        reached: list[str] = []

        async def boom(settings: DynamicSettings) -> None:
            msg = 'подписчик упал'
            raise RuntimeError(msg)

        async def survivor(settings: DynamicSettings) -> None:
            reached.append('ok')

        notifier = SettingsNotifier()
        notifier.subscribe(boom)
        notifier.subscribe(survivor)
        await notifier.notify(DynamicSettings(log_level='INFO'))  # не падает
        assert reached == ['ok']
