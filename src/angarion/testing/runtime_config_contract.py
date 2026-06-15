"""Контрактный набор ``RuntimeConfigPort`` (§12.8 ТЗ; FR-4, T024)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from angarion.domain.models import DynamicSettings

if TYPE_CHECKING:
    from angarion.domain.ports import RuntimeConfigPort


class RuntimeConfigContract:
    """
    Поведенческая спецификация динамических настроек (§12.8): sparse-
    override поверх файла. ``load()`` возвращает текущие override'ы
    (``None`` — поле не переопределено, действует файл); ``save(patch)``
    применяет частично (меняются только не-``None`` поля patch'а);
    ``reset(key)`` удаляет override поля (возврат к файлу).

    Реализация подключается переопределением фикстуры ``runtime_config``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def runtime_config(self) -> RuntimeConfigPort:
        raise NotImplementedError

    async def test_empty_load_has_no_overrides(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        loaded = await runtime_config.load()
        assert loaded.registration_enabled is None
        assert loaded.max_pending_registrations is None
        assert loaded.paused_pipelines is None
        assert loaded.log_level is None

    async def test_save_then_load_roundtrip(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        limit = 5
        await runtime_config.save(
            DynamicSettings(registration_enabled=False, max_pending_registrations=limit)
        )
        loaded = await runtime_config.load()
        assert loaded.registration_enabled is False
        assert loaded.max_pending_registrations == limit

    async def test_save_is_partial_merge(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        await runtime_config.save(DynamicSettings(registration_enabled=False))
        await runtime_config.save(DynamicSettings(log_level='DEBUG'))
        loaded = await runtime_config.load()
        # второй save не затирает поле первого (None в patch = «не трогать»)
        assert loaded.registration_enabled is False
        assert loaded.log_level == 'DEBUG'

    async def test_save_overwrites_same_field(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        first, second = 5, 10
        await runtime_config.save(DynamicSettings(max_pending_registrations=first))
        result = await runtime_config.save(
            DynamicSettings(max_pending_registrations=second)
        )
        assert result.max_pending_registrations == second
        assert (await runtime_config.load()).max_pending_registrations == second

    async def test_paused_pipelines_roundtrip(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        await runtime_config.save(
            DynamicSettings(paused_pipelines=frozenset({'digest', 'forward'}))
        )
        assert (await runtime_config.load()).paused_pipelines == frozenset(
            {'digest', 'forward'}
        )

    async def test_reset_removes_override(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        await runtime_config.save(
            DynamicSettings(registration_enabled=False, log_level='DEBUG')
        )
        await runtime_config.reset('registration_enabled')
        loaded = await runtime_config.load()
        assert loaded.registration_enabled is None  # вернулось к файлу
        assert loaded.log_level == 'DEBUG'  # остальное не тронуто

    async def test_reset_unknown_key_is_noop(
        self, runtime_config: RuntimeConfigPort
    ) -> None:
        await runtime_config.save(DynamicSettings(log_level='INFO'))
        await runtime_config.reset('does_not_exist')
        assert (await runtime_config.load()).log_level == 'INFO'
