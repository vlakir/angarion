"""
Плагин внутреннего транспорта ``internal`` (T037): объявление возможностей,
sink-only-контракт и схема аккаунта.

Сборка sink'а (``make_sender`` → ``InternalSink``) и проводка в конвейер
проверяются сквозным ``tests/e2e/test_chain.py`` через ``build_app``.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from angarion.adapters.internal.plugin import (
    INTERNAL_CAPABILITIES,
    PLUGIN,
    InternalAccountConfig,
)


class TestPluginObject:
    """Декларация entry point ``angarion.adapters:internal``."""

    def test_name_and_sink_only(self) -> None:
        """A2: имя ``internal`` и отсутствие listener'а (sink-only)."""
        assert PLUGIN.name == 'internal'
        assert PLUGIN.make_listener is None
        assert PLUGIN.make_login is None

    def test_capabilities_new_only(self) -> None:
        """Q4: только ``new`` — без правок/удалений/истории/тредов."""
        caps = INTERNAL_CAPABILITIES
        assert caps.edit_events is False
        assert caps.delete_events is False
        assert caps.history_fetch is False
        assert caps.threads is False
        assert caps.user_account is False
        assert caps.push_transport == 'none'

    def test_account_config_model_is_internal_account_config(self) -> None:
        assert PLUGIN.account_config_model is InternalAccountConfig

    def test_account_config_accepts_internal_transport(self) -> None:
        assert InternalAccountConfig(transport='internal').transport == 'internal'

    def test_account_config_rejects_other_transport(self) -> None:
        with pytest.raises(ValidationError):
            InternalAccountConfig.model_validate({'transport': 'telegram'})

    def test_account_config_forbids_extra_keys(self) -> None:
        with pytest.raises(ValidationError):
            InternalAccountConfig.model_validate({'transport': 'internal', 'api_id': 1})
