"""Тесты конфигурации (§11 ТЗ, объём C-4, FR-16): TOML + env, fail-fast."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    PipelineConfig,
    StorageConfig,
    load_settings,
)
from angarion.domain.errors import ConfigError
from angarion.domain.models import EventKind

if TYPE_CHECKING:
    from pathlib import Path

SAMPLE_TOML = """
[accounts.main]
messenger = "memory"

[storage]
backend = "memory"
dedup_ttl_days = 30
registry_window_days = 7

[queue]
backend = "memory"
depth_warn = 100

[worker]
max_retries = 3
backoff_base = 0.5

[catchup]
enabled = false

[pipelines.digest]
processor = "passthrough"
events = ["message_new", "message_edited"]
only_replies = true
sources = [{ account = "main", chat_id = "-100111" }]
targets = [{ account = "main", chat_id = "-100222", thread_id = "5" }]

[pipelines.digest.processor_config]
template = "{{ text }}"
"""


def write_toml(tmp_path: Path, content: str = SAMPLE_TOML) -> Path:
    path = tmp_path / 'angarion.toml'
    path.write_text(content, encoding='utf-8')
    return path


def test_defaults_without_any_config() -> None:
    """Все секции имеют рабочие default'ы (§11, §17.3, plan 2.2)."""
    settings = AngarionSettings()
    assert settings.accounts == {}
    assert settings.pipelines == {}
    assert settings.storage.backend == 'memory'
    assert settings.storage.dedup_ttl_days == 30
    assert settings.storage.registry_window_days == 7
    assert settings.storage.analytics_retention_days == 90
    assert settings.queue.backend == 'memory'
    assert settings.queue.depth_warn == 500
    assert settings.worker.max_retries == 5
    assert settings.worker.backoff_base == 1.0
    assert settings.worker.backoff_cap == 60.0
    assert settings.worker.poll_interval == 1.0
    assert settings.catchup.enabled is True
    assert settings.catchup.max_messages_per_source == 2000
    assert settings.catchup.max_age_days == 7


def test_load_settings_parses_toml(tmp_path: Path) -> None:
    """TOML §11 разбирается в типизированные секции."""
    settings = load_settings(write_toml(tmp_path))
    assert settings.accounts['main'].messenger == 'memory'
    assert settings.queue.depth_warn == 100
    assert settings.worker.max_retries == 3
    assert settings.catchup.enabled is False
    pipeline = settings.pipelines['digest']
    assert pipeline.processor == 'passthrough'
    assert pipeline.events == frozenset(
        {EventKind.MESSAGE_NEW, EventKind.MESSAGE_EDITED}
    )
    assert pipeline.only_replies is True
    assert pipeline.sources == (EndpointConfig(account='main', chat_id='-100111'),)
    assert pipeline.targets[0].thread_id == '5'
    assert pipeline.processor_config == {'template': '{{ text }}'}


def test_account_section_keeps_plugin_specific_keys(tmp_path: Path) -> None:
    """[accounts.*] — сырой dict: схема аккаунта принадлежит плагину (C-4)."""
    toml = '[accounts.main]\nmessenger = "memory"\nsession = "x.session"\n'
    settings = load_settings(write_toml(tmp_path, toml))
    dumped = settings.accounts['main'].model_dump()
    assert dumped == {'messenger': 'memory', 'session': 'x.session'}


def test_account_without_messenger_fails(tmp_path: Path) -> None:
    """Ключ messenger обязателен в каждой секции аккаунта (FR-2)."""
    toml = '[accounts.main]\nsession = "x.session"\n'
    with pytest.raises(ConfigError, match='messenger'):
        load_settings(write_toml(tmp_path, toml))


def test_env_overrides_toml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Env-переменные ``ANGARION_*`` поверх TOML (§11, plan 2.7)."""
    monkeypatch.setenv('ANGARION_STORAGE__DEDUP_TTL_DAYS', '45')
    settings = load_settings(write_toml(tmp_path))
    assert settings.storage.dedup_ttl_days == 45
    assert settings.storage.registry_window_days == 7


def test_missing_toml_file_fails_fast(tmp_path: Path) -> None:
    """Несуществующий файл конфига — ConfigError, не пустые default'ы."""
    with pytest.raises(ConfigError, match='не найден'):
        load_settings(tmp_path / 'nope.toml')


def test_unknown_top_level_section_fails(tmp_path: Path) -> None:
    """Неизвестная секция — fail-fast (extra='forbid')."""
    with pytest.raises(ConfigError):
        load_settings(write_toml(tmp_path, '[unknown_section]\nx = 1\n'))


def test_dedup_ttl_must_cover_registry_window() -> None:
    """Инвариант §11: dedup_ttl_days ≥ registry_window_days."""
    with pytest.raises(ValidationError, match='dedup_ttl_days'):
        StorageConfig(dedup_ttl_days=5, registry_window_days=10)


def test_unbounded_registry_requires_unbounded_dedup() -> None:
    """0 = бессрочно (§17.3): бессрочный реестр требует бессрочного дедупа."""
    with pytest.raises(ValidationError, match='dedup_ttl_days'):
        StorageConfig(dedup_ttl_days=30, registry_window_days=0)
    unbounded = StorageConfig(dedup_ttl_days=0, registry_window_days=0)
    assert unbounded.dedup_ttl_days == 0
    finite_window = StorageConfig(dedup_ttl_days=0, registry_window_days=7)
    assert finite_window.registry_window_days == 7


def test_storage_and_queue_allow_backend_specific_keys() -> None:
    """Бэкенд-специфичные ключи (path и т.п.) проходят структурную стадию."""
    storage = StorageConfig.model_validate({'backend': 'sqlite', 'path': 'a.db'})
    assert storage.model_dump()['path'] == 'a.db'


def test_pipeline_requires_events_sources_targets() -> None:
    """Пустые events/sources/targets — незрелая декларация, fail-fast."""
    base = {
        'processor': 'passthrough',
        'events': ['message_new'],
        'sources': [{'account': 'main', 'chat_id': '-1'}],
        'targets': [{'account': 'main', 'chat_id': '-2'}],
    }
    PipelineConfig.model_validate(base)
    for field in ('events', 'sources', 'targets'):
        with pytest.raises(ValidationError):
            PipelineConfig.model_validate({**base, field: []})


def test_pipeline_rejects_unknown_event_kind() -> None:
    """Вид события вне закрытого EventKind — fail-fast."""
    with pytest.raises(ValidationError):
        PipelineConfig.model_validate(
            {
                'processor': 'passthrough',
                'events': ['message_reacted'],
                'sources': [{'account': 'main', 'chat_id': '-1'}],
                'targets': [{'account': 'main', 'chat_id': '-2'}],
            }
        )


def test_settings_are_frozen() -> None:
    """Конфиг иммутабелен после загрузки."""
    settings = AngarionSettings()
    with pytest.raises(ValidationError):
        setattr(settings, 'storage', StorageConfig())


def test_telegram_runtime_defaults() -> None:
    """Секция [telegram]/[telegram.sender] и prune/interval — рабочие default'ы."""
    settings = AngarionSettings()
    assert settings.telegram.live_buffer_soft_limit == 1000
    assert settings.telegram.sender.chat_per_second == 1.0
    assert settings.telegram.sender.account_per_minute == 20.0
    assert settings.telegram.sender.flood_max_retries == 5
    assert settings.telegram.sender.transient_max_attempts == 3
    assert settings.catchup.interval is None
    assert settings.worker.prune_interval == 0.0
    assert settings.session_key == ''


def test_telegram_section_parsed_from_toml(tmp_path: Path) -> None:
    """[telegram]/[telegram.sender]/[catchup].interval разбираются из TOML."""
    toml = (
        '[telegram]\n'
        'live_buffer_soft_limit = 50\n'
        '[telegram.sender]\n'
        'chat_per_second = 2.0\n'
        'account_per_minute = 30.0\n'
        'flood_max_retries = 2\n'
        '[catchup]\n'
        'interval = 900\n'
        '[worker]\n'
        'prune_interval = 3600\n'
    )
    settings = load_settings(write_toml(tmp_path, toml))
    assert settings.telegram.live_buffer_soft_limit == 50
    assert settings.telegram.sender.chat_per_second == 2.0
    assert settings.telegram.sender.account_per_minute == 30.0
    assert settings.telegram.sender.flood_max_retries == 2
    assert settings.catchup.interval == 900
    assert settings.worker.prune_interval == 3600


def test_session_key_from_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """session_key читается из env ANGARION_SESSION_KEY (секрет, не в TOML)."""
    monkeypatch.setenv('ANGARION_SESSION_KEY', 'secret-key-value')
    settings = load_settings(write_toml(tmp_path))
    assert settings.session_key == 'secret-key-value'


def test_unknown_telegram_key_fails(tmp_path: Path) -> None:
    """Неизвестный ключ в [telegram] — fail-fast (extra='forbid')."""
    with pytest.raises(ConfigError):
        load_settings(write_toml(tmp_path, '[telegram]\nbogus = 1\n'))
