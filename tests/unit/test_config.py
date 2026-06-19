"""Тесты конфигурации (§11 ТЗ, объём C-4, FR-16): TOML + env, fail-fast."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from angarion.config import (
    AngarionSettings,
    EndpointConfig,
    MediaConfig,
    PipelineConfig,
    QueueConfig,
    StorageConfig,
    WorkerConfig,
    load_settings,
)
from angarion.domain.errors import ConfigError
from angarion.domain.models import EventKind, MediaRef

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
    assert settings.queue.keep_acked == 1000
    assert settings.worker.max_retries == 5
    assert settings.worker.backoff_base == 1.0
    assert settings.worker.backoff_cap == 60.0
    assert settings.worker.poll_interval == 1.0
    assert settings.worker.shutdown_drain_seconds == 5.0
    assert settings.catchup.enabled is True
    assert settings.catchup.max_messages_per_source == 2000
    assert settings.catchup.max_age_days == 7
    assert settings.catchup.recent_interval == 30.0
    assert settings.catchup.recent_window_messages == 30
    assert settings.catchup.recent_window_minutes == 10


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


def test_queue_keep_acked_parses_and_rejects_negative() -> None:
    """[queue] keep_acked — ретеншн acked-строк (T016): ≥0, 0 = бессрочно."""
    assert QueueConfig.model_validate({'keep_acked': 0}).keep_acked == 0
    assert QueueConfig.model_validate({'keep_acked': 500}).keep_acked == 500
    with pytest.raises(ValidationError, match='keep_acked'):
        QueueConfig.model_validate({'keep_acked': -1})


def test_worker_shutdown_drain_seconds_must_be_positive() -> None:
    """[worker] shutdown_drain_seconds — граница graceful-дренажа (T031): > 0."""
    cfg = WorkerConfig.model_validate({'shutdown_drain_seconds': 2.5})
    assert cfg.shutdown_drain_seconds == 2.5
    with pytest.raises(ValidationError, match='shutdown_drain_seconds'):
        WorkerConfig.model_validate({'shutdown_drain_seconds': 0})


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


def test_media_defaults_conservative() -> None:
    """[media] по умолчанию не качает (только метаданные, §3.A)."""
    settings = AngarionSettings()
    media = settings.media
    assert media.download is False
    assert media.allowed_kinds == frozenset()
    assert media.max_size == 0
    assert media.storage_dir == 'data/media'
    assert media.storage_path == Path('data/media')
    assert media.retention_days == 0


def test_media_section_parsed_from_toml(tmp_path: Path) -> None:
    """[media] разбирается из TOML с whitelist/лимитом/ретеншном."""
    toml = (
        '[media]\n'
        'download = true\n'
        'allowed_kinds = ["photo", "video"]\n'
        'max_size = 1048576\n'
        'storage_dir = "data/blobs"\n'
        'retention_days = 7\n'
    )
    settings = load_settings(write_toml(tmp_path, toml))
    assert settings.media.download is True
    assert settings.media.allowed_kinds == frozenset({'photo', 'video'})
    assert settings.media.max_size == 1048576
    assert settings.media.storage_path == Path('data/blobs')
    assert settings.media.retention_days == 7


def test_media_unknown_key_fails(tmp_path: Path) -> None:
    """Незнакомый ключ [media] — fail-fast (extra='forbid')."""
    with pytest.raises(ConfigError):
        load_settings(write_toml(tmp_path, '[media]\nbogus = 1\n'))


def _media(**overrides: object) -> MediaRef:
    fields: dict[str, object] = {'kind': 'photo', 'ref': '123:42', 'size': 1000}
    fields.update(overrides)
    return MediaRef.model_validate(fields)


def test_should_download_off_by_default() -> None:
    """Выключенная политика — не качаем ничего."""
    assert MediaConfig().should_download(_media()) is False


def test_should_download_enabled_passes_matching() -> None:
    """Включённая политика без фильтров качает любое вложение с ref."""
    assert MediaConfig(download=True).should_download(_media()) is True


def test_should_download_skips_when_no_ref() -> None:
    """Без платформенной ссылки рефетчить нечего."""
    assert MediaConfig(download=True).should_download(_media(ref=None)) is False


def test_should_download_skips_already_downloaded() -> None:
    """Уже скачанное (local_path) повторно не качаем."""
    media = _media(local_path='/tmp/x.jpg')
    assert MediaConfig(download=True).should_download(media) is False


def test_should_download_respects_allowed_kinds() -> None:
    """Вид вне whitelist'а — пропускаем."""
    policy = MediaConfig(download=True, allowed_kinds=frozenset({'video'}))
    assert policy.should_download(_media(kind='photo')) is False
    assert policy.should_download(_media(kind='video')) is True


def test_should_download_respects_max_size() -> None:
    """Превышение max_size — пропускаем; неизвестный размер — качаем."""
    policy = MediaConfig(download=True, max_size=500)
    assert policy.should_download(_media(size=1000)) is False
    assert policy.should_download(_media(size=400)) is True
    assert policy.should_download(_media(size=None)) is True


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


def test_api_defaults() -> None:
    """Секция [api] (M5/B): рабочие default'ы auth/JWT/регистрации (§12.7)."""
    settings = AngarionSettings()
    assert settings.api.host == '127.0.0.1'
    assert settings.api.port == 8000
    assert settings.api.auth == 'users'
    assert settings.api.secret == ''
    assert settings.api.jwt_lifetime == 3600
    assert settings.api.cookie_secure is False
    assert settings.api.registration_enabled is True
    assert settings.api.max_pending_registrations == 20
    assert settings.admin_login == ''
    assert settings.admin_password == ''


def test_api_section_parsed_from_toml(tmp_path: Path) -> None:
    """[api] разбирается из TOML; auth='none' допустим."""
    toml = (
        '[api]\n'
        'host = "0.0.0.0"\n'
        'port = 9000\n'
        'auth = "none"\n'
        'jwt_lifetime = 900\n'
        'cookie_secure = true\n'
        'registration_enabled = false\n'
        'max_pending_registrations = 5\n'
    )
    settings = load_settings(write_toml(tmp_path, toml))
    assert settings.api.host == '0.0.0.0'
    assert settings.api.port == 9000
    assert settings.api.auth == 'none'
    assert settings.api.jwt_lifetime == 900
    assert settings.api.cookie_secure is True
    assert settings.api.registration_enabled is False
    assert settings.api.max_pending_registrations == 5


def test_api_secret_and_admin_from_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """secret и admin-credentials — из env (секреты, не в TOML; §12.7, FR-0)."""
    monkeypatch.setenv('ANGARION_API__SECRET', 'jwt-secret')
    monkeypatch.setenv('ANGARION_ADMIN_LOGIN', 'root')
    monkeypatch.setenv('ANGARION_ADMIN_PASSWORD', 'pw')
    settings = load_settings(write_toml(tmp_path))
    assert settings.api.secret == 'jwt-secret'
    assert settings.admin_login == 'root'
    assert settings.admin_password == 'pw'


def test_api_rejects_unknown_auth_mode(tmp_path: Path) -> None:
    """auth вне {users,none} — fail-fast."""
    with pytest.raises(ConfigError):
        load_settings(write_toml(tmp_path, '[api]\nauth = "ldap"\n'))


def test_unknown_api_key_fails(tmp_path: Path) -> None:
    """Неизвестный ключ в [api] — fail-fast (extra='forbid')."""
    with pytest.raises(ConfigError):
        load_settings(write_toml(tmp_path, '[api]\nbogus = 1\n'))
