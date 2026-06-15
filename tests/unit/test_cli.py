"""CLI: run / migrate / login + graceful shutdown (M3, T005, фаза 5)."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from cryptography.fernet import Fernet

from angarion import cli
from angarion.adapters.storage.engine import ensure_schema_current
from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.bootstrap import build_storage
from angarion.config import AngarionSettings
from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from pathlib import Path


def _sqlite_settings(tmp_path: Path, **extra: object) -> AngarionSettings:
    data: dict[str, object] = {
        'storage': {'backend': 'sqlite', 'path': str(tmp_path / 'app.db')},
    }
    data.update(extra)
    return AngarionSettings.model_validate(data)


def _write_config(tmp_path: Path) -> Path:
    path = tmp_path / 'app.toml'
    path.write_text(
        '[storage]\nbackend = "sqlite"\npath = '
        f'"{tmp_path / "app.db"}"\n',
        encoding='utf-8',
    )
    return path


class FakeApp:
    def __init__(self) -> None:
        self.events: list[str] = []

    async def start(self) -> None:
        self.events.append('start')

    async def stop(self) -> None:
        self.events.append('stop')


class TestParser:
    def test_missing_command_exits(self) -> None:
        with pytest.raises(SystemExit):
            cli.main([])

    def test_run_requires_config(self) -> None:
        with pytest.raises(SystemExit):
            cli.main(['run'])


class TestMigrate:
    def test_applies_migrations_to_head(self, tmp_path: Path) -> None:
        settings = _sqlite_settings(tmp_path)
        cli.cmd_migrate(settings)
        # схема на head — ensure_schema_current не бросает
        ensure_schema_current(tmp_path / 'app.db')

    def test_creates_missing_parent_dir(self, tmp_path: Path) -> None:
        nested = tmp_path / 'data' / 'sub'
        settings = AngarionSettings.model_validate(
            {'storage': {'backend': 'sqlite', 'path': str(nested / 'app.db')}}
        )
        cli.cmd_migrate(settings)
        assert (nested / 'app.db').exists()

    def test_rejects_non_sqlite_backend(self) -> None:
        settings = AngarionSettings.model_validate({'storage': {'backend': 'memory'}})
        with pytest.raises(ConfigError, match='sqlite'):
            cli.cmd_migrate(settings)

    def test_requires_path(self) -> None:
        settings = AngarionSettings.model_validate({'storage': {'backend': 'sqlite'}})
        with pytest.raises(ConfigError, match='path'):
            cli.cmd_migrate(settings)


class TestServe:
    async def test_starts_then_stops_when_already_signalled(self) -> None:
        app = FakeApp()
        stop = asyncio.Event()
        stop.set()
        await cli._serve(app, stop)  # type: ignore[arg-type]
        assert app.events == ['start', 'stop']

    async def test_stops_on_event_set_after_start(self) -> None:
        app = FakeApp()
        stop = asyncio.Event()
        task = asyncio.create_task(cli._serve(app, stop))  # type: ignore[arg-type]
        for _ in range(100):
            if app.events == ['start']:
                break
            await asyncio.sleep(0.005)
        stop.set()
        await task
        assert app.events == ['start', 'stop']

    async def test_install_signal_handlers_runs(self) -> None:
        cli._install_signal_handlers(asyncio.Event())


class TestRun:
    async def test_builds_app_and_serves(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = FakeApp()
        monkeypatch.setattr(cli, '_install_signal_handlers', lambda stop: stop.set())
        await cli.cmd_run(
            AngarionSettings(),
            app_factory=lambda _settings: app,  # type: ignore[arg-type, return-value]
        )
        assert app.events == ['start', 'stop']

    async def test_role_api_dispatches_to_serve_api(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[str] = []

        async def fake_serve_api(_s: AngarionSettings, _stop: asyncio.Event) -> None:
            seen.append('api')

        monkeypatch.setattr(cli, '_install_signal_handlers', lambda stop: stop.set())
        monkeypatch.setattr(cli, 'serve_api', fake_serve_api)
        await cli.cmd_run(AngarionSettings(), role='api')
        assert seen == ['api']

    async def test_role_combined_dispatches_to_serve_combined(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[str] = []

        async def fake_serve_combined(
            _s: AngarionSettings, _stop: asyncio.Event
        ) -> None:
            seen.append('combined')

        monkeypatch.setattr(cli, '_install_signal_handlers', lambda stop: stop.set())
        monkeypatch.setattr(cli, 'serve_combined', fake_serve_combined)
        await cli.cmd_run(AngarionSettings(), role='combined')
        assert seen == ['combined']


class TestLogin:
    async def test_saves_encrypted_session(self, tmp_path: Path) -> None:
        key = Fernet.generate_key().decode()
        settings = _sqlite_settings(
            tmp_path,
            accounts={'main': {'messenger': 'telegram', 'api_id': 2040, 'api_hash': 'h'}},
            session_key=key,
        )

        async def fake_login(api_id: int, api_hash: str) -> str:
            assert (api_id, api_hash) == (2040, 'h')
            return 'SESSION-XYZ'

        await cli.cmd_login(settings, 'main', login=fake_login)

        storage = build_storage(settings)
        store = EncryptedSessionStore(storage.session, key)
        loaded = await store.load('main')
        await storage.dispose()  # type: ignore[attr-defined]
        assert loaded == 'SESSION-XYZ'

    async def test_unknown_account_fails(self, tmp_path: Path) -> None:
        settings = _sqlite_settings(tmp_path)

        async def fake_login(_api_id: int, _api_hash: str) -> str:
            return 'X'

        with pytest.raises(ConfigError, match='ghost'):
            await cli.cmd_login(settings, 'ghost', login=fake_login)


class TestMain:
    def test_migrate_dispatch_returns_zero(self, tmp_path: Path) -> None:
        rc = cli.main(['migrate', '--config', str(_write_config(tmp_path))])
        assert rc == 0

    def test_missing_config_returns_one(self, tmp_path: Path) -> None:
        rc = cli.main(['migrate', '--config', str(tmp_path / 'nope.toml')])
        assert rc == 1

    def test_run_dispatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        async def fake_run(_settings: AngarionSettings, *, role: str = 'pipeline') -> None:
            calls.append(role)

        monkeypatch.setattr(cli, 'cmd_run', fake_run)
        rc = cli.main(['run', '--config', str(_write_config(tmp_path))])
        assert rc == 0
        assert calls == ['pipeline']

    def test_run_with_api_dispatches_combined(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        async def fake_run(_settings: AngarionSettings, *, role: str = 'pipeline') -> None:
            calls.append(role)

        monkeypatch.setattr(cli, 'cmd_run', fake_run)
        rc = cli.main(
            ['run', '--config', str(_write_config(tmp_path)), '--with-api']
        )
        assert rc == 0
        assert calls == ['combined']

    def test_run_role_api_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        async def fake_run(_settings: AngarionSettings, *, role: str = 'pipeline') -> None:
            calls.append(role)

        monkeypatch.setattr(cli, 'cmd_run', fake_run)
        rc = cli.main(
            ['run', '--config', str(_write_config(tmp_path)), '--role', 'api']
        )
        assert rc == 0
        assert calls == ['api']

    def test_login_dispatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: list[str] = []

        async def fake_login(_settings: AngarionSettings, account: str) -> None:
            captured.append(account)

        monkeypatch.setattr(cli, 'cmd_login', fake_login)
        rc = cli.main(
            ['login', '--config', str(_write_config(tmp_path)), '--account', 'main']
        )
        assert rc == 0
        assert captured == ['main']

    def test_configure_logging_runs(self) -> None:
        cli._configure_logging()
