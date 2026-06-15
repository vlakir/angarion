"""
ASGI-тесты аутентификации (§12.7, T023 фаза 2): JWT-логин, защита
роутеров, роли, саморегистрация-заявка, bootstrap админа и fail-fast.

User store — реальный SQLite (миграция 0003) в ``tmp_path``; остальные
порты InMemory. Матрица «аноним / pending / viewer / admin» × «health /
diagnostics / admin-ручка / register» (§12.7, spec §4, часть A).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from fastapi import APIRouter
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from angarion.adapters.http import AngarionDeps, create_app
from angarion.adapters.http.auth import AdminUser, UserRole
from angarion.adapters.http.auth.bootstrap import ensure_admin
from angarion.adapters.http.auth.users import password_helper
from angarion.adapters.memory.queue import MemoryQueue
from angarion.adapters.memory.storage import (
    MemoryAnalytics,
    MemoryCursorStore,
    MemoryMessageRegistry,
    MemoryStateStore,
)
from angarion.adapters.storage.engine import apply_migrations, make_engine
from angarion.adapters.storage.orm import UserRow
from angarion.config import ApiConfig
from angarion.domain.errors import ConfigError

from conftest import asgi_client, make_settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from httpx import AsyncClient
    from sqlalchemy.ext.asyncio import AsyncEngine

SECRET = 'test-jwt-secret-at-least-32-bytes-long'


@pytest.fixture
async def users_db(tmp_path: object) -> AsyncIterator[async_sessionmaker]:
    """sessionmaker user store поверх реального SQLite с миграцией 0003."""
    db_path = tmp_path / 'app.db'  # type: ignore[operator]
    apply_migrations(db_path)
    engine: AsyncEngine = make_engine(db_path)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


async def seed_user(
    sessions: async_sessionmaker,
    *,
    login: str,
    password: str,
    role: UserRole,
    is_active: bool,
) -> None:
    async with sessions() as session:
        session.add(
            UserRow(
                login=login,
                hashed_password=password_helper.hash(password),
                role=role.value,
                is_active=is_active,
                registered_at=datetime.now(UTC),
            )
        )
        await session.commit()


def build_app(sessions: async_sessionmaker | None, **api: object) -> object:
    settings = make_settings(api=ApiConfig(auth='users', secret=SECRET, **api))
    deps = AngarionDeps(
        queue=MemoryQueue(),
        analytics=MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        settings=settings,
        auth_sessionmaker=sessions,
    )
    admin_router = APIRouter()

    @admin_router.get('/api/v1/admin-check')
    async def admin_check(user: AdminUser) -> dict[str, str]:
        return {'login': user.login}

    return create_app(deps, routers=[admin_router])


async def login(client: AsyncClient, login_: str, password: str) -> str | None:
    resp = await client.post(
        '/api/v1/auth/login', data={'username': login_, 'password': password}
    )
    if resp.status_code != 200:
        return None
    return str(resp.json()['access_token'])


# --- защита роутеров и матрица ролей ---


@pytest.mark.asyncio
async def test_health_is_public_in_users_mode(users_db: async_sessionmaker) -> None:
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        assert (await client.get('/api/v1/health')).status_code == 200


@pytest.mark.asyncio
async def test_diagnostics_requires_auth(users_db: async_sessionmaker) -> None:
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        assert (await client.get('/api/v1/diagnostics')).status_code == 401


@pytest.mark.asyncio
async def test_viewer_reads_diagnostics_not_admin(
    users_db: async_sessionmaker,
) -> None:
    await seed_user(
        users_db, login='v', password='pw', role=UserRole.VIEWER, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        token = await login(client, 'v', 'pw')
        assert token is not None
        headers = {'Authorization': f'Bearer {token}'}
        assert (await client.get('/api/v1/diagnostics', headers=headers)).status_code == 200
        assert (await client.get('/api/v1/admin-check', headers=headers)).status_code == 403


@pytest.mark.asyncio
async def test_admin_reaches_admin_route(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='a', password='pw', role=UserRole.ADMIN, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        token = await login(client, 'a', 'pw')
        headers = {'Authorization': f'Bearer {token}'}
        resp = await client.get('/api/v1/admin-check', headers=headers)
        assert resp.status_code == 200
        assert resp.json() == {'login': 'a'}


@pytest.mark.asyncio
async def test_cookie_token_accepted(users_db: async_sessionmaker) -> None:
    """UI-транспорт: тот же JWT принимается из HTTPOnly-cookie (§12.7)."""
    await seed_user(
        users_db, login='c', password='pw', role=UserRole.VIEWER, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        token = await login(client, 'c', 'pw')
        client.cookies.set('angarionauth', token or '')
        assert (await client.get('/api/v1/diagnostics')).status_code == 200


# --- цикл регистрации ---


@pytest.mark.asyncio
async def test_register_creates_inactive_and_login_denied(
    users_db: async_sessionmaker,
) -> None:
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        reg = await client.post(
            '/api/v1/auth/register', json={'login': 'new', 'password': 'pw'}
        )
        assert reg.status_code == 201
        assert reg.json()['is_active'] is False
        assert reg.json()['role'] == UserRole.VIEWER.value
        # вход заявкой невозможен — аккаунт не одобрен
        assert await login(client, 'new', 'pw') is None


@pytest.mark.asyncio
async def test_register_duplicate_login_rejected(
    users_db: async_sessionmaker,
) -> None:
    await seed_user(
        users_db, login='dup', password='pw', role=UserRole.VIEWER, is_active=False
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        resp = await client.post(
            '/api/v1/auth/register', json={'login': 'dup', 'password': 'pw'}
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_registration_can_be_disabled(users_db: async_sessionmaker) -> None:
    app = build_app(users_db, registration_enabled=False)
    async with asgi_client(app) as client:  # type: ignore[arg-type]
        resp = await client.post(
            '/api/v1/auth/register', json={'login': 'x', 'password': 'pw'}
        )
        assert resp.status_code == 403


# --- auth="none" ---


@pytest.mark.asyncio
async def test_auth_none_allows_admin_without_login() -> None:
    settings = make_settings(api=ApiConfig(auth='none'))
    deps = AngarionDeps(
        queue=MemoryQueue(),
        analytics=MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        settings=settings,
    )
    admin_router = APIRouter()

    @admin_router.get('/api/v1/admin-check')
    async def admin_check(user: AdminUser) -> dict[str, str]:
        return {'login': user.login}

    async with asgi_client(create_app(deps, routers=[admin_router])) as client:
        assert (await client.get('/api/v1/diagnostics')).status_code == 200
        assert (await client.get('/api/v1/admin-check')).status_code == 200


@pytest.mark.asyncio
async def test_auth_none_non_local_bind_warns_but_builds() -> None:
    """``auth="none"`` + bind не на localhost — приложение поднимается (с warning)."""
    settings = make_settings(api=ApiConfig(auth='none', host='0.0.0.0'))
    deps = AngarionDeps(
        queue=MemoryQueue(),
        analytics=MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        settings=settings,
    )
    async with asgi_client(create_app(deps)) as client:
        assert (await client.get('/api/v1/health')).status_code == 200


@pytest.mark.asyncio
async def test_bootstrap_noop_in_auth_none(users_db: async_sessionmaker) -> None:
    """``auth="none"`` без admin-env на пустой БД — no-op, без fail-fast."""
    settings = make_settings(api=ApiConfig(auth='none'))
    await ensure_admin(users_db, settings)
    async with users_db() as session:
        assert await session.scalar(select(func.count()).select_from(UserRow)) == 0


# --- bootstrap первого админа + fail-fast ---


@pytest.mark.asyncio
async def test_bootstrap_creates_admin_on_empty_db(
    users_db: async_sessionmaker,
) -> None:
    settings = make_settings(
        api=ApiConfig(auth='users', secret=SECRET),
        admin_login='root',
        admin_password='rootpw',
    )
    await ensure_admin(users_db, settings)
    app = build_app(users_db)
    async with asgi_client(app) as client:  # type: ignore[arg-type]
        token = await login(client, 'root', 'rootpw')
        assert token is not None
        headers = {'Authorization': f'Bearer {token}'}
        assert (await client.get('/api/v1/admin-check', headers=headers)).status_code == 200


@pytest.mark.asyncio
async def test_bootstrap_fail_fast_without_admin_env(
    users_db: async_sessionmaker,
) -> None:
    settings = make_settings(api=ApiConfig(auth='users', secret=SECRET))
    with pytest.raises(ConfigError):
        await ensure_admin(users_db, settings)


@pytest.mark.asyncio
async def test_bootstrap_noop_when_users_exist(
    users_db: async_sessionmaker,
) -> None:
    await seed_user(
        users_db, login='a', password='pw', role=UserRole.ADMIN, is_active=True
    )
    settings = make_settings(
        api=ApiConfig(auth='users', secret=SECRET),
        admin_login='root',
        admin_password='rootpw',
    )
    await ensure_admin(users_db, settings)  # не должен падать и не создаёт root
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        assert await login(client, 'root', 'rootpw') is None


def test_create_app_fail_fast_without_secret(
    users_db: async_sessionmaker,
) -> None:
    settings = make_settings(api=ApiConfig(auth='users', secret=''))
    deps = AngarionDeps(
        queue=MemoryQueue(),
        analytics=MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        settings=settings,
        auth_sessionmaker=users_db,
    )
    with pytest.raises(ConfigError):
        create_app(deps)


# --- фаза 3: цикл одобрения, лимит, users CRUD, cookie-вход, /ui/users ---


async def _bearer(client: AsyncClient, login_: str, password: str) -> dict[str, str]:
    token = await login(client, login_, password)
    assert token is not None
    return {'Authorization': f'Bearer {token}'}


@pytest.mark.asyncio
async def test_full_registration_approval_login_cycle(
    users_db: async_sessionmaker,
) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        # заявка → вход запрещён (не одобрена)
        assert (
            await client.post(
                '/api/v1/auth/register', json={'login': 'new', 'password': 'pw'}
            )
        ).status_code == 201
        assert await login(client, 'new', 'pw') is None
        # админ одобряет
        admin = await _bearer(client, 'admin', 'pw')
        listing = await client.get('/api/v1/users', headers=admin)
        assert listing.status_code == 200
        new_id = next(u['id'] for u in listing.json() if u['login'] == 'new')
        patch = await client.patch(
            f'/api/v1/users/{new_id}',
            json={'is_active': True, 'role': 'viewer'},
            headers=admin,
        )
        assert patch.status_code == 200
        assert patch.json()['is_active'] is True
        assert patch.json()['approved_by'] == 'admin'
        # теперь вход работает
        assert await login(client, 'new', 'pw') is not None


@pytest.mark.asyncio
async def test_users_crud_requires_admin(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='v', password='pw', role=UserRole.VIEWER, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        viewer = await _bearer(client, 'v', 'pw')
        assert (await client.get('/api/v1/users', headers=viewer)).status_code == 403


@pytest.mark.asyncio
async def test_pending_registration_limit(users_db: async_sessionmaker) -> None:
    app = build_app(users_db, max_pending_registrations=1)
    async with asgi_client(app) as client:  # type: ignore[arg-type]
        assert (
            await client.post(
                '/api/v1/auth/register', json={'login': 'a', 'password': 'pw'}
            )
        ).status_code == 201
        assert (
            await client.post(
                '/api/v1/auth/register', json={'login': 'b', 'password': 'pw'}
            )
        ).status_code == 429


@pytest.mark.asyncio
async def test_admin_create_then_delete_user(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        admin = await _bearer(client, 'admin', 'pw')
        created = await client.post(
            '/api/v1/users',
            json={'login': 'manual', 'password': 'pw', 'role': 'viewer'},
            headers=admin,
        )
        assert created.status_code == 201
        assert created.json()['is_active'] is True
        assert await login(client, 'manual', 'pw') is not None
        deleted = await client.delete(
            f'/api/v1/users/{created.json()["id"]}', headers=admin
        )
        assert deleted.status_code == 204
        assert await login(client, 'manual', 'pw') is None


@pytest.mark.asyncio
async def test_cookie_login_and_logout(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='u', password='pw', role=UserRole.VIEWER, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        assert (await client.get('/ui/login')).status_code == 200
        submit = await client.post(
            '/ui/login', data={'username': 'u', 'password': 'pw'}
        )
        assert submit.status_code == 303
        assert 'angarionauth' in client.cookies
        # cookie из jar даёт доступ к защищённому /ui
        assert (await client.get('/ui')).status_code == 200
        logout = await client.get('/ui/logout')
        assert logout.status_code == 303


@pytest.mark.asyncio
async def test_ui_login_rejects_bad_credentials(users_db: async_sessionmaker) -> None:
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        resp = await client.post(
            '/ui/login', data={'username': 'nobody', 'password': 'x'}
        )
        assert resp.status_code == 401
        assert 'angarionauth' not in client.cookies


@pytest.mark.asyncio
async def test_ui_users_page_and_approve(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    await seed_user(
        users_db, login='pend', password='pw', role=UserRole.VIEWER, is_active=False
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        admin = await _bearer(client, 'admin', 'pw')
        page = await client.get('/ui/users', headers=admin)
        assert page.status_code == 200
        assert 'pend' in page.text
        pend_id = next(
            u['id']
            for u in (await client.get('/api/v1/users', headers=admin)).json()
            if u['login'] == 'pend'
        )
        fragment = await client.post(
            f'/ui/users/{pend_id}/approve', data={'role': 'viewer'}, headers=admin
        )
        assert fragment.status_code == 200
        assert '<html' not in fragment.text.lower()
        assert await login(client, 'pend', 'pw') is not None


@pytest.mark.asyncio
async def test_ui_register_submit_shows_pending_message(
    users_db: async_sessionmaker,
) -> None:
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        assert (await client.get('/ui/register')).status_code == 200
        resp = await client.post(
            '/ui/register', data={'username': 'x', 'password': 'pw'}
        )
        assert resp.status_code == 200
        assert 'дождитесь одобрения' in resp.text


@pytest.mark.asyncio
async def test_ui_register_submit_shows_error(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='taken', password='pw', role=UserRole.VIEWER, is_active=False
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        resp = await client.post(
            '/ui/register', data={'username': 'taken', 'password': 'pw'}
        )
        assert resp.status_code == 400
        assert 'дождитесь одобрения' not in resp.text


@pytest.mark.asyncio
async def test_users_crud_404_on_missing(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    missing = '00000000-0000-0000-0000-0000000000aa'
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        admin = await _bearer(client, 'admin', 'pw')
        assert (
            await client.patch(
                f'/api/v1/users/{missing}', json={'role': 'admin'}, headers=admin
            )
        ).status_code == 404
        assert (
            await client.delete(f'/api/v1/users/{missing}', headers=admin)
        ).status_code == 404


@pytest.mark.asyncio
async def test_admin_create_duplicate_rejected(users_db: async_sessionmaker) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        admin = await _bearer(client, 'admin', 'pw')
        resp = await client.post(
            '/api/v1/users',
            json={'login': 'admin', 'password': 'pw'},
            headers=admin,
        )
        assert resp.status_code == 400


@pytest.mark.asyncio
async def test_ui_user_actions_deactivate_delete_create(
    users_db: async_sessionmaker,
) -> None:
    await seed_user(
        users_db, login='admin', password='pw', role=UserRole.ADMIN, is_active=True
    )
    await seed_user(
        users_db, login='live', password='pw', role=UserRole.VIEWER, is_active=True
    )
    async with asgi_client(build_app(users_db)) as client:  # type: ignore[arg-type]
        admin = await _bearer(client, 'admin', 'pw')
        live_id = next(
            u['id']
            for u in (await client.get('/api/v1/users', headers=admin)).json()
            if u['login'] == 'live'
        )
        # деактивация через htmx → вход закрывается
        assert (
            await client.post(f'/ui/users/{live_id}/deactivate', headers=admin)
        ).status_code == 200
        assert await login(client, 'live', 'pw') is None
        # создание через htmx-форму
        created = await client.post(
            '/ui/users/create',
            data={'login': 'fresh', 'password': 'pw', 'role': 'viewer'},
            headers=admin,
        )
        assert created.status_code == 200
        assert 'fresh' in created.text
        # удаление через htmx
        fresh_id = next(
            u['id']
            for u in (await client.get('/api/v1/users', headers=admin)).json()
            if u['login'] == 'fresh'
        )
        deleted = await client.post(f'/ui/users/{fresh_id}/delete', headers=admin)
        assert deleted.status_code == 200
        assert 'fresh' not in deleted.text


def test_create_app_fail_fast_without_user_store() -> None:
    settings = make_settings(api=ApiConfig(auth='users', secret=SECRET))
    deps = AngarionDeps(
        queue=MemoryQueue(),
        analytics=MemoryAnalytics(),
        registry=MemoryMessageRegistry(),
        state=MemoryStateStore(),
        cursors=MemoryCursorStore(),
        settings=settings,
    )
    with pytest.raises(ConfigError):
        create_app(deps)
