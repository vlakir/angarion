"""
Интеграционный контур Matrix (§13.2, M7 B4, T010): весь пайплайн на
локальном homeserver'е (Synapse/Conduit).

Маркер ``integration`` (default-skip; реквизиты — `.secrets`/env, стенд —
`tests/integration/matrix/`). Сценарии: new/edited/deleted сквозь
пайплайн, доставка из E2EE-комнаты, транзит медиа (`passthrough`).

Драйв источника — **тем же устройством, что и listener** (его nio-клиент):
для E2EE это снимает кросс-девайсный обмен ключами (устройство
расшифровывает собственное megolm-событие), проверяя именно наш путь
decrypt→map→deliver. Цель читает отдельный девайс-«читатель» (цель
незашифрована). Самоочистка — эфемерный стенд.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from matrix_harness import (
    build_plugins,
    build_settings,
    edit,
    logout_listener,
    make_driver,
    make_matrix_client,
    mirror_pipeline,
    post,
    post_media,
    redact,
    source_poster,
    wait_for,
)

from angarion.bootstrap import build_app

if TYPE_CHECKING:
    from pathlib import Path

    from conftest import MatrixEnv

pytestmark = pytest.mark.integration

DELIVERY_TIMEOUT = 45.0


async def _setup(
    matrix_env: MatrixEnv, tmp_path: Path, *, encrypted_source: bool, processor: str
) -> tuple[object, object, object, str, str]:
    """Читатель + MatrixClient + комнаты + запущенный app → (app, reader, client, src, dst)."""
    reader = await make_driver(
        homeserver=matrix_env.homeserver,
        user_id=matrix_env.user_id,
        password=matrix_env.password,
        store_path=str(tmp_path / 'reader-e2e'),
    )
    client = await make_matrix_client(
        homeserver=matrix_env.homeserver,
        user_id=matrix_env.user_id,
        password=matrix_env.password,
        store_path=str(tmp_path / 'listener-e2e'),
    )
    source = await reader.create_room(encrypted=encrypted_source)
    target = await reader.create_room()
    pipelines = {
        'mirror': mirror_pipeline(
            source_room=source, target_room=target, processor=processor
        )
    }
    settings = build_settings(
        homeserver=matrix_env.homeserver,
        user_id=matrix_env.user_id,
        db_path=tmp_path / 'app.db',
        queue_path=tmp_path / 'queue',
        pipelines=pipelines,
    )
    app = build_app(settings, plugins=build_plugins(client))
    await app.start()
    # дать listener-устройству досинхронизировать источник (членство +
    # для E2EE — стейт шифрования) перед публикацией
    await asyncio.sleep(3)
    return app, reader, client, source, target


async def test_new_edited_deleted_through_pipeline(
    matrix_env: MatrixEnv,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """new → edited → deleted сквозь пайплайн (комната A → комната B) live."""
    app, reader, client, source, target = await _setup(
        matrix_env, tmp_path, encrypted_source=False, processor='integration_echo'
    )
    poster = source_poster(client)
    try:
        event_id = await post(poster, source, f'{nonce} hello')
        assert await wait_for(
            lambda: reader.delivered(target, f'NEW {nonce} hello'),
            timeout=DELIVERY_TIMEOUT,
        ), 'NEW не доставлено'

        await edit(poster, source, event_id, f'{nonce} edited')
        assert await wait_for(
            lambda: reader.delivered(target, f'EDIT {nonce} edited'),
            timeout=DELIVERY_TIMEOUT,
        ), 'EDIT не доставлено'

        await redact(poster, source, event_id)
        assert await wait_for(
            lambda: reader.delivered(target, f'DEL {nonce} edited'),
            timeout=DELIVERY_TIMEOUT,
        ), 'DEL не доставлено'
    finally:
        await logout_listener(client)
        await app.stop()
        await reader.aclose()


async def test_delivery_from_encrypted_room(
    matrix_env: MatrixEnv,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """Сообщение из E2EE-комнаты расшифровано listener'ом и доставлено."""
    app, reader, client, source, target = await _setup(
        matrix_env, tmp_path, encrypted_source=True, processor='integration_echo'
    )
    poster = source_poster(client)
    try:
        await post(poster, source, f'{nonce} secret')
        assert await wait_for(
            lambda: reader.delivered(target, f'NEW {nonce} secret'),
            timeout=DELIVERY_TIMEOUT,
        ), 'сообщение из E2EE-комнаты не доставлено (UTD?)'
    finally:
        await logout_listener(client)
        await app.stop()
        await reader.aclose()


async def test_media_transit_through_pipeline(
    matrix_env: MatrixEnv,
    nonce: str,
    tmp_path: Path,
) -> None:
    """Вложение из источника транзитом доезжает в цель (passthrough)."""
    app, reader, client, source, target = await _setup(
        matrix_env, tmp_path, encrypted_source=False, processor='passthrough'
    )
    poster = source_poster(client)
    try:
        file_name = f'{nonce}.txt'
        await post_media(poster, source, file_name, b'angarion media payload')
        assert await wait_for(
            lambda: reader.delivered(target, file_name),
            timeout=DELIVERY_TIMEOUT,
        ), 'медиа не доставлено в цель'
    finally:
        await logout_listener(client)
        await app.stop()
        await reader.aclose()
