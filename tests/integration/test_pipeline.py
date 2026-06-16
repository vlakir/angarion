"""
Интеграционный контур §13.2 (T009 / M6): весь пайплайн на реальном
Telegram-аккаунте и трёх тестовых группах.

Маркер ``integration`` (default-skip, реквизиты — `.secrets`). Сценарии:
new/edited/deleted сквозь пайплайн, multicast в две цели, дедуп при
повторном catch-up, рестарт с непустой очередью, петлевой guard
``source == target``.

**Драйв — через catch-up §9.3, не через live-апдейты.** Приём собственных
live-апдейтов одним аккаунтом в тихую супергруппу ненадёжен (per-channel
``pts`` gap в Telethon — артефакт self-send-драйва, не боевого пути, где
зеркалятся входящие от других). Поэтому драйвер пишет/правит/удаляет в
источнике отдельным ``idle_client`` пока app остановлен, а пайплайн
поднимает события детерминированной history-реконсиляцией (``iter_messages``)
при старте. Перед каждым сценарием источник чистится (``purge_recent``),
чтобы catch-up видел только сообщения теста.

Темп — с запасом под лимиты Telegram; каждый тест прибирает за собой.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from harness import (
    IntegrationPool,
    build_app,
    build_plugins,
    build_settings,
    cleanup,
    count_with,
    delete,
    delivered,
    edit,
    idle_client,
    mirror_pipeline,
    post,
    purge_recent,
    read_texts,
    wait_for,
)

if TYPE_CHECKING:
    from pathlib import Path

    from angarion.bootstrap import AngarionApp
    from conftest import TgEnv

pytestmark = pytest.mark.integration

ACCOUNT = 'main'
DELIVERY_TIMEOUT = 60.0


def _pool(tg_env: TgEnv, session: str) -> IntegrationPool:
    return IntegrationPool(ACCOUNT, tg_env.api_id, tg_env.api_hash, session)


def _settings(
    tg_env: TgEnv,
    tmp_path: Path,
    pipelines: dict[str, object],
    *,
    sender_chat_per_second: float | None = None,
) -> object:
    return build_settings(
        account_id=ACCOUNT,
        api_id=tg_env.api_id,
        api_hash=tg_env.api_hash,
        db_path=tmp_path / 'app.db',
        queue_path=tmp_path / 'queue',
        pipelines=pipelines,  # type: ignore[arg-type]
        catchup_enabled=True,
        sender_chat_per_second=sender_chat_per_second,
    )


async def test_new_edited_deleted_through_pipeline(
    tg_env: TgEnv,
    tg_session_string: str,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """new → edited → deleted сквозь пайплайн (источник A → цель B) через catch-up."""
    src, dst = tg_env.group_a, tg_env.group_b
    pool = _pool(tg_env, tg_session_string)
    pipelines = {
        'mirror': mirror_pipeline(
            account_id=ACCOUNT, source_chat=str(src), target_chats=[str(dst)]
        )
    }
    settings = _settings(tg_env, tmp_path, pipelines)

    # --- 0. Чистый старт + публикация двух сообщений (драйвер, app выключен) ---
    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await purge_recent(drv, [src, dst])
        kept_id = await post(drv, src, f'{nonce} kept')
        gone_id = await post(drv, src, f'{nonce} gone')

    # --- 1. Старт: catch-up эмитит оба новых → доставка в B ---
    app1: AngarionApp = build_app(settings, plugins=build_plugins(pool))
    await app1.start()
    try:
        assert await wait_for(
            lambda: delivered(pool.raw, dst, f'NEW {nonce} kept'),
            timeout=DELIVERY_TIMEOUT,
        ), 'NEW kept не доставлено'
        assert await wait_for(
            lambda: delivered(pool.raw, dst, f'NEW {nonce} gone'),
            timeout=DELIVERY_TIMEOUT,
        ), 'NEW gone не доставлено'
    finally:
        await app1.stop()

    # --- 2. Простой: правка одного, удаление другого, публикация нового ---
    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await edit(drv, src, kept_id, f'{nonce} kept v2')
        await delete(drv, src, gone_id)
        await post(drv, src, f'{nonce} fresh')

    # --- 3. Рестарт: catch-up §9.3 эмитит edited/deleted/new за простой ---
    pool2 = _pool(tg_env, tg_session_string)
    app2: AngarionApp = build_app(settings, plugins=build_plugins(pool2))
    await app2.start()
    try:
        assert await wait_for(
            lambda: delivered(pool2.raw, dst, f'EDIT {nonce} kept v2'),
            timeout=DELIVERY_TIMEOUT,
        ), 'EDIT не доставлено'
        assert await wait_for(
            lambda: delivered(pool2.raw, dst, f'NEW {nonce} fresh'),
            timeout=DELIVERY_TIMEOUT,
        ), 'NEW fresh не доставлено'
        assert await wait_for(
            lambda: delivered(pool2.raw, dst, f'DEL {nonce} gone'),
            timeout=DELIVERY_TIMEOUT,
        ), 'DEL не доставлено'
    finally:
        if pool2.connected:
            await cleanup(pool2.raw, [src, dst], nonce)
        await app2.stop()


async def test_multicast_two_targets(
    tg_env: TgEnv,
    tg_session_string: str,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """Multicast: один источник A → две цели B и C (вариант 1, 3 группы)."""
    src, dst1, dst2 = tg_env.group_a, tg_env.group_b, tg_env.group_c
    pool = _pool(tg_env, tg_session_string)
    pipelines = {
        'fanout': mirror_pipeline(
            account_id=ACCOUNT,
            source_chat=str(src),
            target_chats=[str(dst1), str(dst2)],
        )
    }
    settings = _settings(tg_env, tmp_path, pipelines)

    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await purge_recent(drv, [src, dst1, dst2])
        await post(drv, src, f'{nonce} fanout')

    app: AngarionApp = build_app(settings, plugins=build_plugins(pool))
    await app.start()
    try:
        assert await wait_for(
            lambda: delivered(pool.raw, dst1, f'NEW {nonce} fanout'),
            timeout=DELIVERY_TIMEOUT,
        ), 'не доставлено в цель B'
        assert await wait_for(
            lambda: delivered(pool.raw, dst2, f'NEW {nonce} fanout'),
            timeout=DELIVERY_TIMEOUT,
        ), 'не доставлено в цель C'
    finally:
        if pool.connected:
            await cleanup(pool.raw, [src, dst1, dst2], nonce)
        await app.stop()


async def test_repeat_catchup_dedups(
    tg_env: TgEnv,
    tg_session_string: str,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """Повторный catch-up без изменений в источнике не плодит доставок."""
    src, dst = tg_env.group_a, tg_env.group_b
    pool1 = _pool(tg_env, tg_session_string)
    pipelines = {
        'mirror': mirror_pipeline(
            account_id=ACCOUNT, source_chat=str(src), target_chats=[str(dst)]
        )
    }
    settings = _settings(tg_env, tmp_path, pipelines)

    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await purge_recent(drv, [src, dst])
        await post(drv, src, f'{nonce} once')

    app1: AngarionApp = build_app(settings, plugins=build_plugins(pool1))
    await app1.start()
    try:
        assert await wait_for(
            lambda: delivered(pool1.raw, dst, f'NEW {nonce} once'),
            timeout=DELIVERY_TIMEOUT,
        )
    finally:
        await app1.stop()

    # Рестарт без изменений в источнике → повторный catch-up = no-op (дедуп)
    pool2 = _pool(tg_env, tg_session_string)
    app2: AngarionApp = build_app(settings, plugins=build_plugins(pool2))
    await app2.start()
    try:
        # дать повторному catch-up шанс (ошибочно) продублировать
        assert await wait_for(
            lambda: delivered(pool2.raw, dst, f'NEW {nonce} once'),
            timeout=DELIVERY_TIMEOUT,
        )
        texts = await read_texts(pool2.raw, dst, limit=30)
        assert count_with(texts, f'NEW {nonce} once') == 1, (
            'повторный catch-up продублировал доставку'
        )
    finally:
        if pool2.connected:
            await cleanup(pool2.raw, [src, dst], nonce)
        await app2.stop()


async def test_restart_with_nonempty_queue(
    tg_env: TgEnv,
    tg_session_string: str,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """Рестарт с непустой очередью: persistqueue переживает перезапуск."""
    src, dst = tg_env.group_a, tg_env.group_b
    tags = ('q1', 'q2', 'q3', 'q4')
    pipelines = {
        'mirror': mirror_pipeline(
            account_id=ACCOUNT, source_chat=str(src), target_chats=[str(dst)]
        )
    }
    settings = _settings(tg_env, tmp_path, pipelines)

    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await purge_recent(drv, [src, dst])
        for tag in tags:
            await post(drv, src, f'{nonce} {tag}')

    # --- 1. catch-up ингестит все 4; sender (≤ 1/с на чат) не успевает
    #         доставить всё до быстрой остановки → часть оседает в
    #         persistqueue/outbox и должна пережить рестарт ---
    pool1 = _pool(tg_env, tg_session_string)
    app1: AngarionApp = build_app(settings, plugins=build_plugins(pool1))
    await app1.start()
    try:
        await asyncio.sleep(1.5)  # коротко: часть доставится, часть — нет
        before = count_with(
            await read_texts(pool1.raw, dst, limit=30), f'NEW {nonce} q'
        )
        assert before < len(tags), (
            f'всё доставлено до рестарта ({before}/{len(tags)}) — '
            f'очередь не была непустой, сценарий не воспроизведён'
        )
    finally:
        await app1.stop()

    # --- 2. Рестарт на том же db+queue: накопленное в очереди дослыкается ---
    pool2 = _pool(tg_env, tg_session_string)
    app2: AngarionApp = build_app(settings, plugins=build_plugins(pool2))
    await app2.start()

    async def _all_delivered() -> bool:
        texts = await read_texts(pool2.raw, dst, limit=40)
        return all(count_with(texts, f'NEW {nonce} {tag}') >= 1 for tag in tags)

    try:
        assert await wait_for(_all_delivered, timeout=DELIVERY_TIMEOUT), (
            'накопленное в очереди не доставлено после рестарта'
        )
        await asyncio.sleep(5.0)
        texts = await read_texts(pool2.raw, dst, limit=40)
        for tag in tags:
            assert count_with(texts, f'NEW {nonce} {tag}') == 1, (
                f'доставка {tag} потеряна или задвоена при рестарте'
            )
    finally:
        if pool2.connected:
            await cleanup(pool2.raw, [src, dst], nonce)
        await app2.stop()


async def test_loop_guard_source_equals_target(
    tg_env: TgEnv,
    tg_session_string: str,
    nonce: str,
    echo_registered: None,
    tmp_path: Path,
) -> None:
    """Вариант 2: источник = одна из целей; петля собственных доставок гасится."""
    src, dst = tg_env.group_a, tg_env.group_b  # цели: {A(=src), B}
    pool1 = _pool(tg_env, tg_session_string)
    pipelines = {
        'self': mirror_pipeline(
            account_id=ACCOUNT,
            source_chat=str(src),
            target_chats=[str(src), str(dst)],
        )
    }
    settings = _settings(tg_env, tmp_path, pipelines)

    async with idle_client(tg_env.api_id, tg_env.api_hash, tg_session_string) as drv:
        await purge_recent(drv, [src, dst])
        await post(drv, src, f'{nonce} loop')

    # --- 1. catch-up доставляет в A и B; доставка в A помечена guard'ом ---
    app1: AngarionApp = build_app(settings, plugins=build_plugins(pool1))
    await app1.start()
    try:
        assert await wait_for(
            lambda: delivered(pool1.raw, dst, f'NEW {nonce} loop'),
            timeout=DELIVERY_TIMEOUT,
        ), 'не доставлено в цель B'
        assert await wait_for(
            lambda: delivered(pool1.raw, src, f'NEW {nonce} loop'),
            timeout=DELIVERY_TIMEOUT,
        ), 'не доставлено в цель A (=источник)'
    finally:
        await app1.stop()

    # --- 2. Рестарт: catch-up пере-сканирует A; собственная доставка
    #         в A не порождает повторную (guard пометил её dedup) ---
    pool2 = _pool(tg_env, tg_session_string)
    app2: AngarionApp = build_app(settings, plugins=build_plugins(pool2))
    await app2.start()
    try:
        assert await wait_for(
            lambda: delivered(pool2.raw, dst, f'NEW {nonce} loop'),
            timeout=DELIVERY_TIMEOUT,
        )
        texts = await read_texts(pool2.raw, src, limit=50)
        delivered_count = count_with(texts, f'NEW {nonce} loop')
        assert delivered_count == 1, (
            f'петля не погашена: доставок в источник {delivered_count}'
        )
    finally:
        if pool2.connected:
            await cleanup(pool2.raw, [src, dst], nonce)
        await app2.stop()
