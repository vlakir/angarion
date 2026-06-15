"""Контрактный набор ``CommandOutboxPort`` (§12.9 ТЗ; FR-5, T024)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from angarion.domain.models import CommandKind, CommandStatus

if TYPE_CHECKING:
    from angarion.domain.ports import CommandOutboxPort


class CommandOutboxContract:
    """
    Поведенческая спецификация командного outbox (§12.9): мост
    api→pipeline. ``put`` кладёт ``pending``-команду; ``take`` атомарно
    захватывает ``pending`` → ``taken`` (at-least-once, защита по
    статусу); ``mark_done`` / ``mark_failed`` терминально закрывают
    захваченную команду; ``get`` читает; ``prune`` чистит терминальные.

    Реализация подключается переопределением фикстуры ``command_outbox``.
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def command_outbox(self) -> CommandOutboxPort:
        raise NotImplementedError

    async def test_put_returns_pending_command(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        command = await command_outbox.put(
            CommandKind.CATCHUP, payload={'source_key': 'tg:chat'}
        )
        assert command.kind is CommandKind.CATCHUP
        assert command.payload == {'source_key': 'tg:chat'}
        assert command.status is CommandStatus.PENDING
        assert command.executed_at is None
        assert await command_outbox.get(command.uid) == command

    async def test_put_without_payload_defaults_empty(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        command = await command_outbox.put(CommandKind.RESTART_PIPELINE)
        assert command.payload == {}

    async def test_take_claims_pending(self, command_outbox: CommandOutboxPort) -> None:
        put = await command_outbox.put(CommandKind.NOTIFY)
        taken = await command_outbox.take()
        assert [c.uid for c in taken] == [put.uid]
        assert taken[0].status is CommandStatus.TAKEN
        # повторный take не выдаёт уже захваченную команду
        assert await command_outbox.take() == []

    async def test_take_is_fifo_and_respects_limit(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        first = await command_outbox.put(CommandKind.NOTIFY, payload={'n': 1})
        second = await command_outbox.put(CommandKind.NOTIFY, payload={'n': 2})
        await command_outbox.put(CommandKind.NOTIFY, payload={'n': 3})
        taken = await command_outbox.take(limit=2)
        assert [c.uid for c in taken] == [first.uid, second.uid]

    async def test_empty_take_returns_empty(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        assert await command_outbox.take() == []

    async def test_concurrent_take_claims_each_command_once(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        total = 5
        for n in range(total):
            await command_outbox.put(CommandKind.NOTIFY, payload={'n': n})
        first, second = await asyncio.gather(
            command_outbox.take(limit=total), command_outbox.take(limit=total)
        )
        claimed = [c.uid for c in (*first, *second)]
        assert sorted(claimed) == sorted(set(claimed))  # без дублей
        assert len(claimed) == total

    async def test_mark_done_is_terminal(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        put = await command_outbox.put(CommandKind.NOTIFY)
        await command_outbox.take()
        await command_outbox.mark_done(put.uid, result='delivered')
        stored = await command_outbox.get(put.uid)
        assert stored is not None
        assert stored.status is CommandStatus.DONE
        assert stored.result == 'delivered'
        assert stored.executed_at is not None

    async def test_mark_failed_visible_with_error(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        put = await command_outbox.put(CommandKind.NOTIFY)
        await command_outbox.take()
        await command_outbox.mark_failed(put.uid, error='sink down')
        stored = await command_outbox.get(put.uid)
        assert stored is not None
        assert stored.status is CommandStatus.FAILED
        assert stored.error == 'sink down'
        assert stored.executed_at is not None

    async def test_mark_without_take_is_noop(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        put = await command_outbox.put(CommandKind.NOTIFY)
        await command_outbox.mark_done(put.uid)  # ещё pending, не taken
        stored = await command_outbox.get(put.uid)
        assert stored is not None
        assert stored.status is CommandStatus.PENDING

    async def test_mark_done_twice_is_noop(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        put = await command_outbox.put(CommandKind.NOTIFY)
        await command_outbox.take()
        await command_outbox.mark_done(put.uid, result='first')
        await command_outbox.mark_failed(put.uid, error='second')  # уже done
        stored = await command_outbox.get(put.uid)
        assert stored is not None
        assert stored.status is CommandStatus.DONE
        assert stored.result == 'first'

    async def test_get_unknown_returns_none(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        await command_outbox.put(CommandKind.NOTIFY)
        assert await command_outbox.get(uuid4()) is None

    async def test_prune_removes_terminal_only(
        self, command_outbox: CommandOutboxPort
    ) -> None:
        done = await command_outbox.put(CommandKind.NOTIFY)
        await command_outbox.take()
        await command_outbox.mark_done(done.uid)
        pending = await command_outbox.put(CommandKind.NOTIFY)
        removed = await command_outbox.prune(datetime.now(UTC) + timedelta(minutes=1))
        assert removed == 1
        assert await command_outbox.get(done.uid) is None
        assert await command_outbox.get(pending.uid) is not None
