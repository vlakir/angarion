"""TelegramSender: троттлинг + FloodWait-повтор + transient-ретраи (фаза 4)."""

from __future__ import annotations

import pytest
from telegram_fakes import FakePool, FakeTelegramClient
from test_telegram_throttle import FakeClock

from angarion.adapters.telegram.client import FloodWaitError, TransientSendError
from angarion.adapters.telegram.sender import TelegramSender, as_peer
from angarion.domain.models import AccountRef, Address, DeliveryReceipt, OutboundMessage
from angarion.log import get_logger


def _msg(
    *,
    chat_id: str = '-100123',
    thread_id: str | None = None,
    text: str = 'привет',
    extra: dict[str, object] | None = None,
    account: str = 'main',
) -> OutboundMessage:
    return OutboundMessage(
        idempotency_key=f'k:{chat_id}:{text}',
        target=Address(messenger='telegram', chat_id=chat_id, thread_id=thread_id),
        send_via=AccountRef(messenger='telegram', account_id=account),
        text=text,
        extra=extra or {},
    )


def _sender(
    clients: dict[str, FakeTelegramClient],
    clock: FakeClock,
    **over: object,
) -> TelegramSender:
    params: dict[str, object] = {
        # щедрые лимиты — троттлинг не вмешивается (его тестируем отдельно)
        'chat_per_second': 1000.0,
        'account_per_minute': 1_000_000.0,
        'clock': clock.now,
        'sleep': clock.sleep,
    }
    params.update(over)
    return TelegramSender(pool=FakePool(clients), log=get_logger('test'), **params)  # type: ignore[arg-type]


def test_as_peer_numeric_and_username() -> None:
    assert as_peer('-1001234567890') == -1001234567890
    assert as_peer('123') == 123
    assert as_peer('@group') == '@group'


def test_requires_at_least_one_client() -> None:
    with pytest.raises(ValueError, match='хотя бы один клиент'):
        TelegramSender(pool=FakePool({}), log=get_logger('test'))


async def test_send_returns_receipt_with_external_id() -> None:
    client = FakeTelegramClient()
    receipt = await _sender({'main': client}, FakeClock()).send(_msg())
    assert isinstance(receipt, DeliveryReceipt)
    assert receipt.external_id == '1001'
    assert receipt.delivered_at.tzinfo is not None
    assert client.sent[0]['chat_id'] == -100123  # numeric peer


async def test_extra_parsed_into_send_params() -> None:
    client = FakeTelegramClient()
    await _sender({'main': client}, FakeClock()).send(
        _msg(extra={'parse_mode': 'md', 'silent': True, 'disable_preview': True})
    )
    call = client.sent[0]
    assert call['parse_mode'] == 'md'
    assert call['silent'] is True
    assert call['link_preview'] is False  # disable_preview → link_preview off


async def test_extra_defaults_when_absent() -> None:
    client = FakeTelegramClient()
    await _sender({'main': client}, FakeClock()).send(_msg())
    call = client.sent[0]
    assert call['parse_mode'] is None
    assert call['silent'] is False
    assert call['link_preview'] is True


async def test_unknown_extra_keys_ignored() -> None:
    client = FakeTelegramClient()
    await _sender({'main': client}, FakeClock()).send(
        _msg(extra={'totally_unknown': 'x'})
    )
    assert client.sent[0]['parse_mode'] is None


async def test_thread_id_becomes_reply_to() -> None:
    client = FakeTelegramClient()
    await _sender({'main': client}, FakeClock()).send(_msg(thread_id='55'))
    assert client.sent[0]['reply_to'] == 55


async def test_floodwait_waits_and_retries_same_message() -> None:
    client = FakeTelegramClient(send_effects=[FloodWaitError(seconds=7.0), None])
    clock = FakeClock()
    receipt = await _sender({'main': client}, clock).send(_msg())
    assert len(client.sent) == 2  # то же сообщение отправлено повторно
    assert client.sent[0]['text'] == client.sent[1]['text']
    assert clock.slept == [7.0]  # честно переждали ровно seconds
    assert receipt.external_id == '1001'


async def test_floodwait_exhausted_propagates() -> None:
    client = FakeTelegramClient(
        send_effects=[FloodWaitError(seconds=1.0), FloodWaitError(seconds=1.0)]
    )
    sender = _sender({'main': client}, FakeClock(), flood_max_retries=1)
    with pytest.raises(FloodWaitError):
        await sender.send(_msg())
    assert len(client.sent) == 2


async def test_transient_retried_then_succeeds() -> None:
    client = FakeTelegramClient(send_effects=[TransientSendError('сеть'), None])
    clock = FakeClock()
    receipt = await _sender({'main': client}, clock).send(_msg())
    assert len(client.sent) == 2
    assert receipt.external_id == '1001'


async def test_transient_exhausted_propagates() -> None:
    client = FakeTelegramClient(
        send_effects=[TransientSendError('1'), TransientSendError('2')]
    )
    sender = _sender({'main': client}, FakeClock(), transient_max_attempts=2)
    with pytest.raises(TransientSendError):
        await sender.send(_msg())
    assert len(client.sent) == 2


async def test_multi_account_routes_to_send_via_client() -> None:
    a = FakeTelegramClient()
    b = FakeTelegramClient()
    clock = FakeClock()
    sender = _sender({'acc_a': a, 'acc_b': b}, clock)
    await sender.send(_msg(account='acc_b', chat_id='-100222'))
    assert a.sent == []
    assert b.sent[0]['chat_id'] == -100222


async def test_chat_rate_throttles_second_send() -> None:
    client = FakeTelegramClient()
    clock = FakeClock()
    sender = _sender(
        {'main': client}, clock, chat_per_second=1.0, account_per_minute=1_000_000.0
    )
    await sender.send(_msg(text='1'))
    await sender.send(_msg(text='2'))  # тот же чат — ≤1/с → ждём 1с
    assert clock.slept == [1.0]


async def test_account_rate_throttles_across_chats() -> None:
    client = FakeTelegramClient()
    clock = FakeClock()
    # аккаунт ≤ 60/min = 1/с, бакет аккаунта capacity 60 (стартовый всплеск)
    sender = _sender(
        {'main': client}, clock, chat_per_second=1000.0, account_per_minute=60.0
    )
    for i in range(60):  # израсходовали стартовый бакет аккаунта
        await sender.send(_msg(chat_id=f'-10{i:04d}', text=str(i)))
    assert clock.slept == []
    await sender.send(_msg(chat_id='-19999', text='over'))  # 61-е → ждём ~1с
    assert clock.slept == [1.0]
