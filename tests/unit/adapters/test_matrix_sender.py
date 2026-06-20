"""
MatrixSender: доставка OutboundRecord в Matrix (SinkPort, M7 B3).
Контракт sink + текст/медиа/тред + rate-limit/transient ретраи на fake.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from matrix_fakes import FakeMatrixClient

from angarion.adapters.matrix.client import MatrixRateLimitError, MatrixTransientError
from angarion.adapters.matrix.sender import MatrixSender
from angarion.domain.errors import ConfigError
from angarion.domain.models import MediaRef
from angarion.log import get_logger
from angarion.testing.factories import make_endpoint, make_outbound
from angarion.testing.sink_contract import SinkContract

if TYPE_CHECKING:
    from angarion.adapters.matrix.client import MatrixClientPort


async def _no_sleep(_seconds: float) -> None:
    return


def _sender(client: FakeMatrixClient, **kw: object) -> MatrixSender:
    return MatrixSender(
        clients=cast('dict[str, MatrixClientPort]', {'acc1': client}),
        log=get_logger('test.matrix.sender'),
        sleep=_no_sleep,
        **kw,  # type: ignore[arg-type]
    )


class TestMatrixSink(SinkContract):
    @pytest.fixture
    def sink(self) -> MatrixSender:
        return _sender(FakeMatrixClient())


class TestSend:
    async def test_text_send_returns_event_id(self) -> None:
        client = FakeMatrixClient()
        receipt = await _sender(client).send(make_outbound(text='привет'))
        assert receipt.external_id == '$sent-1'
        assert client.sent[0]['body'] == 'привет'
        assert client.sent[0]['media'] is False

    async def test_restores_client_before_send(self) -> None:
        client = FakeMatrixClient()
        await _sender(client).send(make_outbound())
        assert client.restored == 1  # роль-сплит: sender сам поднимает клиента

    async def test_unknown_account_raises(self) -> None:
        sender = _sender(FakeMatrixClient())
        with pytest.raises(ConfigError, match='нет Matrix-клиента'):
            await sender.send(make_outbound(send_via=make_outbound().send_via.model_copy(
                update={'account_id': 'ghost'}
            )))

    async def test_media_routed_to_send_media(self) -> None:
        client = FakeMatrixClient()
        out = make_outbound(
            text='подпись',
            media=[MediaRef(kind='photo', ref='mxc://s/abc', file_name='p.jpg')],
        )
        await _sender(client).send(out)
        sent = client.sent[0]
        assert sent['media'] is True
        assert sent['kind'] == 'photo'
        assert sent['mxc_ref'] == 'mxc://s/abc'
        assert sent['body'] == 'подпись'

    async def test_media_local_path_passed(self) -> None:
        client = FakeMatrixClient()
        out = make_outbound(
            text='',
            media=[MediaRef(kind='document', local_path='/tmp/f.bin', file_name='f.bin')],
        )
        await _sender(client).send(out)
        sent = client.sent[0]
        assert sent['local_path'] == '/tmp/f.bin'
        assert sent['body'] == 'f.bin'  # пустой text → имя файла

    async def test_thread_id_becomes_thread_root(self) -> None:
        client = FakeMatrixClient()
        out = make_outbound(target=make_endpoint(address='!r:s', thread_id='$root'))
        await _sender(client).send(out)
        assert client.sent[0]['thread_root'] == '$root'


class TestResilience:
    async def test_rate_limit_waited_and_retried(self) -> None:
        client = FakeMatrixClient(send_effects=[MatrixRateLimitError(0.01), None])
        receipt = await _sender(client).send(make_outbound())
        assert len(client.sent) == 2  # переотправили то же
        assert receipt.external_id == '$sent-1'  # успех на второй попытке

    async def test_transient_retried_then_succeeds(self) -> None:
        client = FakeMatrixClient(send_effects=[MatrixTransientError('boom'), None])
        await _sender(client).send(make_outbound())
        assert len(client.sent) == 2

    async def test_transient_exhausted_raises(self) -> None:
        client = FakeMatrixClient(
            send_effects=[MatrixTransientError('x')] * 3
        )
        with pytest.raises(MatrixTransientError):
            await _sender(client, transient_max_attempts=3).send(make_outbound())
        assert len(client.sent) == 3

    async def test_rate_limit_exhausted_raises(self) -> None:
        client = FakeMatrixClient(send_effects=[MatrixRateLimitError(0.0)] * 3)
        with pytest.raises(MatrixRateLimitError):
            await _sender(client, rate_limit_max_retries=2).send(make_outbound())
