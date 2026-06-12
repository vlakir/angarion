"""Контрактный набор ``MessageSinkPort`` (§5 ТЗ; FR-6, SC-5)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from angarion.domain.models import DeliveryReceipt
from angarion.testing.factories import make_outbound

if TYPE_CHECKING:
    from angarion.domain.ports import MessageSinkPort


class MessageSinkContract:
    """
    Поведенческая спецификация sink'а: ``send`` возвращает
    ``DeliveryReceipt`` со временем доставки в UTC (§17.4).
    """

    pytestmark = pytest.mark.asyncio

    @pytest.fixture
    def sink(self) -> MessageSinkPort:
        raise NotImplementedError

    async def test_send_returns_receipt_in_utc(self, sink: MessageSinkPort) -> None:
        receipt = await sink.send(make_outbound())
        assert isinstance(receipt, DeliveryReceipt)
        assert receipt.delivered_at.utcoffset() == timedelta(0)
