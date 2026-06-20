"""
Плагин внутреннего транспорта ``internal`` (T037): sink-only адаптер
прямого провода цепочек пайплайнов (§12.11).

Регистрируется entry point'ом ``angarion.adapters:internal`` в
``pyproject.toml`` самой библиотеки. Listener'а нет (``make_listener=None``,
A2): «вход» приёмника наполняет сам sink через re-ingestion. Возможности —
только ``new`` (брокер-подобный, как Kafka/email в v2): подписка приёмника на
``edited``/``deleted`` отклоняется штатной §12.10-проверкой (A7); catch-up/
история неприменимы (``history_fetch=False``, A8).

Модуль без ``from __future__ import annotations``: аннотации pydantic-модели
вычисляются в runtime; типы bootstrap в сигнатурах фабрик — строками
(TYPE_CHECKING).
"""

from typing import TYPE_CHECKING, Final, Literal

from pydantic import BaseModel, ConfigDict

from angarion.adapters.internal.sink import InternalSink
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.models import INTERNAL_TRANSPORT
from angarion.domain.plugin import AdapterPlugin

if TYPE_CHECKING:
    from collections.abc import Mapping

    from angarion.bootstrap import AdapterDeps

INTERNAL_CAPABILITIES: Final = AdapterCapabilities(
    user_account=False,
    edit_events=False,
    delete_events=False,
    history_fetch=False,
    threads=False,
    push_transport='none',
)
"""Возможности внутреннего транспорта (§12.10): только ``new``, без истории.

``edit_events``/``delete_events`` = ``False`` — re-ingested запись всегда
``new`` (Q4), подписка приёмника на правки/удаления отклоняется fail-fast (A7).
``history_fetch`` = ``False`` — внешнего состояния/курсора нет (A8).
``push_transport='none'`` — listener'а нет, входящие наполняет сам sink.
"""


class InternalAccountConfig(BaseModel):
    """Секция ``[accounts.*]`` внутреннего транспорта: только ``transport``."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    transport: Literal['internal']


def _make_sender(
    deps: 'AdapterDeps', _accounts: 'Mapping[str, BaseModel]'
) -> InternalSink:
    """Фабрика sink'а (§12.11): замыкает исходящие на ``ingest`` (re-ingestion)."""
    return InternalSink(ingest=deps.ingest)


PLUGIN: Final = AdapterPlugin(
    name=INTERNAL_TRANSPORT,
    capabilities=INTERNAL_CAPABILITIES,
    account_config_model=InternalAccountConfig,
    make_listener=None,
    make_sender=_make_sender,
)
"""Значение entry point ``angarion.adapters:internal`` (sink-only, A2)."""
