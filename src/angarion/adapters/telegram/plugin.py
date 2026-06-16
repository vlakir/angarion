"""
Контракт и сборка telegram-плагина (§12.10, §12.11 ТЗ; M3, T005).

Здесь и матрица возможностей со схемой секции ``[accounts.*]`` (фаза 1),
и итоговый объект ``AdapterPlugin`` с фабриками ``make_listener`` /
``make_sender`` (фаза 5, composition root) — значение entry point
``angarion.adapters:telegram``.

Фабрики собирают общий для listener'а и sender'а пул Telethon-клиентов
(``ClientRegistry``, §12.1): один клиент = одна сессия = один процесс.
Чтобы listener и sender делили **один** объект пула, он мемоизируется в
скретчпаде ``deps.shared`` (одна на сборку). Сессии читаются через
расшифровывающий декоратор ``EncryptedSessionStore`` (ключ
``settings.session_key``); пустой ключ при наличии сессий → fail-fast
при подключении (``ClientRegistry.connect_all`` на старте listener'а).

Модуль без ``from __future__ import annotations``: аннотации
pydantic-модели вычисляются в runtime; типы bootstrap/config в
сигнатурах фабрик — строками (TYPE_CHECKING).
"""

from typing import TYPE_CHECKING, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from angarion.adapters.telegram.listener import TelegramListener
from angarion.adapters.telegram.registry import ClientRegistry
from angarion.adapters.telegram.sender import TelegramSender
from angarion.adapters.telegram.session import EncryptedSessionStore
from angarion.domain.capabilities import AdapterCapabilities
from angarion.domain.plugin import AdapterPlugin
from angarion.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from angarion.bootstrap import AdapterDeps
    from angarion.config import EndpointConfig

_REGISTRY_KEY: Final = 'telegram.client_registry'
"""Ключ мемоизации общего ``ClientRegistry`` в ``deps.shared``."""

TELEGRAM_CAPABILITIES: Final = AdapterCapabilities(
    user_account=True,
    edit_events=True,
    delete_events=True,
    history_fetch=True,
    threads=True,
    push_transport='client',
)
"""Матрица возможностей платформы telegram (§12.10, FR «Регистрация»)."""


class TelegramAccountConfig(BaseModel):
    """
    Секция ``[accounts.*]`` платформы telegram (FR «Регистрация»).

    ``api_id``/``api_hash`` — секреты приложения Telegram, как правило
    подаются через env (``ANGARION_ACCOUNTS__<NAME>__API_ID`` и т.п.),
    а не открытым текстом в TOML (§17.7). ``account_id`` (ключ сессии в
    БД) — это имя самой секции ``[accounts.*]``, поэтому отдельным полем
    не дублируется. Ссылка на сессию резолвится по нему в bootstrap.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    messenger: Literal['telegram']
    api_id: int = Field(gt=0)
    api_hash: str = Field(min_length=1)


def _shared_registry(
    deps: 'AdapterDeps', accounts: 'Mapping[str, BaseModel]'
) -> ClientRegistry:
    """Общий ``ClientRegistry`` для listener+sender (мемо в ``deps.shared``)."""
    existing = deps.shared.get(_REGISTRY_KEY)
    if isinstance(existing, ClientRegistry):
        return existing
    credentials: dict[str, tuple[int, str]] = {}
    for name, raw in accounts.items():
        cfg = TelegramAccountConfig.model_validate(raw.model_dump())
        credentials[name] = (cfg.api_id, cfg.api_hash)
    session_store = EncryptedSessionStore(
        deps.storage.session, deps.settings.session_key
    )
    registry = ClientRegistry(credentials=credentials, session_store=session_store)
    deps.shared[_REGISTRY_KEY] = registry
    return registry


def _make_listener(
    deps: 'AdapterDeps',
    accounts: 'Mapping[str, BaseModel]',
    sources: 'Sequence[EndpointConfig]',
) -> TelegramListener:
    """Фабрика listener'а (§12.11): общий пул + проводка [catchup]/[telegram]."""
    catchup = deps.settings.catchup
    return TelegramListener(
        ingest=deps.ingest,
        pool=_shared_registry(deps, accounts),
        sources=sources,
        registry=deps.storage.registry,
        cursors=deps.storage.cursors,
        analytics=deps.storage.analytics,
        log=get_logger('angarion.telegram.listener'),
        catchup_enabled=catchup.enabled,
        catchup_max_messages=catchup.max_messages_per_source,
        catchup_max_age_days=catchup.max_age_days,
        catchup_interval=catchup.interval,
        buffer_soft_limit=deps.settings.telegram.live_buffer_soft_limit,
        media_policy=deps.settings.media,
    )


def _make_sender(
    deps: 'AdapterDeps', accounts: 'Mapping[str, BaseModel]'
) -> TelegramSender:
    """Фабрика sender'а (§12.11): тот же пул + пороги [telegram.sender]."""
    sender_cfg = deps.settings.telegram.sender
    return TelegramSender(
        pool=_shared_registry(deps, accounts),
        log=get_logger('angarion.telegram.sender'),
        chat_per_second=sender_cfg.chat_per_second,
        account_per_minute=sender_cfg.account_per_minute,
        flood_max_retries=sender_cfg.flood_max_retries,
        transient_max_attempts=sender_cfg.transient_max_attempts,
        backoff_base=sender_cfg.backoff_base,
        backoff_cap=sender_cfg.backoff_cap,
        runtime_config=deps.storage.runtime_config,
    )


PLUGIN: Final = AdapterPlugin(
    name='telegram',
    capabilities=TELEGRAM_CAPABILITIES,
    account_config_model=TelegramAccountConfig,
    make_listener=_make_listener,
    make_sender=_make_sender,
)
"""Значение entry point ``angarion.adapters:telegram`` (§12.11, фаза 5)."""
