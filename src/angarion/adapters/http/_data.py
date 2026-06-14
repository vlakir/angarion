"""
Сбор данных портов для встроенных представлений HTTP-адаптера —
общий слой JSON-роутера ``/api/v1`` (§12.5) и SSR Web UI (§12.6).

Источник истины топологии — конфиг + порты; ORM/Telethon сюда не
протекают (§12.5). Вынесено отдельным модулем, чтобы и JSON, и HTML
рендерились из одних данных (одни данные — два представления).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from angarion.domain.keys import make_source_key

if TYPE_CHECKING:
    from angarion.config import AngarionSettings


def source_keys(settings: AngarionSettings) -> list[str]:
    """Уникальные ключи источников из ``[pipelines.*]`` (порядок стабилен)."""
    keys: list[str] = []
    seen: set[str] = set()
    for cfg in settings.pipelines.values():
        for ep in cfg.sources:
            account = settings.accounts.get(ep.account)
            if account is None:  # ссылочная целостность — забота bootstrap
                continue
            key = make_source_key(
                account.messenger, ep.account, ep.chat_id, ep.thread_id
            )
            if key not in seen:
                seen.add(key)
                keys.append(key)
    return keys
