"""
HTTP-адаптер angarion (§12.5): FastAPI как второй driving-адаптер,
симметричный Telethon-listener'у. Публичный API — фабрика
``create_app`` и контейнер портов ``AngarionDeps``; типизированные
DI-зависимости поверх портов — в ``angarion.adapters.http.deps``.

Пакет живёт в extra ``web`` (FastAPI — инфраструктурная зависимость
M5, вне ядра §14.9): импортируется только при установленном
``angarion[web]``, ядро его не трогает.
"""

from angarion.adapters.http.app import create_app
from angarion.adapters.http.composition import build_settings_notifier, build_web_deps
from angarion.adapters.http.deps import AngarionDeps
from angarion.adapters.http.pages import PAGES_GROUP, load_pages
from angarion.adapters.http.templating import Page

__all__ = [
    'PAGES_GROUP',
    'AngarionDeps',
    'Page',
    'build_settings_notifier',
    'build_web_deps',
    'create_app',
    'load_pages',
]
