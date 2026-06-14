"""
Подключение каталога примера ``examples/digest`` и общих фабрик к sys.path.

Тестовые каталоги — не пакеты (плоский sys.path, как в остальных
``tests/``): conftest добавляет каталог примера (чтобы импортировать
``processor``) и ``tests/unit/application`` (переиспользуем ``app_factories``
вместо дублирования фабрик событий/контекста/сервисов).
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_EXAMPLE_DIR = _HERE.parents[3] / 'examples' / 'digest'
_APP_FACTORIES_DIR = _HERE.parents[1] / 'application'

for _path in (_EXAMPLE_DIR, _APP_FACTORIES_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
