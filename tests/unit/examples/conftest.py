"""
Подключение каталогов примеров и общих фабрик к sys.path.

Тестовые каталоги — не пакеты (плоский sys.path, как в остальных
``tests/``): conftest добавляет каталоги примеров (чтобы импортировать их
модули процессоров — ``processor`` у ``digest``, ``media_processor`` у
``media``; имена уникальны, поэтому ``sys.modules`` не конфликтует) и
``tests/unit/application`` (переиспользуем ``app_factories`` вместо
дублирования фабрик событий/контекста/сервисов).
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_EXAMPLES = _HERE.parents[3] / 'examples'
_APP_FACTORIES_DIR = _HERE.parents[1] / 'application'

for _path in (_EXAMPLES / 'digest', _EXAMPLES / 'media', _APP_FACTORIES_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
