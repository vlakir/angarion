"""
Лаунчер примера ``media``: регистрирует кастомный процессор и запускает
конвейер.

``angarion run`` находит процессоры только через entry points
``angarion.processors`` (механизм для устанавливаемых пакетов). Кастомный
процессор примера живёт одним файлом без упаковки, поэтому регистрируем его
в процессе — ``register(MEDIA_NOTE)`` — и запускаем штатный run-loop
библиотеки (``cmd_run``: graceful shutdown по SIGINT/SIGTERM). Каталог
скрипта Python кладёт в ``sys.path`` сам, поэтому ``media_processor``
импортируется напрямую (запускать из каталога примера — это делает
``run.sh``).
"""

import asyncio

from media_processor import MEDIA_NOTE

from angarion.application.processors import register
from angarion.cli import cmd_run
from angarion.config import load_settings


def main() -> None:
    """Зарегистрировать ``media_note`` и запустить конвейер по ``app.toml``."""
    register(MEDIA_NOTE)
    asyncio.run(cmd_run(load_settings('app.toml')))


if __name__ == '__main__':
    main()
