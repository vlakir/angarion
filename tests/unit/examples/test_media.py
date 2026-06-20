"""
Unit-тесты примера процессора ``media_note`` (``examples/media``).

Проверяем целевой сценарий медиа-части: процессор читает скачанный файл по
``MediaRef.local_path`` (фактический размер на диске) и аннотирует им подпись,
перенося само вложение транзитом; деградацию (нет файла / нет скачивания /
нет медиа) и идемпотентную раскладку по целям.

Каталог примера и общие фабрики подключены через ``conftest.py``. Файл вне
``--cov=src`` (пример не в ``src/``), но гоняется в CI.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app_factories import make_context, make_record, make_services, make_target
from media_processor import MEDIA_NOTE, MediaNoteProcessor

from angarion.domain.models import MediaRef, Verdict
from angarion.domain.ports import ProcessorPort

if TYPE_CHECKING:
    from pathlib import Path


def test_satisfies_processor_port() -> None:
    """Объект примера соответствует структурному контракту ``ProcessorPort``."""
    assert isinstance(MEDIA_NOTE, ProcessorPort)
    assert MEDIA_NOTE.name == 'media_note'


async def test_downloaded_media_annotated_with_on_disk_size(tmp_path: Path) -> None:
    """Вложение с local_path: подпись несёт фактический размер файла на диске."""
    blob = tmp_path / 'cat.jpg'
    blob.write_bytes(b'x' * 1234)
    event = make_record(
        text='смотри кота',
        media=[
            MediaRef(
                kind='photo',
                ref='-100123:42',
                file_name='cat.jpg',
                size=999,
                local_path=str(blob),
            )
        ],
    )
    result = await MEDIA_NOTE.process(event, make_context(), make_services())
    assert result.verdict is Verdict.DELIVER
    [msg] = result.outbound
    assert 'смотри кота' in msg.text
    assert '1234 Б на диске' in msg.text  # реальный размер, не метаданные (999)
    assert str(blob) in msg.text
    # вложение переносится транзитом (sender зальёт файл из local_path)
    assert msg.media == event.media


async def test_header_prepended_when_configured() -> None:
    """``processor_config.header`` добавляет строку-заголовок к подписи."""
    event = make_record(text='t', media=[MediaRef(kind='photo', ref='-100123:42')])
    ctx = make_context(settings={'header': '📥 зеркало'})
    result = await MEDIA_NOTE.process(event, ctx, make_services())
    assert result.outbound[0].text.startswith('📥 зеркало\n')


async def test_media_without_local_path_uses_fast_path_note() -> None:
    """Скачивание выключено (нет local_path): подпись помечает fast-path."""
    event = make_record(
        text='', media=[MediaRef(kind='video', ref='-100123:42', size=2048)]
    )
    result = await MEDIA_NOTE.process(event, make_context(), make_services())
    text = result.outbound[0].text
    assert 'fast-path' in text
    assert '2048 Б' in text
    assert result.outbound[0].media == event.media


async def test_missing_local_file_degrades_to_note(tmp_path: Path) -> None:
    """local_path задан, но файла нет: доставка не падает, подпись помечает это."""
    gone = tmp_path / 'gone.bin'  # не создаём
    event = make_record(
        text='x', media=[MediaRef(kind='document', ref='-1:2', local_path=str(gone))]
    )
    result = await MEDIA_NOTE.process(event, make_context(), make_services())
    assert result.verdict is Verdict.DELIVER
    assert 'файл недоступен' in result.outbound[0].text


async def test_no_media_passthrough_text() -> None:
    """Событие без медиа с текстом — пересылается как текст."""
    event = make_record(text='просто текст')
    result = await MEDIA_NOTE.process(event, make_context(), make_services())
    assert result.verdict is Verdict.DELIVER
    assert result.outbound[0].text == 'просто текст'
    assert result.outbound[0].media == []


async def test_no_media_no_text_dropped() -> None:
    """Ни текста, ни медиа — drop (нечего пересылать)."""
    event = make_record(text=None)
    result = await MEDIA_NOTE.process(event, make_context(), make_services())
    assert result.verdict is Verdict.DROP
    assert not result.outbound


async def test_one_outbound_per_target_distinct_keys() -> None:
    """Раскладка по целям: на каждую — свой ``idempotency_key``."""
    event = make_record(text='t', media=[MediaRef(kind='photo', ref='-100123:42')])
    ctx = make_context(targets=[make_target('-100001'), make_target('-100002')])
    result = await MEDIA_NOTE.process(event, ctx, make_services())
    assert len(result.outbound) == 2
    keys = {m.idempotency_key for m in result.outbound}
    assert len(keys) == 2  # ключи различаются по цели/порядковому номеру


def test_config_model_exposed() -> None:
    """``config_model`` отдаёт схему для fail-fast на старте."""
    model = MediaNoteProcessor().config_model()
    assert model is not None
    assert 'header' in model.model_fields
