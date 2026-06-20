"""
Пример процессора с доступом к скачанному медиа (M7, часть A; задача T034).

Демонстрирует целевой сценарий медиа-части ТЗ: при ``[media] download = true``
принимающий аккаунт скачивает вложение при ingest и проставляет
``MediaRef.local_path`` — а процессор **читает локальный файл** (здесь —
его фактический размер на диске) и аннотирует им подпись пересылаемого
сообщения. Само вложение переносится транзитом (``OutboundRecord.media``):
sender при наличии ``local_path`` грузит файл напрямую (работает и кросс-
аккаунт), иначе переотправляет по платформенной ссылке (fast-path).

Это **пример** (код в ``examples/``), а не встроенный процессор (``src/``):
его задача — показать доступ процессора к контенту скачанного вложения.
Имя модуля — ``media_processor`` (не ``processor``): тесты примеров гоняются
в одной pytest-сессии, уникальное имя избегает коллизии ``sys.modules`` с
модулем ``processor`` примера ``digest``.

Модуль без ``from __future__ import annotations``: аннотации Pydantic-моделей
вычисляются в runtime.
"""

import asyncio
import os
from typing import Final

from pydantic import BaseModel, ConfigDict

from angarion.domain.models import (
    MediaRef,
    OutboundRecord,
    PipelineContextData,
    ProcessingResult,
    ProcessorServices,
    Record,
    Verdict,
)
from angarion.domain.ports import ProcessorPort


class MediaNoteConfig(BaseModel):
    """
    ``processor_config`` примера (всё опционально).

    ``header`` — строка-заголовок перед подписью (демонстрирует проводку
    ``processor_config``); пустая — заголовок не добавляется.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    header: str = ''


async def _on_disk_size(local_path: str) -> int | None:
    """
    Фактический размер файла на диске — реальный доступ процессора к
    контенту скачанного вложения. ``os`` синхронен, поэтому offload в
    ``to_thread`` (event loop не блокируется); файла нет → ``None``.
    """
    try:
        return await asyncio.to_thread(os.path.getsize, local_path)
    except OSError:
        return None


def _describe(media: MediaRef, on_disk: int | None) -> str:
    """Человекочитаемая аннотация одного вложения для подписи."""
    name = media.file_name or media.kind
    if media.local_path is not None and on_disk is not None:
        return f'{media.kind} «{name}» — {on_disk} Б на диске ({media.local_path})'
    if media.local_path is not None:
        return f'{media.kind} «{name}» — файл недоступен ({media.local_path})'
    size = media.size if media.size is not None else '?'
    return f'{media.kind} «{name}» — {size} Б, fast-path (без скачивания)'


class MediaNoteProcessor(BaseModel):
    """
    Аннотирует пересылаемое сообщение метаданными скачанных вложений и
    переносит само медиа транзитом на каждую цель (§10.1).

    Идемпотентность под at-least-once — за счёт ``make_idempotency_key``
    (повторная доставка того же события даёт тот же ключ выхода, дубль
    гасит outbox); своё состояние процессору не нужно.

    Конструкция композиции (A-2): JSON-контракт DTO на неё не действует.
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str = 'media_note'

    def config_model(self) -> type[BaseModel] | None:
        """Схема ``processor_config`` (опциональный ``header``)."""
        return MediaNoteConfig

    async def process(
        self,
        event: Record,
        ctx: PipelineContextData,
        svc: ProcessorServices,
    ) -> ProcessingResult:
        """Собрать подпись (текст + аннотация вложений) и переслать медиа."""
        cfg = MediaNoteConfig.model_validate(ctx.settings)
        if not event.media:
            if event.text is None:
                return ProcessingResult(
                    verdict=Verdict.DROP, note='media_note: без текста и медиа'
                )
            caption = event.text
        else:
            caption = await self._caption(event, cfg, svc)
        outbound = [
            OutboundRecord(
                idempotency_key=svc.make_idempotency_key(event, spec.target, n),
                target=spec.target,
                send_via=spec.send_via,
                text=caption,
                media=list(event.media),
            )
            for n, spec in enumerate(ctx.targets)
        ]
        return ProcessingResult(verdict=Verdict.DELIVER, outbound=outbound)

    @staticmethod
    async def _caption(
        event: Record, cfg: MediaNoteConfig, svc: ProcessorServices
    ) -> str:
        """Подпись для события с вложениями: header + текст + аннотация медиа."""
        notes: list[str] = []
        for media in event.media:
            on_disk = (
                await _on_disk_size(media.local_path)
                if media.local_path is not None
                else None
            )
            svc.log.info(
                'media_seen',
                kind=media.kind,
                local_path=media.local_path,
                on_disk_bytes=on_disk,
            )
            notes.append(_describe(media, on_disk))
        parts = [part for part in (cfg.header, event.text) if part]
        parts.append('📎 ' + ' | '.join(notes))
        return '\n'.join(parts)


MEDIA_NOTE: Final[ProcessorPort] = MediaNoteProcessor()
"""Готовый объект процессора — ``run.py`` регистрирует его перед запуском
(``angarion.application.processors.register``); §10.1."""
