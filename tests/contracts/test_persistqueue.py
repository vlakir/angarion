"""
Контрактный набор ``EventQueuePort`` на persistqueue-адаптере
(FR-1 спеки T003; SC-1 — прогон через ``angarion.testing``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from angarion.testing import EventQueueContract

from angarion.adapters.queue.persistqueue_ import PersistQueue

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


class TestPersistQueue(EventQueueContract):
    @pytest.fixture
    def queue(self, tmp_path: Path) -> Iterator[PersistQueue]:
        queue = PersistQueue(path=tmp_path / 'queue.db')
        yield queue
        queue.close()
