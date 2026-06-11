"""Smoke-тест каркаса пакета."""

from __future__ import annotations

import angarion


def test_version_is_exposed() -> None:
    """Пакет импортируется и публикует строковую версию."""
    assert isinstance(angarion.__version__, str)
    assert angarion.__version__
