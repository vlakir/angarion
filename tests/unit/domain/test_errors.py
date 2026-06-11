"""Доменные исключения (§8, FR-3): иерархия под общим корнем."""

from __future__ import annotations

import pytest

from angarion.domain.errors import (
    AngarionError,
    CatchupError,
    ConfigError,
    DeliveryError,
    NotSupportedError,
    ProcessingError,
)

CONCRETE_ERRORS = [
    ProcessingError,
    DeliveryError,
    CatchupError,
    ConfigError,
    NotSupportedError,
]


@pytest.mark.parametrize('error_cls', CONCRETE_ERRORS, ids=lambda c: c.__name__)
def test_inherits_from_angarion_error(error_cls: type[AngarionError]) -> None:
    assert issubclass(error_cls, AngarionError)
    assert issubclass(error_cls, Exception)


@pytest.mark.parametrize('error_cls', CONCRETE_ERRORS, ids=lambda c: c.__name__)
def test_raisable_with_message(error_cls: type[AngarionError]) -> None:
    with pytest.raises(AngarionError, match='boom'):
        raise error_cls('boom')


def test_catching_root_does_not_mask_builtin_errors() -> None:
    """except AngarionError не перехватывает ValueError и прочие builtin."""
    assert not issubclass(ValueError, AngarionError)
    assert not issubclass(AngarionError, ValueError)
