"""Тесты structlog-хелперов и маскирования секретов (§17.7 ТЗ, C-6)."""

from __future__ import annotations

import structlog

from angarion.log import MASK, SECRET_KEYS, get_logger, mask_secrets


def test_masks_values_of_secret_keys_case_insensitive() -> None:
    """Значения секретных ключей заменяются на ``***`` без учёта регистра."""
    event = {
        'event': 'connect',
        'api_hash': 'abc123',
        'Token': 'tok',
        'PASSWORD': 'pwd',
        'Secret': 's3cr3t',
        'authorization': 'Bearer xyz',
    }
    masked = mask_secrets(None, 'info', event)
    assert masked['event'] == 'connect'
    for key in ('api_hash', 'Token', 'PASSWORD', 'Secret', 'authorization'):
        assert masked[key] == MASK


def test_masks_recursively_in_nested_payload() -> None:
    """Маскирование рекурсивно: вложенные dict'ы и списки (§17.7)."""
    event = {
        'event': 'config_loaded',
        'payload': {
            'accounts': {'main': {'api_hash': 'abc', 'session': 'ok.session'}},
            'headers': [{'Authorization': 'Bearer xyz'}, 'plain'],
        },
    }
    masked = mask_secrets(None, 'info', event)
    accounts = masked['payload']['accounts']
    assert accounts['main']['api_hash'] == MASK
    assert accounts['main']['session'] == 'ok.session'
    assert masked['payload']['headers'][0]['Authorization'] == MASK
    assert masked['payload']['headers'][1] == 'plain'


def test_non_secret_values_pass_through_unchanged() -> None:
    """Несекретные ключи и скалярные значения не искажаются."""
    event = {'event': 'tick', 'count': 3, 'ratio': 0.5, 'flag': True, 'none': None}
    assert mask_secrets(None, 'info', event) == event


def test_secret_keys_cover_spec_list() -> None:
    """Перечень §17.7 — публичный контракт."""
    assert SECRET_KEYS == {'api_hash', 'token', 'password', 'secret', 'authorization'}


def test_mask_secrets_works_as_structlog_processor() -> None:
    """Процессор встраивается в цепочку structlog и маскирует событие."""
    capture = structlog.testing.LogCapture()
    logger = structlog.wrap_logger(None, processors=[mask_secrets, capture])
    logger.info('login', token='tok', user='vladimir')
    (entry,) = capture.entries
    assert entry['token'] == MASK
    assert entry['user'] == 'vladimir'


def test_get_logger_returns_bindable_logger() -> None:
    """``get_logger`` отдаёт логгер structlog, поддерживающий bind."""
    log = get_logger('angarion.test')
    bound = log.bind(event_uid='abc')
    assert hasattr(bound, 'info')
