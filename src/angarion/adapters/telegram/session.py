"""
Шифрование сессий Telegram at-rest (Q2 спеки T005, ADR 2026-06-13).

``EncryptedSessionStore`` — декоратор над любым ``SessionStorePort``:
шифрует строку сессии (Fernet) перед записью и расшифровывает при
чтении, поэтому в ``app.db`` ложится только ciphertext. Криптография
живёт здесь, в telegram-адаптере (extra ``angarion[telegram]``), а не в
storage-адаптере: ключ ``ANGARION_SESSION_KEY`` — секрет платформы, а
само хранилище остаётся generic (порт оперирует непрозрачной строкой).

Потеря/ротация ключа = re-login (`angarion login`): расшифровать старый
ciphertext новым ключом нельзя — это ``ConfigError`` с внятным текстом.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cryptography.fernet import Fernet, InvalidToken

from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from angarion.domain.ports import SessionStorePort


class EncryptedSessionStore:
    """``SessionStorePort``-декоратор: Fernet-шифрование строки сессии."""

    def __init__(self, inner: SessionStorePort, key: str) -> None:
        """
        ``key`` — ``ANGARION_SESSION_KEY`` (url-safe base64, 32 байта).

        Пустой ключ допустим при конструировании (fail-fast откладывается
        до фактического доступа к сессии — см. ``ensure_ready``); непустой
        невалидный ключ — ``ConfigError`` сразу.
        """
        self._inner = inner
        if not key:
            self._cipher: Fernet | None = None
            return
        try:
            self._cipher = Fernet(key.encode('utf-8'))
        except ValueError as exc:
            msg = (
                'ANGARION_SESSION_KEY невалиден: ожидается url-safe base64 '
                'из 32 байт (сгенерировать: '
                'python -c "from cryptography.fernet import Fernet; '
                'print(Fernet.generate_key().decode())")'
            )
            raise ConfigError(msg) from exc

    async def load(self, account_id: str) -> str | None:
        """Расшифровать строку сессии аккаунта или None."""
        ciphertext = await self._inner.load(account_id)
        if ciphertext is None:
            return None
        cipher = self._require_cipher()
        try:
            return cipher.decrypt(ciphertext.encode('utf-8')).decode('utf-8')
        except InvalidToken as exc:
            msg = (
                f'не удалось расшифровать сессию аккаунта {account_id!r}: '
                'ANGARION_SESSION_KEY не совпадает с ключом шифрования '
                '(ротация ключа = re-login: `angarion login`)'
            )
            raise ConfigError(msg) from exc

    async def save(self, account_id: str, session_string: str) -> None:
        """Зашифровать и сохранить строку сессии аккаунта."""
        cipher = self._require_cipher()
        token = cipher.encrypt(session_string.encode('utf-8')).decode('utf-8')
        await self._inner.save(account_id, token)

    async def account_ids(self) -> list[str]:
        """Аккаунты с сохранённой сессией (делегирует, без расшифровки)."""
        return await self._inner.account_ids()

    async def ensure_ready(self) -> None:
        """
        Старт-проверка (§FR «Сессия аккаунта»): пустой ключ при наличии
        сессий в хранилище — fail-fast, чтобы не падать в рантайме.
        """
        if self._cipher is None and await self._inner.account_ids():
            msg = (
                'ANGARION_SESSION_KEY не задан, но в хранилище есть '
                'зашифрованные сессии — задай ключ или выполни re-login'
            )
            raise ConfigError(msg)

    def _require_cipher(self) -> Fernet:
        if self._cipher is None:
            msg = (
                'ANGARION_SESSION_KEY не задан — шифрование сессий Telegram '
                'невозможно (Q2 спеки T005)'
            )
            raise ConfigError(msg)
        return self._cipher
