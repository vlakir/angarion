"""
Сессия Matrix-аккаунта и её шифрование at-rest (M7 B1, T010).

``MatrixSession`` — непрозрачная для ядра полезная нагрузка, которую
``angarion login`` кладёт в ``SessionStorePort``, а listener/sender
(B2/B3) читают: токен доступа + ``device_id`` + идентификация
homeserver. E2EE key-store (olm/megolm) — **отдельный** sqlite на ФС
(git-ignored), ведёт его ``matrix-nio`` сам; в эту строку он не входит
(§6 спеки T010).

``MatrixEncryptedSessionStore`` — Fernet-декоратор над любым
``SessionStorePort`` (зеркало telegram-варианта; крипта живёт в адаптере
платформы, ключ ``ANGARION_SESSION_KEY`` — секрет платформы, хранилище
остаётся generic). Matrix держит свой декоратор, не связываясь с
telegram-адаптером (решение Владимира 2026-06-16).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cryptography.fernet import Fernet, InvalidToken
from pydantic import BaseModel, ConfigDict

from angarion.domain.errors import ConfigError

if TYPE_CHECKING:
    from angarion.domain.ports import SessionStorePort


class MatrixSession(BaseModel):
    """
    Непрозрачная сессия Matrix-аккаунта (B1): то, что login сериализует в
    ``SessionStorePort``, а адаптер десериализует при подключении (B2/B3).
    """

    model_config = ConfigDict(frozen=True, extra='forbid')

    homeserver: str
    user_id: str
    device_id: str
    access_token: str

    def to_session_string(self) -> str:
        """Сериализовать в непрозрачную строку для ``SessionStorePort``."""
        return self.model_dump_json()

    @classmethod
    def from_session_string(cls, raw: str) -> MatrixSession:
        """Разобрать строку сессии обратно в ``MatrixSession``."""
        return cls.model_validate_json(raw)


class MatrixEncryptedSessionStore:
    """``SessionStorePort``-декоратор: Fernet-шифрование строки сессии."""

    def __init__(self, inner: SessionStorePort, key: str) -> None:
        """
        ``key`` — ``ANGARION_SESSION_KEY`` (url-safe base64, 32 байта).

        Пустой ключ допустим при конструировании (fail-fast откладывается
        до доступа к сессии — ``ensure_ready``); непустой невалидный
        ключ — ``ConfigError`` сразу.
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
        Старт-проверка: пустой ключ при наличии сессий в хранилище —
        fail-fast, чтобы не падать в рантайме.
        """
        if self._cipher is None and await self._inner.account_ids():
            msg = (
                'ANGARION_SESSION_KEY не задан, но в хранилище есть '
                'зашифрованные сессии — задай ключ или выполни re-login'
            )
            raise ConfigError(msg)

    def _require_cipher(self) -> Fernet:
        if self._cipher is None:
            msg = 'ANGARION_SESSION_KEY не задан — шифрование сессий Matrix невозможно'
            raise ConfigError(msg)
        return self._cipher
