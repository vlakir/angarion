"""M3 (T005): таблица сессий аккаунтов Telegram (StringSession at-rest).

``session_string`` хранится зашифрованным (Fernet, Q2) — шифрует
декоратор ``EncryptedSessionStore`` telegram-адаптера; на уровне БД это
просто TEXT. ``updated_at`` — ``UTCDateTime`` (TEXT ISO 8601, A-4), в
DDL — ``sa.String()``: ревизия заморожена и не зависит от кода пакета.

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0002'
down_revision: str | None = '0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'telegram_sessions',
        sa.Column('account_id', sa.String(), nullable=False),
        sa.Column('session_string', sa.String(), nullable=False),
        sa.Column('updated_at', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('account_id'),
    )


def downgrade() -> None:
    op.drop_table('telegram_sessions')
