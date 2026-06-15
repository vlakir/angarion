"""M5 (T023): таблица пользователей web-доступа (fastapi-users).

UUID ``id`` (``sa.Uuid`` → CHAR(32) в SQLite), уникальный ``login``,
argon2-хэш пароля, роль ``admin``/``viewer`` строкой, ``is_active``
(одобрение). Времена — ``UTCDateTime`` (TEXT ISO 8601, A-4) → ``sa.String``
в DDL: ревизия заморожена и не зависит от кода пакета.

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0003'
down_revision: str | None = '0002'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'users',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('login', sa.String(), nullable=False),
        sa.Column('hashed_password', sa.String(), nullable=False),
        sa.Column('role', sa.String(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('registered_at', sa.String(), nullable=False),
        sa.Column('approved_at', sa.String(), nullable=True),
        sa.Column('approved_by', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('login'),
    )


def downgrade() -> None:
    op.drop_table('users')
