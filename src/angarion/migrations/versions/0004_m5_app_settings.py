"""M5 (T024): таблица override'ов динамических настроек (§12.8).

key-value JSON поверх файла: ``key`` — имя поля ``DynamicSettings``,
``value`` — JSON-сериализация. Время — ``UTCDateTime`` (TEXT ISO 8601,
A-4) → ``sa.String`` в DDL: ревизия заморожена.

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0004'
down_revision: str | None = '0003'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'app_settings',
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('value', sa.String(), nullable=False),
        sa.Column('updated_at', sa.String(), nullable=False),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('key'),
    )


def downgrade() -> None:
    op.drop_table('app_settings')
