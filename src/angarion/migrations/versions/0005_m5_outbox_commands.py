"""M5 (T024): таблица командного outbox api→pipeline (§12.9).

Команды-мост: ``uid`` (PK) — handle для захвата/пометки; ``payload`` /
``result`` — JSON-текст; ``status`` — pending/taken/done/failed строкой.
Время — ``UTCDateTime`` (TEXT ISO 8601, A-4) → ``sa.String`` в DDL:
ревизия заморожена. Индекс по ``status`` ускоряет поллинг pending'ов.

Revision ID: 0005
Revises: 0004
Create Date: 2026-06-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0005'
down_revision: str | None = '0004'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'outbox_commands',
        sa.Column('uid', sa.String(), nullable=False),
        sa.Column('kind', sa.String(), nullable=False),
        sa.Column('payload', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('created_at', sa.String(), nullable=False),
        sa.Column('executed_at', sa.String(), nullable=True),
        sa.Column('result', sa.String(), nullable=True),
        sa.Column('error', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('uid'),
    )
    op.create_index(
        'ix_outbox_commands_status', 'outbox_commands', ['status'], unique=False
    )


def downgrade() -> None:
    op.drop_index('ix_outbox_commands_status', table_name='outbox_commands')
    op.drop_table('outbox_commands')
