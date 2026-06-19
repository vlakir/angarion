"""T027: колонка ``taken_at`` командного outbox — lease-маркер reaper'а.

Reaper зависших ``taken`` (краш consumer'а между ``take`` и пометкой,
§12.9) возвращает команды старше lease обратно в ``pending``. Маркер —
момент захвата ``taken_at`` (``UTCDateTime`` → ``sa.String`` в DDL,
ревизия заморожена; A-4). Nullable: у ``pending``/терминальных — NULL.

Revision ID: 0007
Revises: 0006
Create Date: 2026-06-19
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0007'
down_revision: str | None = '0006'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        'outbox_commands',
        sa.Column('taken_at', sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('outbox_commands', 'taken_at')
