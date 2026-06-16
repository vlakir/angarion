"""M7 (T010, A3): media_hash в реестре и архиве версий (§9.2).

Отпечаток вложений для catch-up-детекции медиа-правок: подмена файла при
том же тексте = новая версия. Колонка nullable (старые строки — NULL,
трактуется как «медиа неизвестно/отсутствует»); бэкафилла нет.

Revision ID: 0006
Revises: 0005
Create Date: 2026-06-16
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0006'
down_revision: str | None = '0005'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column('messages', sa.Column('media_hash', sa.String(), nullable=True))
    op.add_column(
        'message_versions', sa.Column('media_hash', sa.String(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('message_versions', 'media_hash')
    op.drop_column('messages', 'media_hash')
