"""M2: полная схема этапа (FR-4, FR-6 спеки T003) — и только она (C-2).

Временные колонки в ORM — ``UTCDateTime`` (TypeDecorator поверх
``String``, A-4); в DDL это TEXT, поэтому здесь — ``sa.String()``:
ревизия заморожена и не зависит от кода пакета.

Revision ID: 0001
Revises:
Create Date: 2026-06-12
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0001'
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'analytics_events',
        sa.Column('seq', sa.Integer(), nullable=False),
        sa.Column('uid', sa.String(), nullable=False),
        sa.Column('kind', sa.String(), nullable=False),
        sa.Column('event_uid', sa.String(), nullable=True),
        sa.Column('pipeline', sa.String(), nullable=True),
        sa.Column('payload', sa.String(), nullable=False),
        sa.Column('at', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('seq'),
        sa.UniqueConstraint('uid'),
        sqlite_autoincrement=True,
    )
    op.create_index(
        op.f('ix_analytics_events_at'), 'analytics_events', ['at'], unique=False
    )
    op.create_table(
        'dead_letters',
        sa.Column('seq', sa.Integer(), nullable=False),
        sa.Column('uid', sa.String(), nullable=False),
        sa.Column('pipeline', sa.String(), nullable=False),
        sa.Column('envelope', sa.String(), nullable=False),
        sa.Column('error', sa.String(), nullable=False),
        sa.Column('failed_at', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('seq'),
        sa.UniqueConstraint('uid'),
        sqlite_autoincrement=True,
    )
    op.create_table(
        'inbound_dedup',
        sa.Column('dedup_key', sa.String(), nullable=False),
        sa.Column('marked_at', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('dedup_key'),
    )
    op.create_table(
        'messages',
        sa.Column('source_key', sa.String(), nullable=False),
        sa.Column('external_id', sa.String(), nullable=False),
        sa.Column('text', sa.String(), nullable=True),
        sa.Column('content_hash', sa.String(), nullable=True),
        sa.Column('sender_id', sa.String(), nullable=True),
        sa.Column('sender_name', sa.String(), nullable=True),
        sa.Column('event_at', sa.String(), nullable=False),
        sa.Column('edit_ts', sa.String(), nullable=True),
        sa.Column('deleted_at', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('source_key', 'external_id'),
    )
    op.create_table(
        'outbound',
        sa.Column('idempotency_key', sa.String(), nullable=False),
        sa.Column('msg', sa.String(), nullable=False),
        sa.Column('status', sa.String(), nullable=False),
        sa.Column('attempts', sa.Integer(), nullable=False),
        sa.Column('next_attempt_at', sa.String(), nullable=False),
        sa.Column('created_at', sa.String(), nullable=False),
        sa.Column('finished_at', sa.String(), nullable=True),
        sa.Column('receipt', sa.String(), nullable=True),
        sa.Column('last_error', sa.String(), nullable=True),
        sa.Column('pipeline', sa.String(), nullable=True),
        sa.Column('event_uid', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('idempotency_key'),
    )
    op.create_index(
        'ix_outbound_due', 'outbound', ['status', 'next_attempt_at'], unique=False
    )
    op.create_table(
        'processor_state',
        sa.Column('ns', sa.String(), nullable=False),
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('value', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('ns', 'key'),
    )
    op.create_table(
        'source_cursors',
        sa.Column('source_key', sa.String(), nullable=False),
        sa.Column('payload', sa.String(), nullable=False),
        sa.Column('updated_at', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('source_key'),
    )
    op.create_table(
        'message_versions',
        sa.Column('seq', sa.Integer(), nullable=False),
        sa.Column('source_key', sa.String(), nullable=False),
        sa.Column('external_id', sa.String(), nullable=False),
        sa.Column('text', sa.String(), nullable=True),
        sa.Column('content_hash', sa.String(), nullable=True),
        sa.Column('recorded_at', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ['source_key', 'external_id'],
            ['messages.source_key', 'messages.external_id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('seq'),
        sqlite_autoincrement=True,
    )
    op.create_index(
        'ix_message_versions_message',
        'message_versions',
        ['source_key', 'external_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_message_versions_message', table_name='message_versions')
    op.drop_table('message_versions')
    op.drop_table('source_cursors')
    op.drop_table('processor_state')
    op.drop_index('ix_outbound_due', table_name='outbound')
    op.drop_table('outbound')
    op.drop_table('messages')
    op.drop_table('inbound_dedup')
    op.drop_table('dead_letters')
    op.drop_index(op.f('ix_analytics_events_at'), table_name='analytics_events')
    op.drop_table('analytics_events')
