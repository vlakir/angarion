"""T041: транспорт-агностичные имена столбцов outbox/аналитики.

Ядровое обобщение модели (``Record`` + ``transport``): доменные поля
``OutboxRecord.msg``→``.record`` и ``*.event_uid``→``.record_uid``.
Приводим приватные столбцы хранилища в соответствие:

- ``outbound.msg``        → ``outbound.record``       (JSON ``OutboundRecord``)
- ``outbound.event_uid``  → ``outbound.record_uid``
- ``analytics_events.event_uid`` → ``analytics_events.record_uid``

Только переименование столбцов (RENAME COLUMN, SQLite ≥ 3.25 через
``batch_alter_table``); типы и индексы не меняются. JSON-блобы внутри
``record`` на pre-alpha не мигрируются — ключи внутри дампа уже пишутся
новыми именами (big-bang, разрыв со старыми блобами принят).

Revision ID: 0008
Revises: 0007
Create Date: 2026-06-20
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0008'
down_revision: str | None = '0007'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('outbound') as batch:
        batch.alter_column('msg', new_column_name='record')
        batch.alter_column('event_uid', new_column_name='record_uid')
    with op.batch_alter_table('analytics_events') as batch:
        batch.alter_column('event_uid', new_column_name='record_uid')


def downgrade() -> None:
    with op.batch_alter_table('analytics_events') as batch:
        batch.alter_column('record_uid', new_column_name='event_uid')
    with op.batch_alter_table('outbound') as batch:
        batch.alter_column('record_uid', new_column_name='event_uid')
        batch.alter_column('record', new_column_name='msg')
