# pyright: reportUnusedCallResult=false
"""outbox pending partial index

Revision ID: 002
Revises: 001
Create Date: 2026-05-15 21:30:00.000000+03:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = '002'
down_revision: str | Sequence[str] | None = '001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Partial index for the publisher relay claim query:
    #   WHERE status = 'pending' ORDER BY next_attempt_at LIMIT N FOR UPDATE SKIP LOCKED
    # Keeps the index small and hot — only pending rows are tracked.
    op.create_index(
        "ix_outbox_events_pending_next_attempt",
        "outbox_events",
        ["next_attempt_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_pending_next_attempt", table_name="outbox_events")
