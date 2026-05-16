from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

metadata = sa.MetaData()

# Schema owned by api/alembic. This Table lists only the columns the relay touches.
outbox_events = sa.Table(
    "outbox_events",
    metadata,
    sa.Column("id", UUID(as_uuid=True), primary_key=True),
    sa.Column("exchange", sa.Text, nullable=False),
    sa.Column("routing_key", sa.Text, nullable=False),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("event_type", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("dedupe_key", sa.Text, nullable=False),
    sa.Column("locked_by", sa.Text),
    sa.Column("locked_until", sa.DateTime(timezone=True)),
    sa.Column("attempt_count", sa.Integer, nullable=False),
    sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("last_error", sa.Text),
)


def create_engine(dsn: str, pool_min: int, pool_max: int) -> AsyncEngine:
    return create_async_engine(
        dsn,
        pool_size=pool_max,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )
