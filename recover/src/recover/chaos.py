"""Chaos injectors for local testing. SQL copied verbatim from the original Rust chaos.rs."""
from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


_STUCK_LEASES_SQL = text(
    """
    WITH picked AS (
        SELECT id FROM delivery_tasks
        WHERE status IN ('queued','retry_scheduled')
        ORDER BY random()
        LIMIT :count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE delivery_tasks t
    SET status = 'sending',
        lease_token = gen_random_uuid()::text,
        lease_owner = 'chaos',
        lease_until = NOW() - INTERVAL '10 seconds',
        attempt_count = COALESCE(attempt_count, 0) + 1
    FROM picked
    WHERE t.id = picked.id
    """
)

_STUCK_OUTBOX_SQL = text(
    """
    WITH picked AS (
        SELECT id FROM outbox_events
        WHERE status = 'pending'
        ORDER BY random()
        LIMIT :count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_events o
    SET status = 'publishing',
        locked_by = 'chaos',
        locked_until = NOW() - INTERVAL '30 seconds'
    FROM picked
    WHERE o.id = picked.id
    """
)

_ORPHAN_RETRY_SQL = text(
    """
    WITH picked AS (
        SELECT id FROM delivery_tasks
        WHERE status = 'retry_scheduled'
        ORDER BY random()
        LIMIT :count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE delivery_tasks t
    SET available_at = NOW() - INTERVAL '5 minutes'
    FROM picked
    WHERE t.id = picked.id
    """
)


async def inject_stuck_leases(engine: AsyncEngine, count: int) -> int:
    async with engine.begin() as conn:
        res = await conn.execute(_STUCK_LEASES_SQL, {"count": count})
        return int(res.rowcount or 0)


async def inject_stuck_outbox(engine: AsyncEngine, count: int) -> int:
    async with engine.begin() as conn:
        res = await conn.execute(_STUCK_OUTBOX_SQL, {"count": count})
        return int(res.rowcount or 0)


async def inject_orphan_retry(engine: AsyncEngine, count: int) -> int:
    async with engine.begin() as conn:
        res = await conn.execute(_ORPHAN_RETRY_SQL, {"count": count})
        return int(res.rowcount or 0)
