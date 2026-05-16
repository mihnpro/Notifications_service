from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import text

from publisher.db import outbox_events
from publisher.domain.outbox import OutboxRow

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


_CLAIM_SQL = text("""
    WITH cte AS (
        SELECT id
        FROM outbox_events
        WHERE status = 'pending'
          AND next_attempt_at <= now()
        ORDER BY next_attempt_at
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_events o
    SET status = 'publishing',
        locked_by = :worker_id,
        locked_until = now() + make_interval(secs => :lock_ttl),
        attempt_count = o.attempt_count + 1
    FROM cte
    WHERE o.id = cte.id
    RETURNING o.id, o.exchange, o.routing_key, o.payload::text AS payload,
              o.event_type, o.dedupe_key, o.attempt_count
""")


class OutboxRepository:
    def __init__(self, engine: AsyncEngine, worker_id: str, lock_ttl_sec: int) -> None:
        self._engine = engine
        self._worker_id = worker_id
        self._lock_ttl_sec = lock_ttl_sec

    async def claim_batch(self, batch_size: int) -> list[OutboxRow]:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                _CLAIM_SQL,
                {"batch_size": batch_size, "worker_id": self._worker_id, "lock_ttl": self._lock_ttl_sec},
            )
            rows = result.mappings().all()
        return [OutboxRow.model_validate(dict(r)) for r in rows]

    async def delete_by_ids(self, ids: list[UUID]) -> None:
        if not ids:
            return
        stmt = sa.delete(outbox_events).where(outbox_events.c.id.in_(ids))
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def mark_retry(self, event_id: UUID, error: str, backoff_sec: float) -> None:
        stmt = (
            sa.update(outbox_events)
            .where(outbox_events.c.id == event_id)
            .values(
                status="pending",
                locked_by=None,
                locked_until=None,
                last_error=error[:2000],
                next_attempt_at=sa.func.now() + sa.func.make_interval(0, 0, 0, 0, 0, 0, backoff_sec),
            )
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def mark_failed(self, event_id: UUID, error: str) -> None:
        stmt = (
            sa.update(outbox_events)
            .where(outbox_events.c.id == event_id)
            .values(status="failed", locked_by=None, locked_until=None, last_error=error[:2000])
        )
        async with self._engine.begin() as conn:
            await conn.execute(stmt)

    async def recover_stale(self) -> int:
        stmt = (
            sa.update(outbox_events)
            .where(outbox_events.c.status == "publishing")
            .where(outbox_events.c.locked_until < sa.func.now())
            .values(status="pending", locked_by=None, locked_until=None)
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
        return result.rowcount or 0
