from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import repo
from recover.jobs.lease import LeaseRecoveryJob
from recover.jobs.outbox_failed import OutboxFailedScannerJob
from recover.jobs.retry_scanner import RetryScannerJob
from recover.config import JobConfig


pytestmark = pytest.mark.asyncio

BUCKETS = [30, 60, 300, 900]
REGIONS = ["default"]


async def _insert_campaign(db: AsyncEngine, status: str = "running") -> UUID:
    cid = uuid4()
    async with db.begin() as conn:
        await conn.execute(
            text("INSERT INTO campaigns (id, status) VALUES (:id, :s)"),
            {"id": cid, "s": status},
        )
        await conn.execute(
            text(
                "INSERT INTO campaign_stats (campaign_id, total_tasks, sending) "
                "VALUES (:id, 1, 1)"
            ),
            {"id": cid},
        )
    return cid


async def _insert_task(
    db: AsyncEngine,
    campaign_id: UUID,
    *,
    status: str,
    attempt_count: int = 1,
    max_attempts: int = 3,
    lease_until: datetime | None = None,
    available_at: datetime | None = None,
    region: str = "default",
    priority: str = "high",
    queue_group: str = "marketing",
    channel: str = "push",
) -> UUID:
    tid = uuid4()
    async with db.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO delivery_tasks
                    (id, campaign_id, region_id, queue_group, priority, channel_code,
                     status, attempt_count, max_attempts, available_at,
                     lease_token, lease_owner, lease_until)
                VALUES (:id, :cid, :r, :qg, :p, :ch, :st, :ac, :mx, :av,
                        :lt, :lo, :lu)
                """
            ),
            {
                "id": tid,
                "cid": campaign_id,
                "r": region,
                "qg": queue_group,
                "p": priority,
                "ch": channel,
                "st": status,
                "ac": attempt_count,
                "mx": max_attempts,
                "av": available_at,
                "lt": "tok" if lease_until else None,
                "lo": "owner" if lease_until else None,
                "lu": lease_until,
            },
        )
    return tid


async def test_lease_expired_emits_retry_outbox_with_bucket_routing_key(db: AsyncEngine) -> None:
    cid = await _insert_campaign(db)
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    tid = await _insert_task(db, cid, status="sending", attempt_count=1, lease_until=past)

    job = LeaseRecoveryJob(db, JobConfig(batch_size=10, idle_interval_ms=1, busy_interval_ms=1, mid_interval_ms=1, concurrency=4), REGIONS, BUCKETS)
    rows = await job.tick()
    assert rows == 1

    async with db.connect() as conn:
        task = (await conn.execute(text("SELECT status, available_at FROM delivery_tasks WHERE id=:i"), {"i": tid})).mappings().first()
        assert task["status"] == "retry_scheduled"
        assert task["available_at"] is not None

        ev = (await conn.execute(text("SELECT event_type, exchange, routing_key, dedupe_key, payload FROM outbox_events"))).mappings().first()
        assert ev["event_type"] == "TaskRetryScheduled"
        assert ev["exchange"] == "notification.direct"
        assert ev["routing_key"] == "notification.default.marketing.high"
        assert ev["dedupe_key"] == f"lease_recovery_retry:{tid}:1"


async def test_lease_recovery_is_idempotent_on_repeat_ticks(db: AsyncEngine) -> None:
    cid = await _insert_campaign(db)
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    await _insert_task(db, cid, status="sending", attempt_count=1, lease_until=past)

    cfg = JobConfig(batch_size=10, idle_interval_ms=1, busy_interval_ms=1, mid_interval_ms=1, concurrency=4)
    job = LeaseRecoveryJob(db, cfg, REGIONS, BUCKETS)
    rows1 = await job.tick()
    rows2 = await job.tick()
    assert rows1 == 1
    assert rows2 == 0

    async with db.connect() as conn:
        cnt = (await conn.execute(text("SELECT COUNT(*) AS c FROM outbox_events"))).scalar()
        assert cnt == 1


async def test_lease_dead_lettered_when_attempts_exhausted(db: AsyncEngine) -> None:
    cid = await _insert_campaign(db)
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    tid = await _insert_task(db, cid, status="sending", attempt_count=3, max_attempts=3, lease_until=past)

    job = LeaseRecoveryJob(db, JobConfig(batch_size=10, idle_interval_ms=1, busy_interval_ms=1, mid_interval_ms=1, concurrency=4), REGIONS, BUCKETS)
    await job.tick()

    async with db.connect() as conn:
        st = (await conn.execute(text("SELECT status FROM delivery_tasks WHERE id=:i"), {"i": tid})).scalar()
        assert st == "dead_lettered"
        dlq = (await conn.execute(text("SELECT reason_code, status FROM dlq_items WHERE task_id=:i"), {"i": tid})).mappings().first()
        assert dlq["reason_code"] == "lease_expired_max_attempts"
        assert dlq["status"] == "open"


async def test_retry_scanner_repushes_via_main_exchange(db: AsyncEngine) -> None:
    cid = await _insert_campaign(db)
    past_avail = datetime.now(timezone.utc) - timedelta(seconds=120)
    tid = await _insert_task(
        db, cid, status="retry_scheduled", attempt_count=2, available_at=past_avail, priority="low"
    )

    cfg = JobConfig(batch_size=10, idle_interval_ms=1, busy_interval_ms=1, mid_interval_ms=1, concurrency=4)
    job = RetryScannerJob(db, cfg, REGIONS, grace_seconds=30)
    rows = await job.tick()
    assert rows == 1

    async with db.connect() as conn:
        ev = (await conn.execute(text("SELECT exchange, routing_key, dedupe_key FROM outbox_events"))).mappings().first()
        assert ev["exchange"] == "notification.direct"
        assert ev["routing_key"] == "notification.default.marketing.low"
        assert ev["dedupe_key"] == f"retry_repush:{tid}:2"


async def test_outbox_failed_scanner_drains_into_dlq(db: AsyncEngine) -> None:
    cid = await _insert_campaign(db)
    tid = await _insert_task(db, cid, status="sending", attempt_count=1)

    payload = {"task_id": str(tid)}
    async with db.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO outbox_events
                    (id, region_id, status, dedupe_key, event_type, exchange,
                     routing_key, payload, attempt_count, last_error, created_at)
                VALUES (gen_random_uuid(), 'default', 'failed', 'k1', 'TaskDispatch',
                        'notification.direct', 'notification.default.marketing.high',
                        CAST(:p AS JSONB), 5, 'unroutable: no binding', NOW())
                """
            ),
            {"p": json.dumps(payload)},
        )

    cfg = JobConfig(batch_size=10, idle_interval_ms=1, busy_interval_ms=1, mid_interval_ms=1, concurrency=4)
    job = OutboxFailedScannerJob(db, cfg, REGIONS)
    rows = await job.tick()
    assert rows == 1

    async with db.connect() as conn:
        st = (await conn.execute(text("SELECT status FROM delivery_tasks WHERE id=:i"), {"i": tid})).scalar()
        assert st == "dead_lettered"
        dlq = (await conn.execute(text("SELECT reason_code FROM dlq_items WHERE task_id=:i"), {"i": tid})).mappings().first()
        assert dlq["reason_code"] == "publish_unroutable"
        cnt = (await conn.execute(text("SELECT COUNT(*) FROM outbox_events"))).scalar()
        assert cnt == 0
