"""All DB access. SQL is copied verbatim from the original Rust `repo.rs` —
DBA/dashboard contracts depend on exact queries and dedupe-key formats.
"""
from __future__ import annotations

import enum
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.types import String

from recover.retry_bucket import EXCHANGE_DIRECT, Bucket, for_attempt, main_routing_key


# ───────────────────────────── data shapes ─────────────────────────────


@dataclass
class ExpiredLease:
    id: UUID
    campaign_id: UUID
    region_id: str
    queue_group: str
    priority: str
    channel_code: str
    attempt_count: int
    max_attempts: int


@dataclass
class RetryReady:
    id: UUID
    campaign_id: UUID
    region_id: str
    queue_group: str
    priority: str
    channel_code: str
    attempt_count: int


@dataclass
class CompletableCampaign:
    campaign_id: UUID
    total_tasks: int
    succeeded: int
    failed: int
    dead_lettered: int
    cancelled: int
    current_status: str


@dataclass
class TaskRow:
    id: UUID
    campaign_id: UUID
    region_id: str
    queue_group: str
    priority: str
    channel_code: str
    status: str
    attempt_count: int
    max_attempts: int
    available_at: datetime | None


@dataclass
class FailedOutbox:
    id: UUID
    region_id: str
    dedupe_key: str
    last_error: str | None
    payload: Any  # already-parsed JSON


@dataclass
class DlqItemRow:
    id: UUID
    task_id: UUID
    campaign_id: UUID
    reason_code: str
    status: str
    last_replayed_at: datetime | None
    created_at: datetime | None

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "task_id": str(self.task_id),
            "campaign_id": str(self.campaign_id),
            "reason_code": self.reason_code,
            "status": self.status,
            "last_replayed_at": self.last_replayed_at.isoformat() if self.last_replayed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


@dataclass
class BacklogSnapshot:
    stuck_outbox: int
    expired_leases: int
    retry_ready: int
    completable_campaigns: int


# ─────────────────────────────── outcomes ──────────────────────────────


class LeaseRecoveryOutcome(enum.Enum):
    RETRIED = "retried"
    DEAD_LETTERED = "dead_lettered"
    SKIPPED = "skipped"


@dataclass
class LeaseRecoveryResult:
    outcome: LeaseRecoveryOutcome
    bucket: Bucket | None = None


class ForceRetryOutcome(enum.Enum):
    SCHEDULED = "scheduled"
    NOT_RETRYABLE = "not_retryable"
    NOT_FOUND = "not_found"


@dataclass
class ForceRetryResult:
    outcome: ForceRetryOutcome
    current_status: str | None = None


class DlqReplayOutcome(enum.Enum):
    REPLAYED = "replayed"
    NOT_FOUND = "not_found"
    NOT_OPEN = "not_open"
    TASK_MISSING = "task_missing"


@dataclass
class DlqReplayResult:
    outcome: DlqReplayOutcome
    status: str | None = None


class RunRecoveryOutcome(enum.Enum):
    RESET = "reset"
    NOT_FOUND = "not_found"
    NOT_STUCK = "not_stuck"


@dataclass
class RunRecoveryResult:
    outcome: RunRecoveryOutcome
    status: str | None = None


class FailedOutboxOutcome(enum.Enum):
    DEAD_LETTERED = "dead_lettered"
    TASK_MISSING = "task_missing"
    SKIPPED = "skipped"


_TERMINAL_STATUSES = {"succeeded", "failed", "dead_lettered", "cancelled"}
_STATUS_COLS = {"queued", "sending", "succeeded", "failed", "retry_scheduled", "dead_lettered", "cancelled"}


def _region_filter(regions: list[str]) -> list[str] | None:
    return None if any(r == "*" for r in regions) else list(regions)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _json_default(obj: Any) -> Any:
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"unsupported json type: {type(obj)!r}")


def _dumps(payload: dict) -> str:
    return json.dumps(payload, default=_json_default)


def _regions_bindparam(name: str = "regions"):
    return bindparam(name, type_=ARRAY(String()))


# ─────────────────────────────── queries ───────────────────────────────


_BACKLOG_SQL = text(
    """
    SELECT
        (SELECT COUNT(*) FROM outbox_events
          WHERE status = 'publishing'
            AND locked_until IS NOT NULL
            AND locked_until < NOW()
            AND (:regions IS NULL OR region_id = ANY(:regions))) AS stuck_outbox,
        (SELECT COUNT(*) FROM delivery_tasks
          WHERE status = 'sending'
            AND lease_until IS NOT NULL
            AND lease_until < NOW()
            AND (:regions IS NULL OR region_id = ANY(:regions))) AS expired_leases,
        (SELECT COUNT(*) FROM delivery_tasks
          WHERE status = 'retry_scheduled'
            AND available_at IS NOT NULL
            AND available_at < NOW() - INTERVAL '30 seconds'
            AND (:regions IS NULL OR region_id = ANY(:regions))) AS retry_ready,
        (SELECT COUNT(*) FROM campaign_stats s
          JOIN campaigns c ON c.id = s.campaign_id
          WHERE s.total_tasks > 0
            AND (s.queued + s.sending + s.retry_scheduled) = 0
            AND c.status IN ('running','cancelling')) AS completable_campaigns
    """
).bindparams(_regions_bindparam())


async def backlog_snapshot(engine: AsyncEngine, regions: list[str]) -> BacklogSnapshot:
    async with engine.connect() as conn:
        result = await conn.execute(_BACKLOG_SQL, {"regions": _region_filter(regions)})
        row = result.one()
    return BacklogSnapshot(
        stuck_outbox=int(row.stuck_outbox or 0),
        expired_leases=int(row.expired_leases or 0),
        retry_ready=int(row.retry_ready or 0),
        completable_campaigns=int(row.completable_campaigns or 0),
    )


_RESET_STUCK_OUTBOX_SQL = text(
    """
    WITH stuck AS (
        SELECT id FROM outbox_events
        WHERE status = 'publishing'
          AND locked_until IS NOT NULL
          AND locked_until < NOW()
          AND (:regions IS NULL OR region_id = ANY(:regions))
        ORDER BY locked_until ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_events o
    SET status = 'pending',
        locked_by = NULL,
        locked_until = NULL,
        attempt_count = attempt_count + 1,
        next_attempt_at = NOW(),
        last_error = COALESCE(last_error, 'recovered_from_stuck_publishing')
    FROM stuck
    WHERE o.id = stuck.id
    """
).bindparams(_regions_bindparam())


async def reset_stuck_outbox(engine: AsyncEngine, batch_size: int, regions: list[str]) -> int:
    async with engine.begin() as conn:
        result = await conn.execute(
            _RESET_STUCK_OUTBOX_SQL,
            {"batch_size": batch_size, "regions": _region_filter(regions)},
        )
        return int(result.rowcount or 0)


_FETCH_EXPIRED_LEASES_SQL = text(
    """
    SELECT id, campaign_id, region_id, queue_group, priority, channel_code,
           attempt_count, max_attempts
    FROM delivery_tasks
    WHERE status = 'sending'
      AND lease_until IS NOT NULL
      AND lease_until < NOW()
      AND (:regions IS NULL OR region_id = ANY(:regions))
    ORDER BY lease_until ASC
    LIMIT :batch_size
    """
).bindparams(_regions_bindparam())


async def fetch_expired_leases(
    engine: AsyncEngine, batch_size: int, regions: list[str]
) -> list[ExpiredLease]:
    async with engine.connect() as conn:
        result = await conn.execute(
            _FETCH_EXPIRED_LEASES_SQL,
            {"batch_size": batch_size, "regions": _region_filter(regions)},
        )
        rows = result.mappings().all()
    return [
        ExpiredLease(
            id=r["id"],
            campaign_id=r["campaign_id"],
            region_id=r["region_id"],
            queue_group=r["queue_group"],
            priority=r["priority"],
            channel_code=r["channel_code"],
            attempt_count=int(r["attempt_count"]),
            max_attempts=int(r["max_attempts"]),
        )
        for r in rows
    ]


_DEAD_LETTER_LEASE_SQL = text(
    """
    UPDATE delivery_tasks
    SET status = 'dead_lettered',
        lease_token = NULL,
        lease_until = NULL,
        lease_owner = NULL,
        completed_at = NOW()
    WHERE id = :id
      AND status = 'sending'
      AND lease_until < NOW()
    """
)

_RETRY_SCHEDULE_LEASE_SQL = text(
    """
    UPDATE delivery_tasks
    SET status = 'retry_scheduled',
        lease_token = NULL,
        lease_until = NULL,
        lease_owner = NULL,
        available_at = :available_at
    WHERE id = :id
      AND status = 'sending'
      AND lease_until < NOW()
    """
)

_MARK_ATTEMPT_STALE_SQL = text(
    """
    UPDATE delivery_attempts
    SET status = 'stale',
        completed_at = NOW()
    WHERE task_id = :task_id
      AND status = 'started'
    """
)

_INSERT_OUTBOX_SQL = text(
    """
    INSERT INTO outbox_events
        (id, region_id, status, dedupe_key, event_type, exchange,
         routing_key, payload, attempt_count, next_attempt_at, created_at)
    VALUES (gen_random_uuid(), :region_id, 'pending', :dedupe_key, :event_type,
            :exchange, :routing_key, CAST(:payload AS JSONB), 0, NOW(), NOW())
    ON CONFLICT (region_id, dedupe_key) DO NOTHING
    """
)

_INSERT_DLQ_LEASE_SQL = text(
    """
    INSERT INTO dlq_items (id, task_id, campaign_id, region_id, channel_code,
                           reason_code, status, created_at)
    VALUES (gen_random_uuid(), :task_id, :campaign_id, :region_id, :channel_code,
            :reason_code, 'open', NOW())
    ON CONFLICT (task_id) WHERE status = 'open' DO NOTHING
    """
)


async def _bump_stats(conn: AsyncConnection, campaign_id: UUID, new_status: str) -> None:
    if new_status not in _STATUS_COLS:
        raise ValueError(f"unknown status column: {new_status}")
    sql = text(
        f"""
        UPDATE campaign_stats
        SET sending = GREATEST(sending - 1, 0),
            {new_status} = {new_status} + 1,
            updated_at = NOW()
        WHERE campaign_id = :campaign_id
        """
    )
    await conn.execute(sql, {"campaign_id": campaign_id})


async def _rebalance_stats(conn: AsyncConnection, campaign_id: UUID, prev_status: str, new_col: str) -> None:
    if prev_status not in _STATUS_COLS or new_col not in _STATUS_COLS:
        return
    sql = text(
        f"""
        UPDATE campaign_stats
        SET {prev_status} = GREATEST({prev_status} - 1, 0),
            {new_col} = {new_col} + 1,
            updated_at = NOW()
        WHERE campaign_id = :campaign_id
        """
    )
    await conn.execute(sql, {"campaign_id": campaign_id})


async def _insert_outbox(
    conn: AsyncConnection,
    *,
    region_id: str,
    event_type: str,
    exchange: str,
    routing_key: str,
    dedupe_key: str,
    payload: dict,
) -> bool:
    result = await conn.execute(
        _INSERT_OUTBOX_SQL,
        {
            "region_id": region_id,
            "event_type": event_type,
            "exchange": exchange,
            "routing_key": routing_key,
            "dedupe_key": dedupe_key,
            "payload": _dumps(payload),
        },
    )
    return (result.rowcount or 0) > 0


async def recover_expired_lease(
    engine: AsyncEngine, lease: ExpiredLease, buckets_seconds: list[int]
) -> LeaseRecoveryResult:
    dead_letter = lease.attempt_count >= lease.max_attempts

    async with engine.begin() as conn:
        if dead_letter:
            res = await conn.execute(_DEAD_LETTER_LEASE_SQL, {"id": lease.id})
            if (res.rowcount or 0) == 0:
                return LeaseRecoveryResult(LeaseRecoveryOutcome.SKIPPED)
            await conn.execute(_MARK_ATTEMPT_STALE_SQL, {"task_id": lease.id})
            await _bump_stats(conn, lease.campaign_id, "dead_lettered")
            await conn.execute(
                _INSERT_DLQ_LEASE_SQL,
                {
                    "task_id": lease.id,
                    "campaign_id": lease.campaign_id,
                    "region_id": lease.region_id,
                    "channel_code": lease.channel_code,
                    "reason_code": "lease_expired_max_attempts",
                },
            )
            return LeaseRecoveryResult(LeaseRecoveryOutcome.DEAD_LETTERED)

        bucket = for_attempt(lease.attempt_count, buckets_seconds)
        now = _now_utc()
        available_at = now + timedelta(seconds=bucket.delay_seconds)

        res = await conn.execute(
            _RETRY_SCHEDULE_LEASE_SQL,
            {"id": lease.id, "available_at": available_at},
        )
        if (res.rowcount or 0) == 0:
            return LeaseRecoveryResult(LeaseRecoveryOutcome.SKIPPED)

        await conn.execute(_MARK_ATTEMPT_STALE_SQL, {"task_id": lease.id})
        await _bump_stats(conn, lease.campaign_id, "retry_scheduled")

        dedupe_key = f"lease_recovery_retry:{lease.id}:{lease.attempt_count}"
        routing_key = main_routing_key(lease.region_id, lease.queue_group, lease.priority)
        payload = {
            "task_id": str(lease.id),
            "campaign_id": str(lease.campaign_id),
            "region_id": lease.region_id,
            "queue_group": lease.queue_group,
            "priority": lease.priority,
            "channel_code": lease.channel_code,
            "attempt_count": lease.attempt_count,
            "reason": "lease_expired",
            "scheduled_at": now.isoformat(),
            "available_at": available_at.isoformat(),
            "retry_bucket": bucket.label,
        }
        await _insert_outbox(
            conn,
            region_id=lease.region_id,
            event_type="TaskRetryScheduled",
            exchange=EXCHANGE_DIRECT,
            routing_key=routing_key,
            dedupe_key=dedupe_key,
            payload=payload,
        )

    return LeaseRecoveryResult(LeaseRecoveryOutcome.RETRIED, bucket=bucket)


_FETCH_RETRY_READY_SQL = text(
    """
    SELECT t.id, t.campaign_id, t.region_id, t.queue_group, t.priority,
           t.channel_code, t.attempt_count
    FROM delivery_tasks t
    WHERE t.status = 'retry_scheduled'
      AND t.available_at IS NOT NULL
      AND t.available_at < NOW() - make_interval(secs => :grace_seconds)
      AND (:regions IS NULL OR t.region_id = ANY(:regions))
      AND NOT EXISTS (
          SELECT 1 FROM outbox_events o
          WHERE (
                o.dedupe_key = 'retry_repush:' || t.id::text || ':' || t.attempt_count::text
             OR o.dedupe_key = 'lease_recovery_retry:' || t.id::text || ':' || t.attempt_count::text
          )
          AND o.status IN ('pending','publishing')
      )
    ORDER BY t.available_at ASC
    LIMIT :batch_size
    """
).bindparams(_regions_bindparam())


async def fetch_retry_ready(
    engine: AsyncEngine, batch_size: int, regions: list[str], grace_seconds: int
) -> list[RetryReady]:
    async with engine.connect() as conn:
        result = await conn.execute(
            _FETCH_RETRY_READY_SQL,
            {
                "batch_size": batch_size,
                "regions": _region_filter(regions),
                "grace_seconds": float(grace_seconds),
            },
        )
        rows = result.mappings().all()
    return [
        RetryReady(
            id=r["id"],
            campaign_id=r["campaign_id"],
            region_id=r["region_id"],
            queue_group=r["queue_group"],
            priority=r["priority"],
            channel_code=r["channel_code"],
            attempt_count=int(r["attempt_count"]),
        )
        for r in rows
    ]


async def enqueue_retry_repush(engine: AsyncEngine, t: RetryReady) -> bool:
    routing_key = main_routing_key(t.region_id, t.queue_group, t.priority)
    dedupe_key = f"retry_repush:{t.id}:{t.attempt_count}"
    payload = {
        "task_id": str(t.id),
        "campaign_id": str(t.campaign_id),
        "region_id": t.region_id,
        "queue_group": t.queue_group,
        "priority": t.priority,
        "channel_code": t.channel_code,
        "attempt_count": t.attempt_count,
        "reason": "retry_repush",
        "scheduled_at": _now_utc().isoformat(),
    }
    async with engine.begin() as conn:
        return await _insert_outbox(
            conn,
            region_id=t.region_id,
            event_type="TaskRetryRepush",
            exchange=EXCHANGE_DIRECT,
            routing_key=routing_key,
            dedupe_key=dedupe_key,
            payload=payload,
        )


_FETCH_COMPLETABLE_SQL = text(
    """
    SELECT s.campaign_id,
           s.total_tasks,
           s.succeeded,
           s.failed,
           s.dead_lettered,
           s.cancelled,
           c.status AS current_status
    FROM campaign_stats s
    JOIN campaigns c ON c.id = s.campaign_id
    WHERE s.total_tasks > 0
      AND (s.queued + s.sending + s.retry_scheduled) = 0
      AND c.status IN ('running','cancelling')
    ORDER BY c.created_at ASC NULLS LAST
    LIMIT :batch_size
    """
)


async def fetch_completable_campaigns(engine: AsyncEngine, batch_size: int) -> list[CompletableCampaign]:
    async with engine.connect() as conn:
        result = await conn.execute(_FETCH_COMPLETABLE_SQL, {"batch_size": batch_size})
        rows = result.mappings().all()
    return [
        CompletableCampaign(
            campaign_id=r["campaign_id"],
            total_tasks=int(r["total_tasks"]),
            succeeded=int(r["succeeded"]),
            failed=int(r["failed"]),
            dead_lettered=int(r["dead_lettered"]),
            cancelled=int(r["cancelled"]),
            current_status=r["current_status"],
        )
        for r in rows
    ]


_FINALIZE_CAMPAIGN_SQL = text(
    """
    UPDATE campaigns
    SET status = :new_status,
        completed_at = NOW()
    WHERE id = :id
      AND status = :prev_status
    """
)


async def finalize_campaign(engine: AsyncEngine, c: CompletableCampaign) -> bool:
    if c.current_status == "cancelling" or (c.cancelled == c.total_tasks and c.total_tasks > 0):
        new_status = "cancelled"
    elif c.failed == 0 and c.dead_lettered == 0 and c.cancelled == 0:
        new_status = "completed"
    elif c.succeeded == 0:
        new_status = "failed"
    else:
        new_status = "partially_failed"

    async with engine.begin() as conn:
        result = await conn.execute(
            _FINALIZE_CAMPAIGN_SQL,
            {"id": c.campaign_id, "new_status": new_status, "prev_status": c.current_status},
        )
        return (result.rowcount or 0) > 0


_FETCH_TASK_SQL = text(
    """
    SELECT id, campaign_id, region_id, queue_group, priority, channel_code,
           status, attempt_count, max_attempts, available_at
    FROM delivery_tasks
    WHERE id = :id
    """
)

_FETCH_TASK_FOR_UPDATE_SQL = text(
    """
    SELECT id, campaign_id, region_id, queue_group, priority, channel_code,
           status, attempt_count, max_attempts, available_at
    FROM delivery_tasks
    WHERE id = :id
    FOR UPDATE
    """
)


def _row_to_task(r: dict) -> TaskRow:
    return TaskRow(
        id=r["id"],
        campaign_id=r["campaign_id"],
        region_id=r["region_id"],
        queue_group=r["queue_group"],
        priority=r["priority"],
        channel_code=r["channel_code"],
        status=r["status"],
        attempt_count=int(r["attempt_count"]),
        max_attempts=int(r["max_attempts"]),
        available_at=r["available_at"],
    )


async def fetch_task(engine: AsyncEngine, task_id: UUID) -> TaskRow | None:
    async with engine.connect() as conn:
        result = await conn.execute(_FETCH_TASK_SQL, {"id": task_id})
        row = result.mappings().first()
    return _row_to_task(dict(row)) if row else None


_FORCE_RETRY_UPDATE_SQL = text(
    """
    UPDATE delivery_tasks
    SET status = 'retry_scheduled',
        available_at = NOW(),
        lease_token = NULL,
        lease_until = NULL,
        lease_owner = NULL,
        completed_at = NULL,
        max_attempts = :new_max
    WHERE id = :id
      AND status IN ('failed','dead_lettered','cancelled','retry_scheduled')
    """
)

_DLQ_REPLAY_CLOSE_OPEN_SQL = text(
    """
    UPDATE dlq_items
    SET status = 'replayed',
        last_replayed_at = NOW()
    WHERE task_id = :task_id
      AND status = 'open'
    """
)


async def force_retry_task(engine: AsyncEngine, task_id: UUID, ceiling: int) -> ForceRetryResult:
    task = await fetch_task(engine, task_id)
    if task is None:
        return ForceRetryResult(ForceRetryOutcome.NOT_FOUND)
    if task.status not in {"failed", "dead_lettered", "cancelled", "retry_scheduled"}:
        return ForceRetryResult(ForceRetryOutcome.NOT_RETRYABLE, current_status=task.status)

    new_max = min(max(task.attempt_count + 1, task.max_attempts), ceiling)

    async with engine.begin() as conn:
        res = await conn.execute(_FORCE_RETRY_UPDATE_SQL, {"id": task.id, "new_max": new_max})
        if (res.rowcount or 0) == 0:
            return ForceRetryResult(ForceRetryOutcome.NOT_RETRYABLE, current_status=task.status)

        if task.status in {"failed", "dead_lettered", "cancelled"}:
            await _rebalance_stats(conn, task.campaign_id, task.status, "retry_scheduled")

        await conn.execute(_DLQ_REPLAY_CLOSE_OPEN_SQL, {"task_id": task.id})

        dedupe_key = f"manual_retry:{task.id}:{int(time.time() * 1000)}"
        payload = {
            "task_id": str(task.id),
            "campaign_id": str(task.campaign_id),
            "region_id": task.region_id,
            "queue_group": task.queue_group,
            "priority": task.priority,
            "channel_code": task.channel_code,
            "attempt_count": task.attempt_count,
            "reason": "manual_retry",
            "scheduled_at": _now_utc().isoformat(),
        }
        await _insert_outbox(
            conn,
            region_id=task.region_id,
            event_type="TaskRetryScheduled",
            exchange=EXCHANGE_DIRECT,
            routing_key=main_routing_key(task.region_id, task.queue_group, task.priority),
            dedupe_key=dedupe_key,
            payload=payload,
        )
    return ForceRetryResult(ForceRetryOutcome.SCHEDULED)


_FETCH_DLQ_SQL = text("SELECT id, task_id, status FROM dlq_items WHERE id = :id")

_DLQ_REPLAY_UPDATE_TASK_SQL = text(
    """
    UPDATE delivery_tasks
    SET status = 'retry_scheduled',
        available_at = NOW(),
        lease_token = NULL,
        lease_until = NULL,
        lease_owner = NULL,
        completed_at = NULL,
        max_attempts = :new_max
    WHERE id = :id
    """
)

_DLQ_REPLAY_SET_REPLAYED_SQL = text(
    """
    UPDATE dlq_items
    SET status = 'replayed',
        last_replayed_at = NOW()
    WHERE id = :id
    """
)


async def replay_dlq(engine: AsyncEngine, dlq_id: UUID, ceiling: int) -> DlqReplayResult:
    async with engine.connect() as conn:
        row = (await conn.execute(_FETCH_DLQ_SQL, {"id": dlq_id})).mappings().first()
    if row is None:
        return DlqReplayResult(DlqReplayOutcome.NOT_FOUND)
    if row["status"] != "open":
        return DlqReplayResult(DlqReplayOutcome.NOT_OPEN, status=row["status"])
    task = await fetch_task(engine, row["task_id"])
    if task is None:
        return DlqReplayResult(DlqReplayOutcome.TASK_MISSING)

    new_max = min(max(task.attempt_count + 1, task.max_attempts), ceiling)

    async with engine.begin() as conn:
        res = await conn.execute(_DLQ_REPLAY_UPDATE_TASK_SQL, {"id": task.id, "new_max": new_max})
        if (res.rowcount or 0) == 0:
            return DlqReplayResult(DlqReplayOutcome.TASK_MISSING)

        if task.status in {"failed", "dead_lettered", "cancelled"}:
            await _rebalance_stats(conn, task.campaign_id, task.status, "retry_scheduled")

        await conn.execute(_DLQ_REPLAY_SET_REPLAYED_SQL, {"id": dlq_id})

        dedupe_key = f"dlq_replay:{dlq_id}:{int(time.time() * 1000)}"
        payload = {
            "task_id": str(task.id),
            "campaign_id": str(task.campaign_id),
            "region_id": task.region_id,
            "queue_group": task.queue_group,
            "priority": task.priority,
            "channel_code": task.channel_code,
            "attempt_count": task.attempt_count,
            "reason": "dlq_replay",
            "dlq_id": str(dlq_id),
            "scheduled_at": _now_utc().isoformat(),
        }
        await _insert_outbox(
            conn,
            region_id=task.region_id,
            event_type="TaskRetryScheduled",
            exchange=EXCHANGE_DIRECT,
            routing_key=main_routing_key(task.region_id, task.queue_group, task.priority),
            dedupe_key=dedupe_key,
            payload=payload,
        )
    return DlqReplayResult(DlqReplayOutcome.REPLAYED)


_LIST_DLQ_SQL = text(
    """
    SELECT id, task_id, campaign_id, reason_code, status, last_replayed_at, created_at
    FROM dlq_items
    WHERE (:status::text IS NULL OR status = :status)
    ORDER BY created_at DESC NULLS LAST
    LIMIT :limit
    """
)


async def list_dlq(engine: AsyncEngine, status: str | None, limit: int) -> list[DlqItemRow]:
    async with engine.connect() as conn:
        result = await conn.execute(_LIST_DLQ_SQL, {"status": status, "limit": limit})
        rows = result.mappings().all()
    return [
        DlqItemRow(
            id=r["id"],
            task_id=r["task_id"],
            campaign_id=r["campaign_id"],
            reason_code=r["reason_code"],
            status=r["status"],
            last_replayed_at=r["last_replayed_at"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


_FETCH_RUN_SQL = text("SELECT id, status FROM campaign_region_runs WHERE id = :id")
_RECOVER_RUN_SQL = text(
    """
    UPDATE campaign_region_runs
    SET status = 'fanout_pending',
        fanout_lock_until = NULL
    WHERE id = :id
      AND status IN ('fanout_running','fanout_failed')
    """
)


async def recover_run(engine: AsyncEngine, run_id: UUID) -> RunRecoveryResult:
    async with engine.connect() as conn:
        row = (await conn.execute(_FETCH_RUN_SQL, {"id": run_id})).mappings().first()
    if row is None:
        return RunRecoveryResult(RunRecoveryOutcome.NOT_FOUND)
    if row["status"] not in {"fanout_running", "fanout_failed"}:
        return RunRecoveryResult(RunRecoveryOutcome.NOT_STUCK, status=row["status"])

    async with engine.begin() as conn:
        res = await conn.execute(_RECOVER_RUN_SQL, {"id": run_id})
        if (res.rowcount or 0) == 0:
            return RunRecoveryResult(RunRecoveryOutcome.NOT_STUCK, status=row["status"])
    return RunRecoveryResult(RunRecoveryOutcome.RESET)


_FETCH_FAILED_OUTBOX_SQL = text(
    """
    SELECT id, region_id, dedupe_key, last_error, payload
    FROM outbox_events
    WHERE status = 'failed'
      AND (:regions IS NULL OR region_id = ANY(:regions))
    ORDER BY created_at ASC
    LIMIT :batch_size
    """
).bindparams(_regions_bindparam())


async def fetch_failed_outbox(
    engine: AsyncEngine, batch_size: int, regions: list[str]
) -> list[FailedOutbox]:
    async with engine.connect() as conn:
        result = await conn.execute(
            _FETCH_FAILED_OUTBOX_SQL,
            {"batch_size": batch_size, "regions": _region_filter(regions)},
        )
        rows = result.mappings().all()
    return [
        FailedOutbox(
            id=r["id"],
            region_id=r["region_id"],
            dedupe_key=r["dedupe_key"],
            last_error=r["last_error"],
            payload=r["payload"],
        )
        for r in rows
    ]


_DELETE_OUTBOX_BY_ID_SQL = text("DELETE FROM outbox_events WHERE id = :id")
_DELETE_OUTBOX_FAILED_SQL = text("DELETE FROM outbox_events WHERE id = :id AND status = 'failed'")
_DEAD_LETTER_TASK_FROM_PUBLISH_SQL = text(
    """
    UPDATE delivery_tasks
    SET status = 'dead_lettered',
        lease_token = NULL,
        lease_until = NULL,
        lease_owner = NULL,
        completed_at = NOW()
    WHERE id = :id
    """
)

_INSERT_DLQ_PUBLISH_SQL = text(
    """
    INSERT INTO dlq_items (id, task_id, campaign_id, channel_code,
                           reason_code, error_message, status, created_at)
    VALUES (gen_random_uuid(), :task_id, :campaign_id, :channel_code,
            :reason_code, :error_message, 'open', NOW())
    ON CONFLICT (task_id) WHERE status = 'open' DO UPDATE
        SET reason_code = EXCLUDED.reason_code,
            error_message = EXCLUDED.error_message
    """
)


def _extract_task_id(payload: Any) -> UUID | None:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    raw = payload.get("task_id")
    if not isinstance(raw, str):
        return None
    try:
        return UUID(raw)
    except ValueError:
        return None


async def dead_letter_failed_outbox(engine: AsyncEngine, row: FailedOutbox) -> FailedOutboxOutcome:
    task_id = _extract_task_id(row.payload)
    async with engine.begin() as conn:
        if task_id is None:
            await conn.execute(_DELETE_OUTBOX_BY_ID_SQL, {"id": row.id})
            return FailedOutboxOutcome.TASK_MISSING

        result = await conn.execute(_FETCH_TASK_FOR_UPDATE_SQL, {"id": task_id})
        task_row = result.mappings().first()
        if task_row is None:
            await conn.execute(_DELETE_OUTBOX_BY_ID_SQL, {"id": row.id})
            return FailedOutboxOutcome.TASK_MISSING

        task = _row_to_task(dict(task_row))
        already_terminal = task.status in _TERMINAL_STATUSES

        if not already_terminal:
            await conn.execute(_DEAD_LETTER_TASK_FROM_PUBLISH_SQL, {"id": task.id})
            await _rebalance_stats(conn, task.campaign_id, task.status, "dead_lettered")

        last_err = row.last_error or ""
        reason_code = "publish_unroutable" if last_err.startswith("unroutable") else "publish_exhausted"
        truncated = row.last_error[:2000] if row.last_error is not None else None

        await conn.execute(
            _INSERT_DLQ_PUBLISH_SQL,
            {
                "task_id": task.id,
                "campaign_id": task.campaign_id,
                "channel_code": task.channel_code,
                "reason_code": reason_code,
                "error_message": truncated,
            },
        )

        purge_res = await conn.execute(_DELETE_OUTBOX_FAILED_SQL, {"id": row.id})
        if (purge_res.rowcount or 0) == 0:
            # The row was claimed by someone else mid-transaction. Roll back implicitly by raising.
            # Using a tag to signal Skipped — but engine.begin() commits on exit. Force rollback:
            await conn.rollback()
            return FailedOutboxOutcome.SKIPPED
    return FailedOutboxOutcome.DEAD_LETTERED
