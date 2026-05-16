from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import sqlalchemy as sa

from notifications_api.adapters.postgres import PostgresCampaignRepository
from notifications_api.adapters.postgres_models.models import OutboxEventORM
from notifications_api.domain import CampaignStatus
from notifications_api.infra.postgres import AsyncSessionFactory


@dataclass(slots=True, frozen=True)
class ClaimedCancelEvent:
    event_id: UUID
    payload: dict[str, Any]
    attempt_count: int


class CampaignCancelProcessor:
    def __init__(
        self,
        *,
        session_factory: AsyncSessionFactory,
        worker_id: str | None = None,
        poll_interval_seconds: float = 0.5,
        lock_ttl_seconds: int = 30,
        batch_size: int = 20,
    ) -> None:
        self._session_factory = session_factory
        self._worker_id = worker_id or f"api-cancel-processor-{uuid4()}"
        self._poll_interval_seconds = max(poll_interval_seconds, 0.1)
        self._lock_ttl_seconds = max(lock_ttl_seconds, 1)
        self._batch_size = max(batch_size, 1)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                processed = await self._process_once()
            except Exception:
                processed = 0
            if processed == 0:
                await self._sleep_until_stop(stop_event, self._poll_interval_seconds)

    async def _process_once(self) -> int:
        events = await self._claim_batch()
        if not events:
            return 0
        processed = 0
        for event in events:
            try:
                await self._process_event(event)
            except Exception as exc:
                await self._mark_retry(event.event_id, event.attempt_count, str(exc))
            else:
                processed += 1
        return processed

    async def _claim_batch(self) -> list[ClaimedCancelEvent]:
        stmt = sa.text(
            """
            WITH claim AS (
                SELECT id
                FROM outbox_events
                WHERE status = 'pending'
                  AND transport_mode = 'cdc'
                  AND event_type = 'CampaignCancelRequested'
                  AND next_attempt_at <= now()
                ORDER BY next_attempt_at, created_at
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox_events e
            SET status = 'publishing',
                locked_by = :worker_id,
                locked_until = now() + make_interval(secs => :lock_ttl),
                attempt_count = e.attempt_count + 1
            FROM claim
            WHERE e.id = claim.id
            RETURNING e.id, e.payload, e.attempt_count
            """
        )
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    stmt,
                    {
                        "batch_size": self._batch_size,
                        "worker_id": self._worker_id,
                        "lock_ttl": self._lock_ttl_seconds,
                    },
                )
            ).mappings()
            claimed = [
                ClaimedCancelEvent(
                    event_id=row["id"],
                    payload=dict(row["payload"]),
                    attempt_count=int(row["attempt_count"] or 0),
                )
                for row in rows
            ]
            await session.commit()
        return claimed

    async def _process_event(self, event: ClaimedCancelEvent) -> None:
        campaign_id_raw = event.payload.get("campaignId")
        campaign_id = UUID(str(campaign_id_raw))
        async with self._session_factory() as session:
            repo = PostgresCampaignRepository(session)
            campaign = await repo.get_campaign_for_update(campaign_id)
            if campaign is None:
                error_message = f"Campaign not found for cancellation: {campaign_id}"
                raise ValueError(error_message)

            if campaign.status in {CampaignStatus.RUNNING, CampaignStatus.CANCELLING}:
                counts = await repo.cancel_pending_tasks_for_campaign(campaign.id)
                await repo.cancel_active_region_runs_for_campaign(campaign.id)
                await repo.apply_cancel_stats_delta(campaign.id, counts)
                await repo.mark_campaign_cancelled(campaign.id)

            await self._mark_archived(session, event.event_id)
            await session.commit()

    async def _mark_archived(self, session: Any, event_id: UUID) -> None:
        stmt = (
            sa.update(OutboxEventORM)
            .where(
                OutboxEventORM.id == event_id,
                OutboxEventORM.status == "publishing",
                OutboxEventORM.locked_by == self._worker_id,
            )
            .values(
                status="archived",
                locked_by=None,
                locked_until=None,
                published_at=sa.func.now(),
                last_error=None,
            )
        )
        await session.execute(stmt)

    async def _mark_retry(self, event_id: UUID, attempt_count: int, error_text: str) -> None:
        backoff = _backoff_seconds(attempt_count)
        async with self._session_factory() as session:
            stmt = (
                sa.update(OutboxEventORM)
                .where(
                    OutboxEventORM.id == event_id,
                    OutboxEventORM.status == "publishing",
                    OutboxEventORM.locked_by == self._worker_id,
                )
                .values(
                    status="pending",
                    locked_by=None,
                    locked_until=None,
                    last_error=error_text[:2000],
                    next_attempt_at=datetime.now(tz=UTC) + timedelta(seconds=backoff),
                )
            )
            await session.execute(stmt)
            await session.commit()

    async def _sleep_until_stop(self, stop_event: asyncio.Event, duration_seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)


def _backoff_seconds(attempt: int) -> float:
    base = min(2 ** max(attempt, 1), 300)
    return base * 1.1
