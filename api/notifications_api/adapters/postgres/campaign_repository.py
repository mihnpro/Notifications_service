from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import (
    CampaignORM,
    CampaignRegionRunORM,
    CampaignStatsORM,
    ChannelORM,
    DeliveryAttemptORM,
    DeliveryResultORM,
    DeliveryTaskORM,
    OutboxEventORM,
)
from notifications_api.protocol.campaign import (
    CampaignCreate,
    CampaignErrorQuery,
    CampaignListQuery,
    CampaignRepositoryProtocol,
    CampaignTailQuery,
    CursorPoint,
    OutboxEvent,
    OutboxPublisherProtocol,
    Page,
)


class PostgresCampaignRepository(CampaignRepositoryProtocol):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_campaign_for_manager(
        self,
        campaign_id: UUID,
        manager_id: UUID,
        *,
        for_update: bool = False,
    ):
        stmt = sa.select(CampaignORM).where(
            CampaignORM.id == campaign_id,
            CampaignORM.manager_id == manager_id,
        )
        if for_update:
            stmt = stmt.with_for_update()
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        return row.to_domain() if row is not None else None

    async def list_campaigns(self, query: CampaignListQuery):
        stmt = sa.select(CampaignORM).where(CampaignORM.manager_id == query.manager_id)
        if query.status is not None:
            stmt = stmt.where(CampaignORM.status == query.status)
        if query.cursor is not None:
            stmt = stmt.where(
                _build_cursor_filter(
                    created_at_column=CampaignORM.created_at,
                    id_column=CampaignORM.id,
                    cursor=query.cursor,
                )
            )

        stmt = stmt.order_by(CampaignORM.created_at.desc(), CampaignORM.id.desc()).limit(query.limit + 1)
        rows = (await self._session.execute(stmt)).scalars().all()
        has_next = len(rows) > query.limit
        rows = rows[: query.limit]
        next_cursor = None
        if has_next and rows:
            last = rows[-1]
            next_cursor = CursorPoint(timestamp=last.created_at, row_id=last.id)

        return Page(items=[row.to_domain() for row in rows], next_cursor=next_cursor)

    async def list_channels_by_codes(self, codes: list[str]):
        if not codes:
            return []
        rows = (await self._session.execute(sa.select(ChannelORM).where(ChannelORM.code.in_(codes)))).scalars().all()
        return [row.to_domain() for row in rows]

    async def create_campaign(self, data: CampaignCreate):
        campaign = CampaignORM(
            id=data.campaign_id,
            manager_id=data.manager_id,
            name=data.name,
            status="running",
            message_snapshot=data.message_snapshot,
            recipient_selector=data.recipient_selector,
            selected_channel_codes=data.selected_channel_codes,
            priority=data.priority,
            create_idempotency_key=data.create_idempotency_key,
        )
        self._session.add(campaign)
        await self._session.flush()
        return campaign.to_domain()

    async def create_campaign_region_run(
        self,
        *,
        region_run_id: UUID,
        campaign_id: UUID,
        region_id: str,
        status: str,
    ):
        region_run = CampaignRegionRunORM(
            id=region_run_id,
            campaign_id=campaign_id,
            region_id=region_id,
            status=status,
        )
        self._session.add(region_run)
        await self._session.flush()
        return region_run.to_domain()

    async def update_campaign_status(self, campaign_id: UUID, status: str) -> None:
        row = (await self._session.execute(sa.select(CampaignORM).where(CampaignORM.id == campaign_id))).scalar_one()
        row.status = status
        await self._session.flush()

    async def get_campaign_stats(self, campaign_id: UUID, region_id: str):
        row = (
            await self._session.execute(
                sa.select(CampaignStatsORM).where(
                    CampaignStatsORM.campaign_id == campaign_id,
                    CampaignStatsORM.region_id == region_id,
                )
            )
        ).scalar_one_or_none()
        return row.to_domain() if row is not None else None

    async def list_campaign_tasks(self, query: CampaignTailQuery):
        stmt = sa.select(DeliveryTaskORM).where(DeliveryTaskORM.campaign_id == query.campaign_id)
        if query.status is not None:
            stmt = stmt.where(DeliveryTaskORM.status == query.status)
        if query.channel is not None:
            stmt = stmt.where(DeliveryTaskORM.channel_code == query.channel)
        if query.cursor is not None:
            stmt = stmt.where(
                _build_cursor_filter(
                    created_at_column=DeliveryTaskORM.created_at,
                    id_column=DeliveryTaskORM.id,
                    cursor=query.cursor,
                )
            )

        stmt = stmt.order_by(DeliveryTaskORM.created_at.desc(), DeliveryTaskORM.id.desc()).limit(query.limit + 1)
        rows = (await self._session.execute(stmt)).scalars().all()
        has_next = len(rows) > query.limit
        rows = rows[: query.limit]
        next_cursor = None
        if has_next and rows:
            last = rows[-1]
            next_cursor = CursorPoint(timestamp=last.created_at, row_id=last.id)
        return Page(items=[row.to_domain() for row in rows], next_cursor=next_cursor)

    async def list_campaign_results(self, query: CampaignTailQuery):
        stmt = sa.select(DeliveryResultORM).where(DeliveryResultORM.campaign_id == query.campaign_id)
        if query.status is not None:
            stmt = stmt.where(DeliveryResultORM.status == query.status)
        if query.channel is not None:
            stmt = stmt.where(DeliveryResultORM.channel_code == query.channel)
        if query.cursor is not None:
            stmt = stmt.where(
                _build_cursor_filter(
                    created_at_column=DeliveryResultORM.completed_at,
                    id_column=DeliveryResultORM.id,
                    cursor=query.cursor,
                )
            )

        stmt = stmt.order_by(DeliveryResultORM.completed_at.desc(), DeliveryResultORM.id.desc()).limit(query.limit + 1)
        rows = (await self._session.execute(stmt)).scalars().all()
        has_next = len(rows) > query.limit
        rows = rows[: query.limit]
        next_cursor = None
        if has_next and rows:
            last = rows[-1]
            next_cursor = CursorPoint(timestamp=last.completed_at, row_id=last.id)
        return Page(items=[row.to_domain() for row in rows], next_cursor=next_cursor)

    async def list_campaign_errors(self, query: CampaignErrorQuery):
        stmt = sa.select(DeliveryAttemptORM).where(DeliveryAttemptORM.campaign_id == query.campaign_id)
        stmt = stmt.where(DeliveryAttemptORM.status.in_(["failed", "timed_out", "stale"]))
        if query.channel is not None:
            stmt = stmt.where(DeliveryAttemptORM.channel_code == query.channel)
        if query.error_code is not None:
            stmt = stmt.where(DeliveryAttemptORM.error_code == query.error_code)
        if query.cursor is not None:
            stmt = stmt.where(
                _build_cursor_filter(
                    created_at_column=DeliveryAttemptORM.started_at,
                    id_column=DeliveryAttemptORM.id,
                    cursor=query.cursor,
                )
            )

        stmt = stmt.order_by(DeliveryAttemptORM.started_at.desc(), DeliveryAttemptORM.id.desc()).limit(query.limit + 1)
        rows = (await self._session.execute(stmt)).scalars().all()
        has_next = len(rows) > query.limit
        rows = rows[: query.limit]
        next_cursor = None
        if has_next and rows:
            last = rows[-1]
            next_cursor = CursorPoint(timestamp=last.started_at, row_id=last.id)
        return Page(items=[row.to_domain() for row in rows], next_cursor=next_cursor)


class PostgresOutboxPublisher(OutboxPublisherProtocol):
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def publish(self, event: OutboxEvent) -> None:
        self._session.add(
            OutboxEventORM(
                id=event.event_id,
                region_id=event.region_id,
                event_type=event.event_type,
                payload=event.payload,
                routing_key=event.routing_key,
                dedupe_key=event.dedupe_key,
                status=event.status,
                transport_mode=event.transport_mode,
            )
        )
        await self._session.flush()


def _build_cursor_filter(created_at_column: Any, id_column: Any, cursor: CursorPoint) -> Any:
    return sa.or_(
        created_at_column < cursor.timestamp,
        sa.and_(created_at_column == cursor.timestamp, id_column < cursor.row_id),
    )
