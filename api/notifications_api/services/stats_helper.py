from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import CampaignStatsORM
from notifications_api.domain.dlq import StatsCounter


class CampaignStatsHelper:
    """Atomic INC/DEC on campaign_stats counters within an existing transaction.

    Placeholder for the B5 helper — same contract: ``transition`` shifts one
    counter down and another up in a single UPDATE.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def transition(
        self,
        campaign_id: UUID,
        *,
        from_status: StatsCounter | None,
        to_status: StatsCounter | None,
    ) -> None:
        values: dict[str, sa.ColumnElement[int]] = {}
        if from_status is not None:
            col = getattr(CampaignStatsORM, from_status.value)
            values[from_status.value] = col - 1
        if to_status is not None:
            col = getattr(CampaignStatsORM, to_status.value)
            values[to_status.value] = col + 1
        if not values:
            return
        values["updated_at"] = sa.func.now()
        stmt = sa.update(CampaignStatsORM).where(CampaignStatsORM.campaign_id == campaign_id).values(**values)
        await self._session.execute(stmt)
