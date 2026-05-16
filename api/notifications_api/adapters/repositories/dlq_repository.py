from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import DlqItemORM
from notifications_api.domain.dlq import DlqStatus


class DlqRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_open(
        self,
        *,
        task_id: UUID,
        campaign_id: UUID,
        channel_code: str,
        reason_code: str,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        """Returns True if inserted, False if a row for task_id already existed."""
        stmt = (
            pg_insert(DlqItemORM)
            .values(
                task_id=task_id,
                campaign_id=campaign_id,
                channel_code=channel_code,
                reason_code=reason_code,
                error_code=error_code,
                error_message=error_message,
                status=DlqStatus.OPEN.value,
            )
            .on_conflict_do_nothing(index_elements=["task_id"])
            .returning(DlqItemORM.id)
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def get_open_for_update(self, dlq_item_id: UUID) -> DlqItemORM | None:
        stmt = (
            sa.select(DlqItemORM)
            .where(DlqItemORM.id == dlq_item_id, DlqItemORM.status == DlqStatus.OPEN.value)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def exists(self, dlq_item_id: UUID) -> bool:
        stmt = sa.select(sa.literal(1)).where(DlqItemORM.id == dlq_item_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def mark_replayed(self, dlq_item_id: UUID) -> None:
        stmt = (
            sa.update(DlqItemORM)
            .where(DlqItemORM.id == dlq_item_id)
            .values(status=DlqStatus.REPLAYED.value, last_replayed_at=sa.func.now())
        )
        await self._session.execute(stmt)

    async def list_filtered(
        self,
        *,
        campaign_id: UUID | None,
        status: DlqStatus | None,
        limit: int,
        offset: int,
    ) -> list[DlqItemORM]:
        stmt = sa.select(DlqItemORM).order_by(DlqItemORM.created_at.desc()).limit(limit).offset(offset)
        if campaign_id is not None:
            stmt = stmt.where(DlqItemORM.campaign_id == campaign_id)
        if status is not None:
            stmt = stmt.where(DlqItemORM.status == status.value)
        result = await self._session.execute(stmt)
        return list(result.scalars().all())
