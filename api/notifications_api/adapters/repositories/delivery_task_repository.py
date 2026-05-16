from uuid import UUID

import sqlalchemy as sa

from notifications_api.adapters.postgres_models.models import DeliveryTaskORM
from notifications_api.domain.dlq import DeliveryStatus

from sqlalchemy.ext.asyncio import AsyncSession


class DeliveryTaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_for_update(self, task_id: UUID) -> DeliveryTaskORM | None:
        stmt = sa.select(DeliveryTaskORM).where(DeliveryTaskORM.id == task_id).with_for_update()
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def mark_dead_lettered(self, task_id: UUID, error_code: str | None, error_message: str | None) -> None:
        stmt = (
            sa.update(DeliveryTaskORM)
            .where(DeliveryTaskORM.id == task_id)
            .values(
                status=DeliveryStatus.DEAD_LETTERED.value,
                completed_at=sa.func.now(),
                last_error_code=error_code,
                last_error_message=error_message,
                lease_owner=None,
                lease_token=None,
                lease_until=None,
            )
        )
        await self._session.execute(stmt)

    async def reset_for_replay(self, task_id: UUID) -> None:
        stmt = (
            sa.update(DeliveryTaskORM)
            .where(DeliveryTaskORM.id == task_id)
            .values(
                status=DeliveryStatus.QUEUED.value,
                attempt_count=0,
                available_at=sa.func.now(),
                last_error_code=None,
                last_error_message=None,
                started_at=None,
                completed_at=None,
                lease_owner=None,
                lease_token=None,
                lease_until=None,
            )
        )
        await self._session.execute(stmt)
