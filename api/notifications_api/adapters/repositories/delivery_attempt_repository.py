from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import DeliveryAttemptORM
from notifications_api.domain.dlq import DeliveryAttemptErrorType, DeliveryAttemptStatus


class DeliveryAttemptRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_final_failure(
        self,
        *,
        task_id: UUID,
        campaign_id: UUID,
        attempt_no: int,
        channel_code: str,
        error_type: DeliveryAttemptErrorType,
        error_code: str | None,
        error_message: str | None,
    ) -> None:
        stmt = sa.insert(DeliveryAttemptORM).values(
            task_id=task_id,
            campaign_id=campaign_id,
            attempt_no=attempt_no,
            channel_code=channel_code,
            status=DeliveryAttemptStatus.FAILED.value,
            error_type=error_type.value,
            error_code=error_code,
            error_message=error_message,
            completed_at=sa.func.now(),
        )
        await self._session.execute(stmt)
