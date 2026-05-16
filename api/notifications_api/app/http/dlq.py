from typing import Annotated
from uuid import UUID

from litestar import Controller, get, post
from litestar.params import Dependency, Parameter
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.repositories.delivery_attempt_repository import DeliveryAttemptRepository
from notifications_api.adapters.repositories.delivery_task_repository import DeliveryTaskRepository
from notifications_api.adapters.repositories.dlq_repository import DlqRepository
from notifications_api.adapters.repositories.outbox_repository import OutboxWriter
from notifications_api.domain.dlq import (
    DlqItemView,
    DlqStatus,
    ReplayRequest,
    ReplayResult,
)
from notifications_api.services.dlq_service import DlqService
from notifications_api.services.stats_helper import CampaignStatsHelper


def _make_service(session: AsyncSession) -> DlqService:
    return DlqService(
        session=session,
        tasks=DeliveryTaskRepository(session),
        attempts=DeliveryAttemptRepository(session),
        dlq=DlqRepository(session),
        outbox=OutboxWriter(session),
        stats=CampaignStatsHelper(session),
    )


class DlqController(Controller):
    path = "/dlq"
    tags = ["dlq"]

    @get("/")
    async def list_dlq(
        self,
        session: Annotated[AsyncSession, Dependency(skip_validation=True)],
        campaign_id: UUID | None = None,
        status: DlqStatus | None = None,
        limit: Annotated[int, Parameter(ge=1, le=500)] = 100,
        offset: Annotated[int, Parameter(ge=0)] = 0,
    ) -> dict[str, list[DlqItemView]]:
        service = _make_service(session)
        items = await service.list_items(
            campaign_id=campaign_id, status=status, limit=limit, offset=offset
        )
        return {"items": items}

    @post("/replay")
    async def replay_dlq(
        self,
        data: ReplayRequest,
        session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    ) -> ReplayResult:
        service = _make_service(session)
        return await service.replay(data.dlq_item_ids)
