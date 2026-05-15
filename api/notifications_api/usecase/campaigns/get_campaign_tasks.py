from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from notifications_api.domain import DeliveryRecord
from notifications_api.protocol.campaign import CampaignRepositoryProtocol, CampaignTailQuery, CursorPoint, Page
from notifications_api.usecase.campaigns.get_campaign import GetCampaignRequest, GetCampaignUsecase

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(slots=True, frozen=True)
class GetCampaignTasksRequest:
    campaign_id: UUID
    manager_id: UUID
    limit: int
    cursor: CursorPoint | None
    status: str | None
    channel: str | None


@final
class GetCampaignTasksUsecase:
    def __init__(
        self,
        *,
        campaign_repository: CampaignRepositoryProtocol,
        get_campaign_usecase: GetCampaignUsecase,
    ) -> None:
        self._campaign_repository = campaign_repository
        self._get_campaign_usecase = get_campaign_usecase

    async def execute(self, request: GetCampaignTasksRequest) -> Page[DeliveryRecord]:
        _ = await self._get_campaign_usecase.execute(
            GetCampaignRequest(campaign_id=request.campaign_id, manager_id=request.manager_id)
        )
        return await self._campaign_repository.list_campaign_tasks(
            CampaignTailQuery(
                campaign_id=request.campaign_id,
                limit=request.limit,
                cursor=request.cursor,
                status=request.status,
                channel=request.channel,
            )
        )
