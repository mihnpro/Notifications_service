from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from notifications_api.domain import DEFAULT_REGION, Campaign, CampaignStats
from notifications_api.protocol.campaign import CampaignRepositoryProtocol
from notifications_api.usecase.campaigns.get_campaign import GetCampaignRequest, GetCampaignUsecase

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


@dataclass(slots=True, frozen=True)
class CampaignStatsView:
    campaign: Campaign
    stats: CampaignStats | None
    updated_at: datetime


@dataclass(slots=True, frozen=True)
class GetCampaignStatsViewRequest:
    campaign_id: UUID
    manager_id: UUID


@final
class GetCampaignStatsUsecase:
    def __init__(
        self,
        *,
        campaign_repository: CampaignRepositoryProtocol,
        get_campaign_usecase: GetCampaignUsecase,
    ) -> None:
        self._campaign_repository = campaign_repository
        self._get_campaign_usecase = get_campaign_usecase

    async def execute(self, request: GetCampaignStatsViewRequest) -> CampaignStatsView:
        campaign = await self._get_campaign_usecase.execute(
            GetCampaignRequest(campaign_id=request.campaign_id, manager_id=request.manager_id)
        )
        stats = await self._campaign_repository.get_campaign_stats(request.campaign_id, DEFAULT_REGION)
        updated_at = stats.updated_at if stats is not None else campaign.created_at
        return CampaignStatsView(campaign=campaign, stats=stats, updated_at=updated_at)
