from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from notifications_api.domain import Campaign
from notifications_api.protocol.campaign import CampaignRepositoryProtocol
from notifications_api.usecase.campaigns.errors import CampaignUsecaseNotFoundError

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(slots=True, frozen=True)
class GetCampaignRequest:
    campaign_id: UUID
    manager_id: UUID


@final
class GetCampaignUsecase:
    def __init__(self, *, campaign_repository: CampaignRepositoryProtocol) -> None:
        self._campaign_repository = campaign_repository

    async def execute(self, request: GetCampaignRequest) -> Campaign:
        campaign = await self._campaign_repository.get_campaign_for_manager(
            request.campaign_id,
            request.manager_id,
        )
        if campaign is None:
            raise CampaignUsecaseNotFoundError("Campaign not found")
        return campaign
