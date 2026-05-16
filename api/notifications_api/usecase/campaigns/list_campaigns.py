from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from notifications_api.domain import Campaign
from notifications_api.protocol.campaign import CampaignListQuery, CampaignRepositoryProtocol, CursorPoint, Page

if TYPE_CHECKING:
    from uuid import UUID


@dataclass(slots=True, frozen=True)
class ListCampaignsRequest:
    manager_id: UUID
    limit: int
    cursor: CursorPoint | None
    status: str | None


@final
class ListCampaignsUsecase:
    def __init__(self, *, campaign_repository: CampaignRepositoryProtocol) -> None:
        self._campaign_repository = campaign_repository

    async def execute(self, request: ListCampaignsRequest) -> Page[Campaign]:
        return await self._campaign_repository.list_campaigns(
            CampaignListQuery(
                manager_id=request.manager_id,
                limit=request.limit,
                cursor=request.cursor,
                status=request.status,
            )
        )
