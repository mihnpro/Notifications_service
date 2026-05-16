from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from notifications_api.domain import Campaign, CampaignRegionRun, CampaignStats, Channel, DeliveryError, DeliveryRecord

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID


@dataclass(slots=True, frozen=True)
class CursorPoint:
    timestamp: datetime
    row_id: UUID


@dataclass(slots=True, frozen=True)
class Page[T]:
    items: list[T]
    next_cursor: CursorPoint | None


@dataclass(slots=True, frozen=True)
class CampaignListQuery:
    manager_id: UUID
    limit: int
    cursor: CursorPoint | None
    status: str | None


@dataclass(slots=True, frozen=True)
class CampaignTailQuery:
    campaign_id: UUID
    limit: int
    cursor: CursorPoint | None
    status: str | None = None
    channel: str | None = None


@dataclass(slots=True, frozen=True)
class CampaignErrorQuery:
    campaign_id: UUID
    limit: int
    cursor: CursorPoint | None
    channel: str | None = None
    error_code: str | None = None


@dataclass(slots=True, frozen=True)
class CampaignCreate:
    campaign_id: UUID
    manager_id: UUID
    name: str
    message_snapshot: dict[str, object]
    recipient_selector: dict[str, object]
    selected_channel_codes: list[str]
    priority: str
    create_idempotency_key: str


@dataclass(slots=True, frozen=True)
class OutboxEvent:
    event_id: UUID
    region_id: str
    event_type: str
    payload: dict[str, object]
    routing_key: str
    dedupe_key: str
    status: str = "pending"
    transport_mode: str = "rabbitmq_direct"


class CampaignRepositoryProtocol(Protocol):
    async def get_campaign_for_manager(
        self,
        campaign_id: UUID,
        manager_id: UUID,
        *,
        for_update: bool = False,
    ) -> Campaign | None: ...

    async def list_campaigns(self, query: CampaignListQuery) -> Page[Campaign]: ...

    async def list_channels_by_codes(self, codes: list[str]) -> list[Channel]: ...

    async def create_campaign(self, data: CampaignCreate) -> Campaign: ...

    async def create_campaign_region_run(
        self,
        *,
        region_run_id: UUID,
        campaign_id: UUID,
        region_id: str,
        status: str,
    ) -> CampaignRegionRun: ...

    async def update_campaign_status(self, campaign_id: UUID, status: str) -> None: ...

    async def get_campaign_stats(self, campaign_id: UUID, region_id: str) -> CampaignStats | None: ...

    async def list_campaign_tasks(self, query: CampaignTailQuery) -> Page[DeliveryRecord]: ...

    async def list_campaign_results(self, query: CampaignTailQuery) -> Page[DeliveryRecord]: ...

    async def list_campaign_errors(self, query: CampaignErrorQuery) -> Page[DeliveryError]: ...


class OutboxPublisherProtocol(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...
