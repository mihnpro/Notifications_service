from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, final
from uuid import UUID, uuid4

from litestar.status_codes import HTTP_202_ACCEPTED

from notifications_api.app.http.idempotency import (
    complete_idempotent_request,
    fail_idempotent_request,
    payload_hash,
    start_idempotent_request,
)
from notifications_api.domain import (
    DEFAULT_REGION,
    CampaignPriority,
    Channel,
    DomainValidationError,
    RecipientSelector,
)
from notifications_api.protocol.campaign import (
    CampaignCreate,
    CampaignRepositoryProtocol,
    OutboxEvent,
    OutboxPublisherProtocol,
)
from notifications_api.usecase.campaigns.errors import CampaignUsecaseValidationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(slots=True, frozen=True)
class CreateCampaignRequest:
    manager_id: UUID
    name: str
    region_ids: list[str]
    message: dict[str, object]
    recipient_selector: dict[str, object]
    channels: list[str]
    priority: str
    idempotency_scope: str
    idempotency_key: str
    idempotency_payload: dict[str, object]
    idempotency_ttl_seconds: int


@dataclass(slots=True, frozen=True)
class CreateCampaignResponse:
    status_code: int
    payload: dict[str, object]


@final
class CreateCampaignUsecase:
    def __init__(
        self,
        *,
        session: AsyncSession,
        campaign_repository: CampaignRepositoryProtocol,
        outbox_publisher: OutboxPublisherProtocol,
    ) -> None:
        self._session = session
        self._campaign_repository = campaign_repository
        self._outbox_publisher = outbox_publisher

    async def execute(self, request: CreateCampaignRequest) -> CreateCampaignResponse:
        self._validate_request(request)
        try:
            _ = RecipientSelector.from_payload(request.recipient_selector)
        except DomainValidationError as exc:
            raise CampaignUsecaseValidationError(exc.message, exc.details) from exc

        start_result = await start_idempotent_request(
            session=self._session,
            scope=request.idempotency_scope,
            key=request.idempotency_key,
            request_hash=payload_hash(request.idempotency_payload),
            ttl_seconds=request.idempotency_ttl_seconds,
        )
        await self._session.commit()
        if start_result.is_replay and start_result.replay is not None:
            return CreateCampaignResponse(
                status_code=start_result.replay.status_code,
                payload=start_result.replay.payload,
            )

        try:
            channels = await self._campaign_repository.list_channels_by_codes(request.channels)
            self._validate_channels(channels=channels, requested_codes=request.channels)

            campaign_id = uuid4()
            region_run_id = uuid4()
            campaign = await self._campaign_repository.create_campaign(
                CampaignCreate(
                    campaign_id=campaign_id,
                    manager_id=request.manager_id,
                    name=request.name,
                    message_snapshot=request.message,
                    recipient_selector=request.recipient_selector,
                    selected_channel_codes=request.channels,
                    priority=request.priority,
                    create_idempotency_key=request.idempotency_key,
                )
            )
            region_run = await self._campaign_repository.create_campaign_region_run(
                region_run_id=region_run_id,
                campaign_id=campaign_id,
                region_id=DEFAULT_REGION,
                status="fanout_pending",
            )

            dedupe_key = f"campaign-region-run-requested:{DEFAULT_REGION}:{region_run.id}"
            event_payload: dict[str, object] = {
                "messageType": "CampaignRegionRunRequested",
                "version": 1,
                "campaignId": str(campaign.id),
                "campaignRegionRunId": str(region_run.id),
                "regionId": DEFAULT_REGION,
                "priority": request.priority,
                "dedupeKey": dedupe_key,
            }
            await self._outbox_publisher.publish(
                OutboxEvent(
                    event_id=uuid4(),
                    region_id=DEFAULT_REGION,
                    event_type="CampaignRegionRunRequested",
                    payload=event_payload,
                    routing_key=f"notification.{DEFAULT_REGION}.fanout.{request.priority}",
                    dedupe_key=dedupe_key,
                )
            )

            response_payload: dict[str, object] = {
                "campaignId": str(campaign.id),
                "status": campaign.status.value,
                "regionRuns": [
                    {
                        "id": str(region_run.id),
                        "regionId": region_run.region_id,
                        "status": region_run.status.value,
                    }
                ],
            }
            await complete_idempotent_request(
                session=self._session,
                scope=request.idempotency_scope,
                key=request.idempotency_key,
                payload=response_payload,
                status_code=HTTP_202_ACCEPTED,
            )
            await self._session.commit()
            return CreateCampaignResponse(status_code=HTTP_202_ACCEPTED, payload=response_payload)
        except Exception:
            await self._session.rollback()
            await fail_idempotent_request(
                session=self._session,
                scope=request.idempotency_scope,
                key=request.idempotency_key,
            )
            await self._session.commit()
            raise

    def _validate_request(self, request: CreateCampaignRequest) -> None:
        if sorted(set(request.region_ids)) != [DEFAULT_REGION]:
            raise CampaignUsecaseValidationError("Only regionIds=['default'] is supported in MVP", {})
        if not request.channels:
            raise CampaignUsecaseValidationError("At least one channel is required", {})
        allowed_priorities = sorted(priority.value for priority in CampaignPriority)
        if request.priority not in allowed_priorities:
            raise CampaignUsecaseValidationError("Invalid priority", {"allowed": allowed_priorities})
        if not request.message:
            raise CampaignUsecaseValidationError("message must not be empty", {})

    def _validate_channels(self, *, channels: Sequence[Channel], requested_codes: list[str]) -> None:
        channel_by_code = {channel.code: channel for channel in channels}
        missing = sorted(set(requested_codes) - set(channel_by_code))
        if missing:
            raise CampaignUsecaseValidationError("Some channels were not found", {"missingChannels": missing})

        disabled = sorted(channel.code for channel in channels if channel.state == "disabled")
        if disabled:
            raise CampaignUsecaseValidationError(
                "Selected channels are disabled in region default",
                {"disabledChannels": disabled, "regionId": DEFAULT_REGION},
            )
