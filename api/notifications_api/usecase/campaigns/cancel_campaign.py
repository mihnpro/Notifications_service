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
from notifications_api.domain import DEFAULT_REGION, CampaignStatus
from notifications_api.protocol.campaign import CampaignRepositoryProtocol, OutboxEvent, OutboxPublisherProtocol
from notifications_api.usecase.campaigns.errors import CampaignUsecaseNotFoundError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(slots=True, frozen=True)
class CancelCampaignRequest:
    campaign_id: UUID
    manager_id: UUID
    reason: str | None
    idempotency_scope: str
    idempotency_key: str
    idempotency_payload: dict[str, object]
    idempotency_ttl_seconds: int


@dataclass(slots=True, frozen=True)
class CancelCampaignResponse:
    status_code: int
    payload: dict[str, object]


@final
class CancelCampaignUsecase:
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

    async def execute(self, request: CancelCampaignRequest) -> CancelCampaignResponse:
        start_result = await start_idempotent_request(
            session=self._session,
            scope=request.idempotency_scope,
            key=request.idempotency_key,
            request_hash=payload_hash(request.idempotency_payload),
            ttl_seconds=request.idempotency_ttl_seconds,
        )
        await self._session.commit()
        if start_result.is_replay and start_result.replay is not None:
            return CancelCampaignResponse(
                status_code=start_result.replay.status_code,
                payload=start_result.replay.payload,
            )

        try:
            campaign = await self._campaign_repository.get_campaign_for_manager(
                request.campaign_id,
                request.manager_id,
                for_update=True,
            )
            if campaign is None:
                raise CampaignUsecaseNotFoundError("Campaign not found")

            next_status = campaign.status
            if campaign.can_request_cancel():
                next_status = CampaignStatus.CANCELLING
                await self._campaign_repository.update_campaign_status(campaign.id, CampaignStatus.CANCELLING.value)

                dedupe_key = f"campaign-cancel-requested:{DEFAULT_REGION}:{campaign.id}"
                payload: dict[str, object] = {
                    "messageType": "CampaignCancelRequested",
                    "version": 1,
                    "campaignId": str(campaign.id),
                    "regionId": DEFAULT_REGION,
                    "reason": request.reason or "manual_cancel",
                    "dedupeKey": dedupe_key,
                }
                await self._outbox_publisher.publish(
                    OutboxEvent(
                        event_id=uuid4(),
                        region_id=DEFAULT_REGION,
                        event_type="CampaignCancelRequested",
                        payload=payload,
                        routing_key=f"notification.{DEFAULT_REGION}.cancel.requested",
                        dedupe_key=dedupe_key,
                        transport_mode="cdc",
                    )
                )

            response_payload: dict[str, object] = {"campaignId": str(campaign.id), "status": next_status.value}
            await complete_idempotent_request(
                session=self._session,
                scope=request.idempotency_scope,
                key=request.idempotency_key,
                payload=response_payload,
                status_code=HTTP_202_ACCEPTED,
            )
            await self._session.commit()
            return CancelCampaignResponse(status_code=HTTP_202_ACCEPTED, payload=response_payload)
        except Exception:
            await self._session.rollback()
            await fail_idempotent_request(
                session=self._session,
                scope=request.idempotency_scope,
                key=request.idempotency_key,
            )
            await self._session.commit()
            raise
