from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from notifications_api.app.http.idempotency import IdempotencyStartResult
from notifications_api.domain import (
    Campaign,
    CampaignPriority,
    CampaignStatus,
    RecipientSelector,
    RecipientSelectorType,
)
from notifications_api.protocol.campaign import OutboxEvent
from notifications_api.usecase.campaigns.cancel_campaign import (
    CancelCampaignRequest,
    CancelCampaignUsecase,
)


class _SessionStub:
    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class _CampaignRepositoryStub:
    def __init__(self, campaign: Campaign) -> None:
        self._campaign = campaign
        self.updated: list[tuple[UUID, str]] = []

    async def get_campaign_for_manager(
        self,
        campaign_id: UUID,
        manager_id: UUID,
        *,
        for_update: bool = False,  # noqa: ARG002
    ) -> Campaign | None:
        if self._campaign.id == campaign_id and self._campaign.manager_id == manager_id:
            return self._campaign
        return None

    async def update_campaign_status(self, campaign_id: UUID, status: str) -> None:
        self.updated.append((campaign_id, status))


class _OutboxPublisherStub:
    def __init__(self) -> None:
        self.events: list[OutboxEvent] = []

    async def publish(self, event: OutboxEvent) -> None:
        self.events.append(event)


def _build_campaign(status: CampaignStatus) -> Campaign:
    return Campaign(
        id=uuid4(),
        manager_id=uuid4(),
        name="Campaign",
        status=status,
        message_snapshot={"subject": "s", "body": "b"},
        recipient_selector=RecipientSelector(type=RecipientSelectorType.ALL),
        selected_channel_codes=("email",),
        priority=CampaignPriority.NORMAL,
        created_at=datetime.now(tz=UTC),
        completed_at=None,
    )


def _patch_idempotency(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _start_idempotent_request(**_kwargs: object) -> IdempotencyStartResult:
        return IdempotencyStartResult(is_replay=False)

    async def _complete_idempotent_request(**_kwargs: object) -> None:
        return None

    async def _fail_idempotent_request(**_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        "notifications_api.usecase.campaigns.cancel_campaign.start_idempotent_request",
        _start_idempotent_request,
    )
    monkeypatch.setattr(
        "notifications_api.usecase.campaigns.cancel_campaign.complete_idempotent_request",
        _complete_idempotent_request,
    )
    monkeypatch.setattr(
        "notifications_api.usecase.campaigns.cancel_campaign.fail_idempotent_request",
        _fail_idempotent_request,
    )


@pytest.mark.asyncio
async def test_cancel_running_campaign_sets_cancelling_and_emits_event(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_idempotency(monkeypatch)
    campaign = _build_campaign(CampaignStatus.RUNNING)
    repository = _CampaignRepositoryStub(campaign)
    outbox = _OutboxPublisherStub()
    usecase = CancelCampaignUsecase(
        session=_SessionStub(),
        campaign_repository=repository,  # type: ignore[arg-type]
        outbox_publisher=outbox,  # type: ignore[arg-type]
    )

    result = await usecase.execute(
        CancelCampaignRequest(
            campaign_id=campaign.id,
            manager_id=campaign.manager_id,
            reason="manual",
            idempotency_scope="scope",
            idempotency_key="key",
            idempotency_payload={},
            idempotency_ttl_seconds=60,
        )
    )

    assert result.payload["status"] == "cancelling"
    assert repository.updated == [(campaign.id, "cancelling")]
    assert len(outbox.events) == 1
    assert outbox.events[0].event_type == "CampaignCancelRequested"
    assert outbox.events[0].transport_mode == "cdc"


@pytest.mark.asyncio
async def test_cancel_already_cancelled_campaign_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_idempotency(monkeypatch)
    campaign = _build_campaign(CampaignStatus.CANCELLED)
    repository = _CampaignRepositoryStub(campaign)
    outbox = _OutboxPublisherStub()
    usecase = CancelCampaignUsecase(
        session=_SessionStub(),
        campaign_repository=repository,  # type: ignore[arg-type]
        outbox_publisher=outbox,  # type: ignore[arg-type]
    )

    result = await usecase.execute(
        CancelCampaignRequest(
            campaign_id=campaign.id,
            manager_id=campaign.manager_id,
            reason="manual",
            idempotency_scope="scope",
            idempotency_key="key-2",
            idempotency_payload={},
            idempotency_ttl_seconds=60,
        )
    )

    assert result.payload["status"] == "cancelled"
    assert repository.updated == []
    assert outbox.events == []
