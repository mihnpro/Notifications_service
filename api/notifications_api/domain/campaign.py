from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING
from uuid import UUID

DEFAULT_REGION = "default"
RECIPIENT_IDS_MAX_SIZE = 10_000

if TYPE_CHECKING:
    from datetime import datetime


class CampaignStatus(StrEnum):
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIALLY_FAILED = "partially_failed"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class CampaignRegionRunStatus(StrEnum):
    FANOUT_PENDING = "fanout_pending"
    FANOUT_RUNNING = "fanout_running"
    FANOUT_COMPLETED = "fanout_completed"
    FANOUT_FAILED = "fanout_failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class CampaignPriority(StrEnum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"


class RecipientSelectorType(StrEnum):
    ALL = "all"
    USER_IDS = "user_ids"
    EXTERNAL_IDS = "external_ids"
    SEGMENT = "segment"


FINAL_CAMPAIGN_STATUSES = {
    CampaignStatus.COMPLETED,
    CampaignStatus.PARTIALLY_FAILED,
    CampaignStatus.FAILED,
    CampaignStatus.CANCELLED,
}


@dataclass(slots=True, frozen=True)
class DomainValidationError(Exception):
    message: str
    details: dict[str, object]


@dataclass(slots=True, frozen=True)
class RecipientSelector:
    type: RecipientSelectorType
    user_ids: tuple[UUID, ...] | None = None
    external_ids: tuple[str, ...] | None = None
    filter: Mapping[str, object] | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> RecipientSelector:
        raw_type = payload.get("type")
        if not isinstance(raw_type, str):
            raise DomainValidationError("Unsupported recipientSelector.type", {"type": raw_type})
        try:
            selector_type = RecipientSelectorType(raw_type)
        except ValueError as exc:
            raise DomainValidationError("Unsupported recipientSelector.type", {"type": raw_type}) from exc

        raw_user_ids = payload.get("user_ids", payload.get("userIds"))
        user_ids: list[UUID] | None = None
        if isinstance(raw_user_ids, list):
            user_ids = []
            for raw_user_id in raw_user_ids:
                if isinstance(raw_user_id, UUID):
                    user_ids.append(raw_user_id)
                    continue
                if not isinstance(raw_user_id, str):
                    raise DomainValidationError("recipientSelector.userIds must contain UUID values", {})
                try:
                    user_ids.append(UUID(raw_user_id))
                except ValueError as exc:
                    raise DomainValidationError("recipientSelector.userIds must contain UUID values", {}) from exc

        raw_external_ids = payload.get("external_ids", payload.get("externalIds"))
        external_ids: tuple[str, ...] | None = None
        if isinstance(raw_external_ids, list):
            if any(not isinstance(item, str) for item in raw_external_ids):
                raise DomainValidationError("recipientSelector.externalIds must contain strings", {})
            external_ids = tuple(raw_external_ids)

        raw_filter = payload.get("filter")
        selector_filter = raw_filter if isinstance(raw_filter, Mapping) else None

        selector = cls(
            type=selector_type,
            user_ids=tuple(user_ids) if user_ids is not None else None,
            external_ids=external_ids,
            filter=selector_filter,
        )
        selector.validate()
        return selector

    def validate(self) -> None:
        if self.type == RecipientSelectorType.ALL:
            return
        if self.type == RecipientSelectorType.USER_IDS:
            if not self.user_ids:
                raise DomainValidationError("recipientSelector.userIds is required for type=user_ids", {})
            if len(self.user_ids) > RECIPIENT_IDS_MAX_SIZE:
                raise DomainValidationError(
                    "recipientSelector.userIds exceeds max size",
                    {"max": RECIPIENT_IDS_MAX_SIZE},
                )
            return
        if self.type == RecipientSelectorType.EXTERNAL_IDS:
            if not self.external_ids:
                raise DomainValidationError("recipientSelector.externalIds is required for type=external_ids", {})
            if len(self.external_ids) > RECIPIENT_IDS_MAX_SIZE:
                raise DomainValidationError(
                    "recipientSelector.externalIds exceeds max size",
                    {"max": RECIPIENT_IDS_MAX_SIZE},
                )
            return
        if self.type == RecipientSelectorType.SEGMENT and not self.filter:
            raise DomainValidationError("recipientSelector.filter is required for type=segment", {})


@dataclass(slots=True, frozen=True)
class Channel:
    id: UUID
    code: str
    state: str


@dataclass(slots=True, frozen=True)
class Campaign:
    id: UUID
    manager_id: UUID
    name: str
    status: CampaignStatus
    message_snapshot: Mapping[str, object]
    recipient_selector: RecipientSelector
    selected_channel_codes: tuple[str, ...]
    priority: CampaignPriority
    created_at: datetime
    completed_at: datetime | None

    def can_request_cancel(self) -> bool:
        return self.status not in FINAL_CAMPAIGN_STATUSES and self.status != CampaignStatus.CANCELLING


@dataclass(slots=True, frozen=True)
class CampaignRegionRun:
    id: UUID
    campaign_id: UUID
    region_id: str
    status: CampaignRegionRunStatus


@dataclass(slots=True, frozen=True)
class CampaignStats:
    campaign_id: UUID
    region_id: str
    total_tasks: int
    queued: int
    sending: int
    succeeded: int
    failed: int
    retry_scheduled: int
    dead_lettered: int
    cancelled: int
    updated_at: datetime


@dataclass(slots=True, frozen=True)
class DeliveryRecord:
    task_id: UUID
    region_id: str
    user_id: UUID
    recipient_address_snapshot: str
    channel_code: str
    message_snapshot: Mapping[str, object]
    status: str
    attempt_count: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    available_at: datetime | None = None
    provider_code: str | None = None
    provider_request_id: str | None = None
    last_error_code: str | None = None
    last_error_message: str | None = None


@dataclass(slots=True, frozen=True)
class DeliveryError:
    id: UUID
    task_id: UUID
    channel_code: str
    status: str
    error_type: str | None
    error_code: str | None
    error_message: str | None
    provider_code: str | None
    provider_request_id: str | None
    started_at: datetime
    completed_at: datetime | None
