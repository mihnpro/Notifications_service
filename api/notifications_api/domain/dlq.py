from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class DlqStatus(StrEnum):
    OPEN = "open"
    REPLAYED = "replayed"
    IGNORED = "ignored"


class DeliveryStatus(StrEnum):
    QUEUED = "queued"
    SENDING = "sending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class DlqReasonCode(StrEnum):
    MAX_ATTEMPTS_EXCEEDED = "max_attempts_exceeded"
    PERMANENT_ERROR = "permanent_error"
    PROVIDER_REJECTED = "provider_rejected"
    TTL_EXPIRED = "ttl_expired"
    BAD_PAYLOAD = "bad_payload"
    UNROUTED = "unrouted"
    UNKNOWN = "unknown"


class DeliveryAttemptStatus(StrEnum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    STALE = "stale"


class DeliveryAttemptErrorType(StrEnum):
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"


class OutboxEventType(StrEnum):
    TASK_REPLAY = "task.replay"


class StatsCounter(StrEnum):
    """campaign_stats column names that may be inc/dec-ed by stats helper."""

    QUEUED = "queued"
    SENDING = "sending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


class DlqItemView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_id: UUID
    campaign_id: UUID
    channel_code: str
    reason_code: str
    error_code: str | None
    error_message: str | None
    status: DlqStatus
    created_at: datetime
    last_replayed_at: datetime | None


class ReplayRequest(BaseModel):
    dlq_item_ids: list[UUID] = Field(min_length=1, max_length=500)


class ReplayResult(BaseModel):
    replayed: list[UUID]
    skipped: list[UUID]
    not_found: list[UUID]
