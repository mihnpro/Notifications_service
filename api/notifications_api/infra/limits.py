from pydantic import BaseModel, Field


class LimitsConfig(BaseModel):
    idempotency_ttl_seconds: int = Field(default=3600, ge=1)
    pagination_default_limit: int = Field(default=50, ge=1)
    pagination_max_limit: int = Field(default=200, ge=1)
    dlq_replay_max_limit: int = Field(default=1000, ge=1)
    dlq_replay_max_additional_attempts: int = Field(default=10, ge=0)
    users_bulk_max_batch: int = Field(default=1000, ge=1)
