from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, ConfigDict


class OutboxRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    exchange: str
    routing_key: str
    payload: str
    event_type: str
    dedupe_key: str
    attempt_count: int
