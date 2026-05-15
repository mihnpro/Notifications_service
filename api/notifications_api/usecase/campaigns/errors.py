from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CampaignUsecaseValidationError(Exception):
    message: str
    details: dict[str, object]


@dataclass(slots=True, frozen=True)
class CampaignUsecaseNotFoundError(Exception):
    message: str
