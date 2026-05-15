from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from litestar import Request
from litestar.status_codes import HTTP_409_CONFLICT
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import IdempotencyKeyORM
from notifications_api.app.http.auth import ManagerIdentity
from notifications_api.app.http.errors import ApiError, raise_validation
from notifications_api.infra.config import GlobalConfig


@dataclass(slots=True, frozen=True)
class ReplayResponse:
    status_code: int
    payload: dict[str, Any]


@dataclass(slots=True, frozen=True)
class IdempotencyStartResult:
    is_replay: bool
    replay: ReplayResponse | None = None


def canonicalize_payload(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def payload_hash(payload: Any) -> str:
    canonical = canonicalize_payload(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def extract_idempotency_key(request: Request[Any, Any, Any]) -> str:
    value = request.headers.get("Idempotency-Key")
    if value is None or not value.strip():
        raise_validation("Missing Idempotency-Key header")
    trimmed = value.strip()
    if len(trimmed) > 255:
        raise_validation("Idempotency-Key is too long")
    return trimmed


def build_scope(request: Request[Any, Any, Any], manager: ManagerIdentity) -> str:
    route_handler = request.route_handler
    paths = route_handler.paths
    if paths:
        route_template = sorted(paths)[0]
    else:
        route_template = request.url.path
    return f"{manager.manager_id}:{request.method.upper()}:{route_template}"


async def start_idempotent_request(
    *,
    session: AsyncSession,
    scope: str,
    key: str,
    request_hash: str,
    ttl_seconds: int,
) -> IdempotencyStartResult:
    stmt = (
        sa.select(IdempotencyKeyORM)
        .where(
            IdempotencyKeyORM.scope == scope,
            IdempotencyKeyORM.key == key,
        )
        .with_for_update()
    )
    existing = (await session.execute(stmt)).scalar_one_or_none()
    now = datetime.now(tz=UTC)
    expires_at = now + timedelta(seconds=ttl_seconds)

    if existing is None:
        session.add(
            IdempotencyKeyORM(
                scope=scope,
                key=key,
                request_hash=request_hash,
                status="processing",
                expires_at=expires_at,
            )
        )
        await session.flush()
        return IdempotencyStartResult(is_replay=False)

    if existing.request_hash != request_hash:
        raise ApiError(
            status_code=HTTP_409_CONFLICT,
            code="IDEMPOTENCY_KEY_REUSED",
            message="Idempotency key was already used with a different request body",
        )

    if existing.status == "processing":
        raise ApiError(
            status_code=HTTP_409_CONFLICT,
            code="REQUEST_ALREADY_PROCESSING",
            message="Request with the same idempotency key is already processing",
        )

    if existing.status == "completed" and existing.response_payload:
        response_payload = existing.response_payload
        status_code_raw = response_payload.get("statusCode")
        payload_raw = response_payload.get("payload")
        if isinstance(status_code_raw, int) and isinstance(payload_raw, dict):
            return IdempotencyStartResult(
                is_replay=True,
                replay=ReplayResponse(status_code=status_code_raw, payload=payload_raw),
            )

    existing.status = "processing"
    existing.response_payload = None
    existing.expires_at = expires_at
    await session.flush()
    return IdempotencyStartResult(is_replay=False)


async def complete_idempotent_request(
    *,
    session: AsyncSession,
    scope: str,
    key: str,
    payload: dict[str, Any],
    status_code: int,
) -> None:
    stmt = sa.select(IdempotencyKeyORM).where(
        IdempotencyKeyORM.scope == scope,
        IdempotencyKeyORM.key == key,
    )
    record = (await session.execute(stmt)).scalar_one()
    record.status = "completed"
    record.response_payload = {"statusCode": status_code, "payload": payload}
    await session.flush()


async def fail_idempotent_request(
    *,
    session: AsyncSession,
    scope: str,
    key: str,
) -> None:
    stmt = sa.select(IdempotencyKeyORM).where(
        IdempotencyKeyORM.scope == scope,
        IdempotencyKeyORM.key == key,
    )
    record = (await session.execute(stmt)).scalar_one_or_none()
    if record is None:
        return
    record.status = "failed"
    await session.flush()


def idempotency_ttl(config: GlobalConfig) -> int:
    return max(config.idempotency_ttl_seconds, 60)
