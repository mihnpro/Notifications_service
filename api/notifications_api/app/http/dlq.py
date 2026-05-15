from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from litestar import Request, get, post
from litestar.datastructures import State
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import DeliveryTaskORM, DlqItemORM, OutboxEventORM
from notifications_api.app.http.auth import ManagerIdentity
from notifications_api.app.http.errors import ApiError, raise_validation
from notifications_api.app.http.idempotency import (
    build_scope,
    complete_idempotent_request,
    extract_idempotency_key,
    fail_idempotent_request,
    idempotency_ttl,
    payload_hash,
    start_idempotent_request,
)
from notifications_api.app.http.pagination import build_cursor_filter, decode_cursor, encode_cursor
from notifications_api.app.http.schemas import ApiModel
from notifications_api.infra.config import GlobalConfig

DEFAULT_REGION = "default"


class DlqReplayFilter(ApiModel):
    campaign_id: UUID | None = None
    channel: str | None = None
    error_code: str | None = None


class DlqReplayRequest(ApiModel):
    region_id: str
    filter: DlqReplayFilter
    limit: int = 100
    additional_attempts: int = 0
    reason: str | None = None


@get("/dlq")
async def list_dlq(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    campaign_id: UUID | None = None,
    channel: str | None = None,
    error_code: str | None = None,
    status: str | None = "open",
) -> Response[dict[str, object]]:
    _ = manager
    applied_limit = limit or config.pagination_default_limit
    if applied_limit < 1 or applied_limit > config.pagination_max_limit:
        raise_validation(
            "Invalid pagination limit",
            {"min": 1, "max": config.pagination_max_limit},
        )
    stmt = sa.select(DlqItemORM).where(DlqItemORM.region_id == DEFAULT_REGION)
    if campaign_id:
        stmt = stmt.where(DlqItemORM.campaign_id == campaign_id)
    if channel:
        stmt = stmt.where(DlqItemORM.channel_code == channel)
    if error_code:
        stmt = stmt.where(DlqItemORM.error_code == error_code)
    if status:
        stmt = stmt.where(DlqItemORM.status == status)
    if cursor:
        parsed = decode_cursor(cursor)
        stmt = stmt.where(build_cursor_filter(DlqItemORM.created_at, DlqItemORM.id, parsed))
    stmt = stmt.order_by(DlqItemORM.created_at.desc(), DlqItemORM.id.desc()).limit(applied_limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_next = len(rows) > applied_limit
    rows = rows[:applied_limit]
    payload: dict[str, object] = {
        "items": [
            {
                "id": str(row.id),
                "regionId": row.region_id,
                "taskId": str(row.task_id),
                "campaignId": str(row.campaign_id),
                "channel": row.channel_code,
                "reasonCode": row.reason_code,
                "errorCode": row.error_code,
                "errorMessage": row.error_message,
                "status": row.status,
                "createdAt": row.created_at.astimezone(UTC).isoformat(),
                "lastReplayedAt": row.last_replayed_at.astimezone(UTC).isoformat() if row.last_replayed_at else None,
            }
            for row in rows
        ],
        "nextCursor": encode_cursor(rows[-1].created_at, rows[-1].id) if has_next and rows else None,
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@post("/dlq/replay")
async def replay_dlq(
    data: DlqReplayRequest,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    if data.region_id != DEFAULT_REGION:
        raise_validation("Only regionId=default is supported in MVP")
    if data.limit < 1 or data.limit > config.dlq_replay_max_limit:
        raise_validation("Replay limit is out of range", {"max": config.dlq_replay_max_limit})
    if data.additional_attempts < 0 or data.additional_attempts > config.dlq_replay_max_additional_attempts:
        raise_validation(
            "additionalAttempts is out of range",
            {"max": config.dlq_replay_max_additional_attempts},
        )

    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    start_result = await start_idempotent_request(
        session=session,
        scope=scope,
        key=idempotency_key,
        request_hash=payload_hash(data.model_dump(by_alias=True, mode="json")),
        ttl_seconds=idempotency_ttl(config),
    )
    await session.commit()
    if start_result.is_replay and start_result.replay is not None:
        return Response(content=start_result.replay.payload, status_code=start_result.replay.status_code)

    try:
        if config.feature_dlq_replay_noop:
            noop_payload: dict[str, object] = {
                "requested": data.limit,
                "replayed": 0,
                "skipped": 0,
                "execution": "noop_backend_unavailable",
                "consistency": "eventual",
            }
            await complete_idempotent_request(
                session=session,
                scope=scope,
                key=idempotency_key,
                payload=noop_payload,
                status_code=HTTP_200_OK,
            )
            await session.commit()
            return Response(content=noop_payload, status_code=HTTP_200_OK)

        stmt = sa.select(DlqItemORM).where(
            DlqItemORM.region_id == DEFAULT_REGION,
            DlqItemORM.status == "open",
        )
        if data.filter.campaign_id:
            stmt = stmt.where(DlqItemORM.campaign_id == data.filter.campaign_id)
        if data.filter.channel:
            stmt = stmt.where(DlqItemORM.channel_code == data.filter.channel)
        if data.filter.error_code:
            stmt = stmt.where(DlqItemORM.error_code == data.filter.error_code)
        stmt = (
            stmt
            .order_by(DlqItemORM.created_at.asc(), DlqItemORM.id.asc())
            .limit(data.limit)
            .with_for_update(skip_locked=True)
        )
        dlq_items = (await session.execute(stmt)).scalars().all()

        replayed = 0
        skipped = 0
        now = datetime.now(tz=UTC)
        for item in dlq_items:
            task = (
                await session.execute(
                    sa.select(DeliveryTaskORM).where(DeliveryTaskORM.id == item.task_id).with_for_update()
                )
            ).scalar_one_or_none()
            if task is None or task.status != "dead_lettered":
                skipped += 1
                continue
            task.status = "queued"
            task.available_at = now
            task.lease_owner = None
            task.lease_token = None
            task.lease_until = None
            if data.additional_attempts > 0:
                task.max_attempts = task.max_attempts + data.additional_attempts

            dedupe_key = (
                f"task-retry-scheduled:{DEFAULT_REGION}:{task.campaign_region_run_id}:{task.id}:{task.attempt_count}"
            )
            outbox_payload = {
                "messageType": "TaskRetryScheduled",
                "version": 1,
                "taskId": str(task.id),
                "campaignId": str(task.campaign_id),
                "campaignRegionRunId": str(task.campaign_region_run_id),
                "regionId": DEFAULT_REGION,
                "channelCode": task.channel_code,
                "queueGroup": task.queue_group,
                "priority": task.priority,
                "retryDelaySeconds": 0,
                "availableAt": task.available_at.astimezone(UTC).isoformat(),
                "dedupeKey": dedupe_key,
            }
            session.add(
                OutboxEventORM(
                    id=uuid4(),
                    region_id=DEFAULT_REGION,
                    event_type="TaskRetryScheduled",
                    payload=outbox_payload,
                    routing_key=f"notification.{DEFAULT_REGION}.{task.queue_group}.retry.0s",
                    dedupe_key=dedupe_key,
                    status="pending",
                    transport_mode="rabbitmq_direct",
                )
            )
            item.status = "replayed"
            item.last_replayed_at = now
            replayed += 1

        payload: dict[str, object] = {
            "requested": data.limit,
            "matched": len(dlq_items),
            "replayed": replayed,
            "skipped": skipped,
            "execution": "performed",
            "consistency": "eventual",
        }
        await complete_idempotent_request(
            session=session,
            scope=scope,
            key=idempotency_key,
            payload=payload,
            status_code=HTTP_200_OK,
        )
        await session.commit()
        return Response(content=payload, status_code=HTTP_200_OK)
    except ApiError:
        await session.rollback()
        await fail_idempotent_request(session=session, scope=scope, key=idempotency_key)
        await session.commit()
        raise
    except Exception:
        await session.rollback()
        await fail_idempotent_request(session=session, scope=scope, key=idempotency_key)
        await session.commit()
        raise
