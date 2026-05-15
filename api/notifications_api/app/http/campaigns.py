from __future__ import annotations

from datetime import UTC
from typing import Annotated, Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from litestar import Request, get, post
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK, HTTP_202_ACCEPTED
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import (
    CampaignORM,
    CampaignRegionRunORM,
    CampaignStatsORM,
    ChannelORM,
    DeliveryAttemptORM,
    DeliveryResultORM,
    DeliveryTaskORM,
    OutboxEventORM,
)
from notifications_api.app.http.auth import ManagerIdentity
from notifications_api.app.http.errors import ApiError, raise_not_found, raise_validation
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
FINAL_CAMPAIGN_STATUSES = {"completed", "partially_failed", "failed", "cancelled"}
CAMPAIGN_PRIORITIES = {"low", "normal", "high"}
RECIPIENT_SELECTOR_TYPES = {"all", "user_ids", "external_ids", "segment"}


class RecipientSelector(ApiModel):
    type: str
    user_ids: list[UUID] | None = None
    external_ids: list[str] | None = None
    filter: dict[str, object] | None = None


class CreateCampaignRequest(ApiModel):
    name: str
    region_ids: list[str]
    message: dict[str, object]
    recipient_selector: RecipientSelector
    channels: list[str]
    priority: str = "normal"


class CancelCampaignRequest(ApiModel):
    reason: str | None = None


def _validate_limit(raw_limit: int | None, config: GlobalConfig) -> int:
    limit = raw_limit or config.pagination_default_limit
    if limit < 1 or limit > config.pagination_max_limit:
        raise_validation(
            "Invalid pagination limit",
            {"min": 1, "max": config.pagination_max_limit},
        )
    return limit


def _validate_regions(region_ids: list[str]) -> None:
    normalized = sorted(set(region_ids))
    if normalized != [DEFAULT_REGION]:
        raise_validation("Only regionIds=['default'] is supported in MVP")


def _validate_selector(selector: RecipientSelector) -> None:
    if selector.type not in RECIPIENT_SELECTOR_TYPES:
        raise_validation("Unsupported recipientSelector.type", {"type": selector.type})
    if selector.type == "all":
        return
    if selector.type == "user_ids":
        if not selector.user_ids:
            raise_validation("recipientSelector.userIds is required for type=user_ids")
        if len(selector.user_ids) > 10_000:
            raise_validation("recipientSelector.userIds exceeds max size", {"max": 10_000})
        return
    if selector.type == "external_ids":
        if not selector.external_ids:
            raise_validation("recipientSelector.externalIds is required for type=external_ids")
        if len(selector.external_ids) > 10_000:
            raise_validation("recipientSelector.externalIds exceeds max size", {"max": 10_000})
        return
    if selector.type == "segment":
        if not selector.filter:
            raise_validation("recipientSelector.filter is required for type=segment")


async def _load_campaign_for_manager(
    session: AsyncSession,
    campaign_id: UUID,
    manager_id: UUID,
) -> CampaignORM:
    campaign = (
        await session.execute(
            sa.select(CampaignORM).where(
                CampaignORM.id == campaign_id,
                CampaignORM.manager_id == manager_id,
            )
        )
    ).scalar_one_or_none()
    if campaign is None:
        raise_not_found("Campaign not found")
    return campaign


def _campaign_to_item(campaign: CampaignORM) -> dict[str, object]:
    return {
        "campaignId": str(campaign.id),
        "name": campaign.name,
        "status": campaign.status,
        "priority": campaign.priority,
        "regionIds": [DEFAULT_REGION],
        "channels": campaign.selected_channel_codes,
        "createdAt": campaign.created_at.astimezone(UTC).isoformat(),
        "completedAt": campaign.completed_at.astimezone(UTC).isoformat() if campaign.completed_at else None,
    }


def _validate_channels_ready(channels: list[ChannelORM], codes: list[str]) -> None:
    by_code = {channel.code: channel for channel in channels}
    missing = sorted(set(codes) - set(by_code))
    if missing:
        raise_validation("Some channels were not found", {"missingChannels": missing})
    disabled = [channel.code for channel in channels if channel.state == "disabled"]
    if disabled:
        raise_validation(
            "Selected channels are disabled in region default",
            {"disabledChannels": sorted(disabled), "regionId": DEFAULT_REGION},
        )


@post("/campaigns")
async def create_campaign(
    data: CreateCampaignRequest,
    request: Request[object, object, object],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    _validate_regions(data.region_ids)
    _validate_selector(data.recipient_selector)
    if not data.channels:
        raise_validation("At least one channel is required")
    if data.priority not in CAMPAIGN_PRIORITIES:
        raise_validation("Invalid priority", {"allowed": sorted(CAMPAIGN_PRIORITIES)})
    if not data.message:
        raise_validation("message must not be empty")

    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    request_hash = payload_hash(data.model_dump(by_alias=True, mode="json"))

    start_result = await start_idempotent_request(
        session=session,
        scope=scope,
        key=idempotency_key,
        request_hash=request_hash,
        ttl_seconds=idempotency_ttl(config),
    )
    await session.commit()

    if start_result.is_replay and start_result.replay is not None:
        return Response(content=start_result.replay.payload, status_code=start_result.replay.status_code)

    try:
        channels = (
            await session.execute(sa.select(ChannelORM).where(ChannelORM.code.in_(data.channels)))
        ).scalars().all()
        _validate_channels_ready(channels, data.channels)

        campaign = CampaignORM(
            id=uuid4(),
            manager_id=manager.manager_id,
            name=data.name,
            status="running",
            message_snapshot=data.message,
            recipient_selector=data.recipient_selector.model_dump(by_alias=True, mode="json", exclude_none=True),
            selected_channel_codes=data.channels,
            priority=data.priority,
            create_idempotency_key=idempotency_key,
        )
        session.add(campaign)
        await session.flush()

        region_run = CampaignRegionRunORM(
            id=uuid4(),
            campaign_id=campaign.id,
            region_id=DEFAULT_REGION,
            status="fanout_pending",
        )
        session.add(region_run)
        await session.flush()

        event_payload = {
            "messageType": "CampaignRegionRunRequested",
            "version": 1,
            "campaignId": str(campaign.id),
            "campaignRegionRunId": str(region_run.id),
            "regionId": DEFAULT_REGION,
            "priority": data.priority,
            "dedupeKey": f"campaign-region-run-requested:{DEFAULT_REGION}:{region_run.id}",
        }
        session.add(
            OutboxEventORM(
                id=uuid4(),
                region_id=DEFAULT_REGION,
                event_type="CampaignRegionRunRequested",
                payload=event_payload,
                routing_key=f"notification.{DEFAULT_REGION}.fanout.{data.priority}",
                dedupe_key=event_payload["dedupeKey"],
                status="pending",
                transport_mode="rabbitmq_direct",
            )
        )

        response_payload = {
            "campaignId": str(campaign.id),
            "status": campaign.status,
            "regionRuns": [
                {
                    "id": str(region_run.id),
                    "regionId": region_run.region_id,
                    "status": region_run.status,
                }
            ],
        }
        await complete_idempotent_request(
            session=session,
            scope=scope,
            key=idempotency_key,
            payload=response_payload,
            status_code=HTTP_202_ACCEPTED,
        )
        await session.commit()
        return Response(content=response_payload, status_code=HTTP_202_ACCEPTED)
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


@post("/campaigns/{campaign_id:uuid}/cancel")
async def cancel_campaign(
    campaign_id: UUID,
    request: Request[object, object, object],
    data: CancelCampaignRequest,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    request_hash = payload_hash(
        {
            "campaignId": str(campaign_id),
            **data.model_dump(by_alias=True, mode="json", exclude_none=True),
        }
    )

    start_result = await start_idempotent_request(
        session=session,
        scope=scope,
        key=idempotency_key,
        request_hash=request_hash,
        ttl_seconds=idempotency_ttl(config),
    )
    await session.commit()

    if start_result.is_replay and start_result.replay is not None:
        return Response(content=start_result.replay.payload, status_code=start_result.replay.status_code)

    try:
        campaign = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
        if campaign.status not in FINAL_CAMPAIGN_STATUSES and campaign.status != "cancelling":
            campaign.status = "cancelling"
            dedupe_key = f"campaign-cancel-requested:{DEFAULT_REGION}:{campaign.id}"
            payload = {
                "messageType": "CampaignCancelRequested",
                "version": 1,
                "campaignId": str(campaign.id),
                "regionId": DEFAULT_REGION,
                "reason": data.reason or "manual_cancel",
                "dedupeKey": dedupe_key,
            }
            session.add(
                OutboxEventORM(
                    id=uuid4(),
                    region_id=DEFAULT_REGION,
                    event_type="CampaignCancelRequested",
                    payload=payload,
                    routing_key=f"notification.{DEFAULT_REGION}.fanout.high",
                    dedupe_key=dedupe_key,
                    status="pending",
                    transport_mode="rabbitmq_direct",
                )
            )

        response_payload = {"campaignId": str(campaign.id), "status": campaign.status}
        await complete_idempotent_request(
            session=session,
            scope=scope,
            key=idempotency_key,
            payload=response_payload,
            status_code=HTTP_202_ACCEPTED,
        )
        await session.commit()
        return Response(content=response_payload, status_code=HTTP_202_ACCEPTED)
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


@get("/campaigns")
async def list_campaigns(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
) -> Response[dict[str, object]]:
    applied_limit = _validate_limit(limit, config)
    stmt = sa.select(CampaignORM).where(CampaignORM.manager_id == manager.manager_id)
    if status:
        stmt = stmt.where(CampaignORM.status == status)
    if cursor:
        parsed = decode_cursor(cursor)
        stmt = stmt.where(build_cursor_filter(CampaignORM.created_at, CampaignORM.id, parsed))
    stmt = stmt.order_by(CampaignORM.created_at.desc(), CampaignORM.id.desc()).limit(applied_limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_next = len(rows) > applied_limit
    rows = rows[:applied_limit]
    next_cursor = None
    if has_next and rows:
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, last.id)
    payload = {"items": [_campaign_to_item(row) for row in rows], "nextCursor": next_cursor}
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}")
async def get_campaign(
    campaign_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    campaign = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
    return Response(content=_campaign_to_item(campaign), status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/stats")
async def campaign_stats(
    campaign_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    campaign = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
    stats = (
        await session.execute(
            sa.select(CampaignStatsORM).where(
                CampaignStatsORM.campaign_id == campaign.id,
                CampaignStatsORM.region_id == DEFAULT_REGION,
            )
        )
    ).scalar_one_or_none()
    updated_at = campaign.created_at
    stats_payload: dict[str, object] = {
        "totalTasks": 0,
        "queued": 0,
        "sending": 0,
        "succeeded": 0,
        "failed": 0,
        "retryScheduled": 0,
        "deadLettered": 0,
        "cancelled": 0,
    }
    if stats:
        updated_at = stats.updated_at
        stats_payload = {
            "totalTasks": int(stats.total_tasks),
            "queued": int(stats.queued),
            "sending": int(stats.sending),
            "succeeded": int(stats.succeeded),
            "failed": int(stats.failed),
            "retryScheduled": int(stats.retry_scheduled),
            "deadLettered": int(stats.dead_lettered),
            "cancelled": int(stats.cancelled),
        }
    payload = {
        "campaignId": str(campaign.id),
        "status": campaign.status,
        "stats": stats_payload,
        "consistency": "eventual",
        "updatedAt": updated_at.astimezone(UTC).isoformat(),
    }
    return Response(content=payload, status_code=HTTP_200_OK)


def _serialize_message(snapshot: dict[str, Any]) -> dict[str, object]:
    return {key: value for key, value in snapshot.items() if isinstance(key, str)}


@get("/campaigns/{campaign_id:uuid}/tasks")
async def campaign_tasks(
    campaign_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
    channel: str | None = None,
) -> Response[dict[str, object]]:
    _ = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
    applied_limit = _validate_limit(limit, config)
    stmt = sa.select(DeliveryTaskORM).where(DeliveryTaskORM.campaign_id == campaign_id)
    if status:
        stmt = stmt.where(DeliveryTaskORM.status == status)
    if channel:
        stmt = stmt.where(DeliveryTaskORM.channel_code == channel)
    if cursor:
        parsed = decode_cursor(cursor)
        stmt = stmt.where(build_cursor_filter(DeliveryTaskORM.created_at, DeliveryTaskORM.id, parsed))
    stmt = stmt.order_by(DeliveryTaskORM.created_at.desc(), DeliveryTaskORM.id.desc()).limit(applied_limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_next = len(rows) > applied_limit
    rows = rows[:applied_limit]
    next_cursor = encode_cursor(rows[-1].created_at, rows[-1].id) if has_next and rows else None
    payload = {
        "items": [
            {
                "taskId": str(row.id),
                "regionId": row.region_id,
                "userId": str(row.user_id),
                "recipient": row.recipient_address_snapshot,
                "channel": row.channel_code,
                "message": _serialize_message(row.message_snapshot),
                "status": row.status,
                "attemptCount": row.attempt_count,
                "createdAt": row.created_at.astimezone(UTC).isoformat(),
                "startedAt": row.started_at.astimezone(UTC).isoformat() if row.started_at else None,
                "completedAt": row.completed_at.astimezone(UTC).isoformat() if row.completed_at else None,
                "availableAt": row.available_at.astimezone(UTC).isoformat(),
                "lastErrorCode": row.last_error_code,
                "lastErrorMessage": row.last_error_message,
            }
            for row in rows
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/results")
async def campaign_results(
    campaign_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
    channel: str | None = None,
) -> Response[dict[str, object]]:
    _ = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
    applied_limit = _validate_limit(limit, config)
    stmt = sa.select(DeliveryResultORM).where(DeliveryResultORM.campaign_id == campaign_id)
    if status:
        stmt = stmt.where(DeliveryResultORM.status == status)
    if channel:
        stmt = stmt.where(DeliveryResultORM.channel_code == channel)
    if cursor:
        parsed = decode_cursor(cursor)
        stmt = stmt.where(build_cursor_filter(DeliveryResultORM.completed_at, DeliveryResultORM.id, parsed))
    stmt = stmt.order_by(DeliveryResultORM.completed_at.desc(), DeliveryResultORM.id.desc()).limit(applied_limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_next = len(rows) > applied_limit
    rows = rows[:applied_limit]
    next_cursor = encode_cursor(rows[-1].completed_at, rows[-1].id) if has_next and rows else None
    payload = {
        "items": [
            {
                "taskId": str(row.task_id),
                "regionId": row.region_id,
                "userId": str(row.user_id),
                "recipient": row.recipient_address_snapshot,
                "channel": row.channel_code,
                "message": _serialize_message(row.message_snapshot),
                "status": row.status,
                "attemptCount": row.attempt_count,
                "providerCode": row.provider_code,
                "providerRequestId": row.provider_request_id,
                "createdAt": row.created_at.astimezone(UTC).isoformat(),
                "startedAt": row.started_at.astimezone(UTC).isoformat() if row.started_at else None,
                "completedAt": row.completed_at.astimezone(UTC).isoformat(),
            }
            for row in rows
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/errors")
async def campaign_errors(
    campaign_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    channel: str | None = None,
    error_code: str | None = None,
) -> Response[dict[str, object]]:
    _ = await _load_campaign_for_manager(session, campaign_id, manager.manager_id)
    applied_limit = _validate_limit(limit, config)
    stmt = sa.select(DeliveryAttemptORM).where(DeliveryAttemptORM.campaign_id == campaign_id)
    stmt = stmt.where(DeliveryAttemptORM.status.in_(["failed", "timed_out", "stale"]))
    if channel:
        stmt = stmt.where(DeliveryAttemptORM.channel_code == channel)
    if error_code:
        stmt = stmt.where(DeliveryAttemptORM.error_code == error_code)
    if cursor:
        parsed = decode_cursor(cursor)
        stmt = stmt.where(build_cursor_filter(DeliveryAttemptORM.started_at, DeliveryAttemptORM.id, parsed))
    stmt = stmt.order_by(DeliveryAttemptORM.started_at.desc(), DeliveryAttemptORM.id.desc()).limit(applied_limit + 1)
    rows = (await session.execute(stmt)).scalars().all()
    has_next = len(rows) > applied_limit
    rows = rows[:applied_limit]
    next_cursor = encode_cursor(rows[-1].started_at, rows[-1].id) if has_next and rows else None
    payload = {
        "items": [
            {
                "attemptId": str(row.id),
                "taskId": str(row.task_id),
                "channel": row.channel_code,
                "status": row.status,
                "errorType": row.error_type,
                "errorCode": row.error_code,
                "errorMessage": row.error_message,
                "providerCode": row.provider_code,
                "providerRequestId": row.provider_request_id,
                "startedAt": row.started_at.astimezone(UTC).isoformat(),
                "completedAt": row.completed_at.astimezone(UTC).isoformat() if row.completed_at else None,
            }
            for row in rows
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)
