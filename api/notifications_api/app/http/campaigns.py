from __future__ import annotations

from datetime import UTC
from typing import Annotated, Any, NoReturn
from uuid import UUID

from litestar import Request, get, post
from litestar.datastructures import State  # noqa: TC002
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK

from notifications_api.app.http.auth import ManagerIdentity
from notifications_api.app.http.errors import raise_not_found, raise_validation
from notifications_api.app.http.idempotency import build_scope, extract_idempotency_key, idempotency_ttl
from notifications_api.app.http.pagination import decode_cursor, encode_cursor
from notifications_api.app.http.schemas import ApiModel
from notifications_api.infra.config import GlobalConfig
from notifications_api.infra.metrics import CAMPAIGNS_CANCELLED, CAMPAIGNS_CREATED
from notifications_api.protocol.campaign import CursorPoint
from notifications_api.usecase.campaigns import (
    CampaignUsecaseNotFoundError,
    CampaignUsecaseValidationError,
    CancelCampaignUsecase,
    CreateCampaignUsecase,
    GetCampaignErrorsRequest,
    GetCampaignErrorsUsecase,
    GetCampaignRequest,
    GetCampaignResultsRequest,
    GetCampaignResultsUsecase,
    GetCampaignStatsUsecase,
    GetCampaignStatsViewRequest,
    GetCampaignTasksRequest,
    GetCampaignTasksUsecase,
    GetCampaignUsecase,
    ListCampaignsRequest,
    ListCampaignsUsecase,
)
from notifications_api.usecase.campaigns import CancelCampaignRequest as CancelCampaignCommand
from notifications_api.usecase.campaigns import CreateCampaignRequest as CreateCampaignCommand


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


def _campaign_to_item(campaign: Any) -> dict[str, object]:
    return {
        "campaignId": str(campaign.id),
        "name": campaign.name,
        "status": campaign.status.value,
        "priority": campaign.priority.value,
        "regionIds": ["default"],
        "channels": list(campaign.selected_channel_codes),
        "createdAt": campaign.created_at.astimezone(UTC).isoformat(),
        "completedAt": campaign.completed_at.astimezone(UTC).isoformat() if campaign.completed_at else None,
    }


def _serialize_message(snapshot: dict[str, Any]) -> dict[str, object]:
    return {key: value for key, value in snapshot.items() if isinstance(key, str)}


def _to_cursor_point(cursor: str | None) -> CursorPoint | None:
    if cursor is None:
        return None
    decoded = decode_cursor(cursor)
    return CursorPoint(timestamp=decoded.created_at, row_id=decoded.row_id)


def _handle_usecase_validation_error(exc: CampaignUsecaseValidationError) -> NoReturn:
    raise_validation(exc.message, exc.details)


def _handle_usecase_not_found_error(exc: CampaignUsecaseNotFoundError) -> NoReturn:
    raise_not_found(exc.message)


@post("/campaigns")
async def create_campaign(
    data: CreateCampaignRequest,
    request: Request[Any, Any, State],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    create_campaign_usecase: Annotated[CreateCampaignUsecase, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    try:
        result = await create_campaign_usecase.execute(
            request=CreateCampaignCommand(
                manager_id=manager.manager_id,
                name=data.name,
                region_ids=data.region_ids,
                message=data.message,
                recipient_selector=data.recipient_selector.model_dump(by_alias=True, mode="json", exclude_none=True),
                channels=data.channels,
                priority=data.priority,
                idempotency_scope=scope,
                idempotency_key=idempotency_key,
                idempotency_payload=data.model_dump(by_alias=True, mode="json"),
                idempotency_ttl_seconds=idempotency_ttl(config),
            )
        )
    except CampaignUsecaseValidationError as exc:
        _handle_usecase_validation_error(exc)

    CAMPAIGNS_CREATED.inc()
    return Response(content=result.payload, status_code=result.status_code)


@post("/campaigns/{campaign_id:uuid}/cancel")
async def cancel_campaign(
    campaign_id: UUID,
    request: Request[Any, Any, State],
    data: CancelCampaignRequest,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    cancel_campaign_usecase: Annotated[CancelCampaignUsecase, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    command = CancelCampaignCommand(
        campaign_id=campaign_id,
        manager_id=manager.manager_id,
        reason=data.reason,
        idempotency_scope=scope,
        idempotency_key=idempotency_key,
        idempotency_payload={
            "campaignId": str(campaign_id),
            **data.model_dump(by_alias=True, mode="json", exclude_none=True),
        },
        idempotency_ttl_seconds=idempotency_ttl(config),
    )

    try:
        result = await cancel_campaign_usecase.execute(command)
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

    CAMPAIGNS_CANCELLED.inc()
    return Response(content=result.payload, status_code=result.status_code)


@get("/campaigns")
async def list_campaigns(
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    list_campaigns_usecase: Annotated[ListCampaignsUsecase, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
) -> Response[dict[str, object]]:
    applied_limit = _validate_limit(limit, config)
    page = await list_campaigns_usecase.execute(
        ListCampaignsRequest(
            manager_id=manager.manager_id,
            limit=applied_limit,
            cursor=_to_cursor_point(cursor),
            status=status,
        )
    )
    next_cursor = None
    if page.next_cursor is not None:
        next_cursor = encode_cursor(page.next_cursor.timestamp, page.next_cursor.row_id)
    payload: dict[str, object] = {
        "items": [_campaign_to_item(campaign) for campaign in page.items],
        "nextCursor": next_cursor,
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}")
async def get_campaign(
    campaign_id: UUID,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    get_campaign_usecase: Annotated[GetCampaignUsecase, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    try:
        campaign = await get_campaign_usecase.execute(
            GetCampaignRequest(campaign_id=campaign_id, manager_id=manager.manager_id)
        )
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

    return Response(content=_campaign_to_item(campaign), status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/stats")
async def campaign_stats(
    campaign_id: UUID,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    get_campaign_stats_usecase: Annotated[GetCampaignStatsUsecase, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    try:
        view = await get_campaign_stats_usecase.execute(
            GetCampaignStatsViewRequest(campaign_id=campaign_id, manager_id=manager.manager_id)
        )
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

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
    if view.stats is not None:
        stats_payload = {
            "totalTasks": view.stats.total_tasks,
            "queued": view.stats.queued,
            "sending": view.stats.sending,
            "succeeded": view.stats.succeeded,
            "failed": view.stats.failed,
            "retryScheduled": view.stats.retry_scheduled,
            "deadLettered": view.stats.dead_lettered,
            "cancelled": view.stats.cancelled,
        }

    payload: dict[str, object] = {
        "campaignId": str(view.campaign.id),
        "status": view.campaign.status.value,
        "stats": stats_payload,
        "consistency": "eventual",
        "updatedAt": view.updated_at.astimezone(UTC).isoformat(),
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/tasks")
async def campaign_tasks(
    campaign_id: UUID,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    get_campaign_tasks_usecase: Annotated[GetCampaignTasksUsecase, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
    channel: str | None = None,
) -> Response[dict[str, object]]:
    applied_limit = _validate_limit(limit, config)
    try:
        page = await get_campaign_tasks_usecase.execute(
            GetCampaignTasksRequest(
                campaign_id=campaign_id,
                manager_id=manager.manager_id,
                limit=applied_limit,
                cursor=_to_cursor_point(cursor),
                status=status,
                channel=channel,
            )
        )
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

    next_cursor = None
    if page.next_cursor is not None:
        next_cursor = encode_cursor(page.next_cursor.timestamp, page.next_cursor.row_id)
    payload: dict[str, object] = {
        "items": [
            {
                "taskId": str(row.task_id),
                "regionId": row.region_id,
                "userId": str(row.user_id),
                "recipient": row.recipient_address_snapshot,
                "channel": row.channel_code,
                "message": _serialize_message(dict(row.message_snapshot)),
                "status": row.status,
                "attemptCount": row.attempt_count,
                "createdAt": row.created_at.astimezone(UTC).isoformat(),
                "startedAt": row.started_at.astimezone(UTC).isoformat() if row.started_at else None,
                "completedAt": row.completed_at.astimezone(UTC).isoformat() if row.completed_at else None,
                "availableAt": row.available_at.astimezone(UTC).isoformat() if row.available_at else None,
                "lastErrorCode": row.last_error_code,
                "lastErrorMessage": row.last_error_message,
            }
            for row in page.items
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/results")
async def campaign_results(
    campaign_id: UUID,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    get_campaign_results_usecase: Annotated[GetCampaignResultsUsecase, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    status: str | None = None,
    channel: str | None = None,
) -> Response[dict[str, object]]:
    applied_limit = _validate_limit(limit, config)
    try:
        page = await get_campaign_results_usecase.execute(
            GetCampaignResultsRequest(
                campaign_id=campaign_id,
                manager_id=manager.manager_id,
                limit=applied_limit,
                cursor=_to_cursor_point(cursor),
                status=status,
                channel=channel,
            )
        )
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

    next_cursor = None
    if page.next_cursor is not None:
        next_cursor = encode_cursor(page.next_cursor.timestamp, page.next_cursor.row_id)
    payload: dict[str, object] = {
        "items": [
            {
                "taskId": str(row.task_id),
                "regionId": row.region_id,
                "userId": str(row.user_id),
                "recipient": row.recipient_address_snapshot,
                "channel": row.channel_code,
                "message": _serialize_message(dict(row.message_snapshot)),
                "status": row.status,
                "attemptCount": row.attempt_count,
                "providerCode": row.provider_code,
                "providerRequestId": row.provider_request_id,
                "createdAt": row.created_at.astimezone(UTC).isoformat(),
                "startedAt": row.started_at.astimezone(UTC).isoformat() if row.started_at else None,
                "completedAt": row.completed_at.astimezone(UTC).isoformat() if row.completed_at else None,
            }
            for row in page.items
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@get("/campaigns/{campaign_id:uuid}/errors")
async def campaign_errors(
    campaign_id: UUID,
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
    get_campaign_errors_usecase: Annotated[GetCampaignErrorsUsecase, Dependency(skip_validation=True)],
    limit: int | None = None,
    cursor: str | None = None,
    channel: str | None = None,
    error_code: str | None = None,
) -> Response[dict[str, object]]:
    applied_limit = _validate_limit(limit, config)
    try:
        page = await get_campaign_errors_usecase.execute(
            GetCampaignErrorsRequest(
                campaign_id=campaign_id,
                manager_id=manager.manager_id,
                limit=applied_limit,
                cursor=_to_cursor_point(cursor),
                channel=channel,
                error_code=error_code,
            )
        )
    except CampaignUsecaseNotFoundError as exc:
        _handle_usecase_not_found_error(exc)

    next_cursor = None
    if page.next_cursor is not None:
        next_cursor = encode_cursor(page.next_cursor.timestamp, page.next_cursor.row_id)
    payload: dict[str, object] = {
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
            for row in page.items
        ],
        "nextCursor": next_cursor,
        "consistency": "eventual",
    }
    return Response(content=payload, status_code=HTTP_200_OK)
