from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from litestar import Request, post
from litestar.datastructures import State
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import ChannelORM, UserChannelORM, UserORM
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
from notifications_api.app.http.schemas import ApiModel
from notifications_api.infra.config import GlobalConfig

DEFAULT_REGION = "default"
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^\+?[0-9]{7,20}$")


class UserChannelInput(ApiModel):
    channel: str
    address: str
    status: str = "active"
    verified: bool = True


class UserBulkItem(ApiModel):
    external_id: str
    status: str = "active"
    channels: list[UserChannelInput]


class UsersBulkRequest(ApiModel):
    mode: str = "skip_duplicates"
    items: list[UserBulkItem]


class RecipientSelectorInput(ApiModel):
    type: str
    user_ids: list[UUID] | None = None
    external_ids: list[str] | None = None
    filter: dict[str, object] | None = None


class UsersEstimateRequest(ApiModel):
    region_id: str = DEFAULT_REGION
    channels: list[str]
    recipient_selector: RecipientSelectorInput


def _validate_address(channel: ChannelORM, address: str) -> bool:
    if channel.adapter_name == "email":
        return EMAIL_RE.match(address) is not None
    if channel.adapter_name == "sms":
        return PHONE_RE.match(address) is not None
    return True


@post("/users/estimate")
async def users_estimate(
    data: UsersEstimateRequest,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    _ = manager
    if data.region_id != DEFAULT_REGION:
        raise_validation("Only regionId=default is supported in MVP", {})
    if not data.channels:
        return Response(
            content={
                "regionId": DEFAULT_REGION,
                "estimatedUsers": 0,
                "estimatedTasks": 0,
                "channels": [],
                "selectorType": data.recipient_selector.type,
            },
            status_code=HTTP_200_OK,
        )

    selector_type = data.recipient_selector.type
    if selector_type not in {"all", "external_ids", "user_ids", "segment"}:
        raise_validation("Unsupported recipientSelector.type", {"type": selector_type})

    channel_rows = (
        await session.execute(
            sa.select(ChannelORM).where(
                ChannelORM.code.in_(data.channels),
                ChannelORM.state != "disabled",
            )
        )
    ).scalars().all()
    channel_by_code = {channel.code: channel for channel in channel_rows}
    missing_channels = sorted(set(data.channels) - set(channel_by_code))
    if missing_channels:
        raise_validation("Some channels are disabled or missing", {"channels": missing_channels})

    user_filters = [
        UserORM.region_id == DEFAULT_REGION,
        UserORM.status == "active",
    ]
    if selector_type == "external_ids":
        external_ids = [value.strip() for value in (data.recipient_selector.external_ids or []) if value.strip()]
        if not external_ids:
            return Response(
                content={
                    "regionId": DEFAULT_REGION,
                    "estimatedUsers": 0,
                    "estimatedTasks": 0,
                    "channels": sorted(channel_by_code),
                    "selectorType": selector_type,
                },
                status_code=HTTP_200_OK,
            )
        user_filters.append(UserORM.external_id.in_(external_ids))
    elif selector_type == "user_ids":
        user_ids = data.recipient_selector.user_ids or []
        if not user_ids:
            return Response(
                content={
                    "regionId": DEFAULT_REGION,
                    "estimatedUsers": 0,
                    "estimatedTasks": 0,
                    "channels": sorted(channel_by_code),
                    "selectorType": selector_type,
                },
                status_code=HTTP_200_OK,
            )
        user_filters.append(UserORM.id.in_(user_ids))

    channel_ids = [channel.id for channel in channel_rows]
    users_query = (
        sa.select(sa.func.count(sa.distinct(UserORM.id)))
        .select_from(UserORM)
        .join(UserChannelORM, UserChannelORM.user_id == UserORM.id)
        .where(
            *user_filters,
            UserChannelORM.channel_id.in_(channel_ids),
            UserChannelORM.status == "active",
        )
    )
    tasks_query = (
        sa.select(sa.func.count(UserChannelORM.id))
        .select_from(UserORM)
        .join(UserChannelORM, UserChannelORM.user_id == UserORM.id)
        .where(
            *user_filters,
            UserChannelORM.channel_id.in_(channel_ids),
            UserChannelORM.status == "active",
        )
    )

    estimated_users = int((await session.execute(users_query)).scalar_one() or 0)
    estimated_tasks = int((await session.execute(tasks_query)).scalar_one() or 0)
    return Response(
        content={
            "regionId": DEFAULT_REGION,
            "estimatedUsers": estimated_users,
            "estimatedTasks": estimated_tasks,
            "channels": sorted(channel_by_code),
            "selectorType": selector_type,
        },
        status_code=HTTP_200_OK,
    )


@post("/users/bulk")
async def users_bulk_import(
    data: UsersBulkRequest,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    if data.mode not in {"skip_duplicates", "upsert"}:
        raise_validation("mode must be skip_duplicates or upsert")
    if not data.items:
        raise_validation("items must not be empty")
    if len(data.items) > config.users_bulk_max_batch:
        raise_validation("Batch size exceeds limit", {"maxBatch": config.users_bulk_max_batch})

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
        channel_codes = sorted({channel.channel for item in data.items for channel in item.channels})
        db_channels = (
            (await session.execute(sa.select(ChannelORM).where(ChannelORM.code.in_(channel_codes)))).scalars().all()
        )
        channels_map = {channel.code: channel for channel in db_channels}
        missing = sorted(set(channel_codes) - set(channels_map))
        if missing:
            raise_validation("Unknown channels in payload", {"missingChannels": missing})

        inserted_users = 0
        updated_users = 0
        skipped_users = 0
        inserted_user_channels = 0
        updated_user_channels = 0
        skipped_user_channels = 0
        errors: list[dict[str, object]] = []

        for index, item in enumerate(data.items):
            if item.status not in {"active", "inactive", "blocked"}:
                errors.append({
                    "index": index,
                    "externalId": item.external_id,
                    "code": "INVALID_USER_STATUS",
                    "message": f"Unsupported user status: {item.status}",
                })
                continue

            user = (
                await session.execute(
                    sa.select(UserORM).where(
                        UserORM.region_id == DEFAULT_REGION,
                        UserORM.external_id == item.external_id,
                    )
                )
            ).scalar_one_or_none()

            if user is None:
                user = UserORM(id=uuid4(), region_id=DEFAULT_REGION, external_id=item.external_id, status=item.status)
                session.add(user)
                await session.flush()
                inserted_users += 1
            elif data.mode == "upsert":
                if user.status != item.status:
                    user.status = item.status
                    updated_users += 1
            else:
                skipped_users += 1

            for channel_payload in item.channels:
                if channel_payload.status not in {"active", "inactive"}:
                    errors.append({
                        "index": index,
                        "externalId": item.external_id,
                        "code": "INVALID_CHANNEL_STATUS",
                        "message": f"Unsupported user_channel status: {channel_payload.status}",
                        "channel": channel_payload.channel,
                    })
                    continue
                channel = channels_map[channel_payload.channel]
                if not _validate_address(channel, channel_payload.address):
                    errors.append({
                        "index": index,
                        "externalId": item.external_id,
                        "code": "INVALID_ADDRESS",
                        "message": "Address format does not match channel type",
                        "channel": channel_payload.channel,
                        "address": channel_payload.address,
                    })
                    continue
                existing_user_channel = (
                    await session.execute(
                        sa.select(UserChannelORM).where(
                            UserChannelORM.user_id == user.id,
                            UserChannelORM.channel_id == channel.id,
                            UserChannelORM.address == channel_payload.address,
                        )
                    )
                ).scalar_one_or_none()
                if existing_user_channel is None:
                    session.add(
                        UserChannelORM(
                            id=uuid4(),
                            region_id=DEFAULT_REGION,
                            user_id=user.id,
                            channel_id=channel.id,
                            address=channel_payload.address,
                            status=channel_payload.status,
                            verified=channel_payload.verified,
                        )
                    )
                    inserted_user_channels += 1
                    continue

                if data.mode == "upsert":
                    changed = False
                    if existing_user_channel.status != channel_payload.status:
                        existing_user_channel.status = channel_payload.status
                        changed = True
                    if existing_user_channel.verified != channel_payload.verified:
                        existing_user_channel.verified = channel_payload.verified
                        changed = True
                    if changed:
                        updated_user_channels += 1
                else:
                    skipped_user_channels += 1

        payload: dict[str, object] = {
            "mode": data.mode,
            "regionId": DEFAULT_REGION,
            "inserted": {"users": inserted_users, "userChannels": inserted_user_channels},
            "updated": {"users": updated_users, "userChannels": updated_user_channels},
            "skipped": {"users": skipped_users, "userChannels": skipped_user_channels},
            "errors": errors,
            "processedAt": datetime.now(tz=UTC).isoformat(),
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
