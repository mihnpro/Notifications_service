from __future__ import annotations

from datetime import UTC
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID, uuid4

import sqlalchemy as sa
from litestar import Request, get, patch, post, put
from litestar.datastructures import State
from litestar.params import Dependency
from litestar.response import Response
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres_models.models import ChannelORM
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
from notifications_api.app.http.schemas import ApiModel
from notifications_api.infra.config import GlobalConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

DEFAULT_REGION = "default"
QUEUE_GROUPS = {"email", "sms", "push", "messenger"}
CHANNEL_STATES = {"enabled", "disabled", "degraded"}
DISABLE_POLICIES = {"retry_later", "fail_fast"}


# bobo vava
# у меня нет времени переписывать, сори что приходиться это видеться
class CreateChannelRequest(ApiModel):
    code: str
    display_name: str
    global_state: str = "enabled"
    adapter_name: str = "stub"
    queue_group: str
    provider_code: str | None = None
    rate_limits: dict[str, object] | None = None
    retry_policy: dict[str, object] | None = None
    disable_policy: str = "retry_later"


class PatchChannelRequest(ApiModel):
    display_name: str | None = None
    global_state: str | None = None
    adapter_name: str | None = None
    queue_group: str | None = None
    provider_code: str | None = None
    rate_limits: dict[str, object] | None = None
    retry_policy: dict[str, object] | None = None
    disable_policy: str | None = None


class UpsertRegionalConfigRequest(ApiModel):
    state: str
    adapter_version: str | None = "v1"
    provider_code: str | None = None
    config_ref: str | None = None
    rate_limits: dict[str, object] | None = None
    retry_policy: dict[str, object] | None = None
    disable_policy: str = "retry_later"


def _validate_channel_state(state: str) -> None:
    if state not in CHANNEL_STATES:
        raise_validation("Invalid channel state", {"allowed": sorted(CHANNEL_STATES)})


def _validate_queue_group(queue_group: str) -> None:
    if queue_group not in QUEUE_GROUPS:
        raise_validation("Invalid queue_group", {"allowed": sorted(QUEUE_GROUPS)})


def _validate_disable_policy(disable_policy: str) -> None:
    if disable_policy not in DISABLE_POLICIES:
        raise_validation("Invalid disable_policy", {"allowed": sorted(DISABLE_POLICIES)})


def _channel_to_payload(channel: ChannelORM) -> dict[str, object]:
    return {
        "id": str(channel.id),
        "code": channel.code,
        "displayName": channel.display_name,
        "globalState": channel.state,
        "adapterName": channel.adapter_name,
        "queueGroup": channel.queue_group,
        "providerCode": channel.provider_code,
        "rateLimits": channel.rate_limits,
        "retryPolicy": channel.retry_policy,
        "disablePolicy": channel.disable_policy,
        "createdAt": channel.created_at.astimezone(UTC).isoformat(),
    }


async def _get_channel(session: AsyncSession, channel_id: UUID) -> ChannelORM:
    channel = (await session.execute(sa.select(ChannelORM).where(ChannelORM.id == channel_id))).scalar_one_or_none()
    if channel is None:
        raise_not_found("Channel not found")
    return channel


async def _run_idempotent_mutation(
    *,
    request: Request[Any, Any, State],
    manager: ManagerIdentity,
    session: AsyncSession,
    config: GlobalConfig,
    request_payload: dict[str, object],
    on_mutation: Callable[[], Awaitable[dict[str, object]]],
    status_code: int,
) -> Response[dict[str, object]]:
    idempotency_key = extract_idempotency_key(request)
    scope = build_scope(request, manager)
    start_result = await start_idempotent_request(
        session=session,
        scope=scope,
        key=idempotency_key,
        request_hash=payload_hash(request_payload),
        ttl_seconds=idempotency_ttl(config),
    )
    await session.commit()
    if start_result.is_replay and start_result.replay is not None:
        return Response(content=start_result.replay.payload, status_code=start_result.replay.status_code)

    try:
        payload = await on_mutation()
        await complete_idempotent_request(
            session=session,
            scope=scope,
            key=idempotency_key,
            payload=payload,
            status_code=status_code,
        )
        await session.commit()
        return Response(content=payload, status_code=status_code)
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


@get("/channels")
async def list_channels(
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    _ = manager
    rows = (
        (await session.execute(sa.select(ChannelORM).order_by(ChannelORM.created_at.desc(), ChannelORM.id.desc())))
        .scalars()
        .all()
    )
    return Response(content={"items": [_channel_to_payload(row) for row in rows]}, status_code=HTTP_200_OK)


@post("/channels")
async def create_channel(
    data: CreateChannelRequest,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    _validate_queue_group(data.queue_group)
    _validate_channel_state(data.global_state)
    _validate_disable_policy(data.disable_policy)

    async def _mutation() -> dict[str, object]:
        exists = (await session.execute(sa.select(ChannelORM).where(ChannelORM.code == data.code))).scalar_one_or_none()
        if exists is not None:
            raise_validation("Channel code already exists", {"code": data.code})
        channel = ChannelORM(
            id=uuid4(),
            code=data.code,
            display_name=data.display_name,
            state=data.global_state,
            adapter_name=data.adapter_name,
            queue_group=data.queue_group,
            provider_code=data.provider_code,
            rate_limits=data.rate_limits or {"rps": 10, "maxConcurrency": 10},
            retry_policy=data.retry_policy or {"maxAttempts": 5, "baseDelaySeconds": 30, "maxDelaySeconds": 1800},
            disable_policy=data.disable_policy,
        )
        session.add(channel)
        await session.flush()
        await session.refresh(channel)
        return _channel_to_payload(channel)

    return await _run_idempotent_mutation(
        request=request,
        manager=manager,
        session=session,
        config=config,
        request_payload=data.model_dump(by_alias=True, mode="json", exclude_none=False),
        on_mutation=_mutation,
        status_code=HTTP_201_CREATED,
    )


@patch("/channels/{channel_id:uuid}")
async def patch_channel(
    channel_id: UUID,
    data: PatchChannelRequest,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    if data.queue_group is not None:
        _validate_queue_group(data.queue_group)
    if data.global_state is not None:
        _validate_channel_state(data.global_state)
    if data.disable_policy is not None:
        _validate_disable_policy(data.disable_policy)

    async def _mutation() -> dict[str, object]:
        channel = await _get_channel(session, channel_id)
        if data.display_name is not None:
            channel.display_name = data.display_name
        if data.global_state is not None:
            channel.state = data.global_state
        if data.adapter_name is not None:
            channel.adapter_name = data.adapter_name
        if data.queue_group is not None:
            channel.queue_group = data.queue_group
        if data.provider_code is not None:
            channel.provider_code = data.provider_code
        if data.rate_limits is not None:
            channel.rate_limits = data.rate_limits
        if data.retry_policy is not None:
            channel.retry_policy = data.retry_policy
        if data.disable_policy is not None:
            channel.disable_policy = data.disable_policy
        await session.flush()
        return _channel_to_payload(channel)

    payload: dict[str, object] = {
        "channelId": str(channel_id),
        **data.model_dump(by_alias=True, mode="json", exclude_none=True),
    }
    return await _run_idempotent_mutation(
        request=request,
        manager=manager,
        session=session,
        config=config,
        request_payload=payload,
        on_mutation=_mutation,
        status_code=HTTP_200_OK,
    )


@post("/channels/{channel_id:uuid}/enable")
async def enable_channel(
    channel_id: UUID,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    async def _mutation() -> dict[str, object]:
        channel = await _get_channel(session, channel_id)
        channel.state = "enabled"
        await session.flush()
        return _channel_to_payload(channel)

    return await _run_idempotent_mutation(
        request=request,
        manager=manager,
        session=session,
        config=config,
        request_payload={"channelId": str(channel_id), "action": "enable"},
        on_mutation=_mutation,
        status_code=HTTP_200_OK,
    )


@post("/channels/{channel_id:uuid}/disable")
async def disable_channel(
    channel_id: UUID,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    async def _mutation() -> dict[str, object]:
        channel = await _get_channel(session, channel_id)
        channel.state = "disabled"
        await session.flush()
        return _channel_to_payload(channel)

    return await _run_idempotent_mutation(
        request=request,
        manager=manager,
        session=session,
        config=config,
        request_payload={"channelId": str(channel_id), "action": "disable"},
        on_mutation=_mutation,
        status_code=HTTP_200_OK,
    )


@get("/channels/{channel_id:uuid}/regional-configs")
async def get_channel_regional_configs(
    channel_id: UUID,
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    _ = manager
    channel = await _get_channel(session, channel_id)
    payload: dict[str, object] = {
        "items": [
            {
                "regionId": DEFAULT_REGION,
                "state": channel.state,
                "adapterVersion": "v1",
                "providerCode": channel.provider_code,
                "configRef": None,
                "rateLimits": channel.rate_limits,
                "retryPolicy": channel.retry_policy,
                "disablePolicy": channel.disable_policy,
            }
        ]
    }
    return Response(content=payload, status_code=HTTP_200_OK)


@put("/channels/{channel_id:uuid}/regional-configs/{region_id:str}")
async def put_channel_regional_config(
    channel_id: UUID,
    region_id: str,
    data: UpsertRegionalConfigRequest,
    request: Request[Any, Any, State],
    session: Annotated[AsyncSession, Dependency(skip_validation=True)],
    manager: Annotated[ManagerIdentity, Dependency(skip_validation=True)],
    config: Annotated[GlobalConfig, Dependency(skip_validation=True)],
) -> Response[dict[str, object]]:
    if region_id != DEFAULT_REGION:
        raise_validation("Only regionId=default is supported in MVP")
    _validate_channel_state(data.state)
    _validate_disable_policy(data.disable_policy)

    async def _mutation() -> dict[str, object]:
        channel = await _get_channel(session, channel_id)
        channel.state = data.state
        channel.provider_code = data.provider_code
        if data.rate_limits is not None:
            channel.rate_limits = data.rate_limits
        if data.retry_policy is not None:
            channel.retry_policy = data.retry_policy
        channel.disable_policy = data.disable_policy
        await session.flush()
        return {
            "regionId": DEFAULT_REGION,
            "state": channel.state,
            "adapterVersion": data.adapter_version or "v1",
            "providerCode": channel.provider_code,
            "configRef": data.config_ref,
            "rateLimits": channel.rate_limits,
            "retryPolicy": channel.retry_policy,
            "disablePolicy": channel.disable_policy,
        }

    payload: dict[str, object] = {
        "channelId": str(channel_id),
        "regionId": region_id,
        **data.model_dump(by_alias=True, mode="json", exclude_none=False),
    }
    return await _run_idempotent_mutation(
        request=request,
        manager=manager,
        session=session,
        config=config,
        request_payload=payload,
        on_mutation=_mutation,
        status_code=HTTP_200_OK,
    )
