from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from litestar import Litestar
from litestar.di import Provide
from litestar.exceptions import HTTPException, NotFoundException, ValidationException

from notifications_api.app.http.auth import provide_manager
from notifications_api.app.http.campaigns import (
    campaign_errors,
    campaign_results,
    campaign_stats,
    campaign_tasks,
    cancel_campaign,
    create_campaign,
    get_campaign,
    list_campaigns,
)
from notifications_api.app.http.channels import (
    create_channel,
    disable_channel,
    enable_channel,
    get_channel_regional_configs,
    list_channels,
    patch_channel,
    put_channel_regional_config,
)
from notifications_api.app.http.dlq import list_dlq, replay_dlq
from notifications_api.app.http.errors import (
    ApiError,
    api_error_handler,
    generic_exception_handler,
    http_exception_handler,
    integrity_error_handler,
    not_found_exception_handler,
    validation_exception_handler,
)
from notifications_api.app.http.health import health, healthz, readyz
from notifications_api.app.http.users import users_bulk_import
from notifications_api.infra.config import GlobalConfig
from notifications_api.infra.postgres import (
    create_async_engine_from_config,
    create_session_factory,
    provide_engine,
    provide_session,
)
from sqlalchemy.exc import IntegrityError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


def provide_config() -> GlobalConfig:
    return GlobalConfig.load()


@asynccontextmanager
async def app_lifespan(app: Litestar) -> AsyncGenerator[None, None]:
    config = GlobalConfig.load()
    engine = create_async_engine_from_config(config.postgres)
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)
    try:
        yield
    finally:
        engine_to_close: AsyncEngine = app.state.engine
        await engine_to_close.dispose()


app = Litestar(
    route_handlers=[
        health,
        healthz,
        readyz,
        create_campaign,
        cancel_campaign,
        list_campaigns,
        get_campaign,
        campaign_stats,
        campaign_tasks,
        campaign_results,
        campaign_errors,
        list_channels,
        create_channel,
        patch_channel,
        enable_channel,
        disable_channel,
        get_channel_regional_configs,
        put_channel_regional_config,
        list_dlq,
        replay_dlq,
        users_bulk_import,
    ],
    dependencies={
        "config": Provide(provide_config, sync_to_thread=False),
        "engine": Provide(provide_engine, sync_to_thread=False),
        "session": Provide(provide_session),
        "manager": Provide(provide_manager),
    },
    lifespan=[app_lifespan],
    exception_handlers={
        ApiError: api_error_handler,
        ValidationException: validation_exception_handler,
        NotFoundException: not_found_exception_handler,
        HTTPException: http_exception_handler,
        IntegrityError: integrity_error_handler,
        Exception: generic_exception_handler,
    },
)
