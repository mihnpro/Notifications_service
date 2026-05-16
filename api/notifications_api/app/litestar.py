import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from litestar import Litestar
from litestar.di import Provide
from litestar.exceptions import HTTPException, NotFoundException, ValidationException
from litestar.openapi import OpenAPIConfig
from litestar.openapi.plugins import SwaggerRenderPlugin
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from notifications_api.adapters.postgres import PostgresCampaignRepository, PostgresOutboxPublisher
from notifications_api.app.cancel_processor import CampaignCancelProcessor
from notifications_api.app.http.auth import login, provide_manager
from notifications_api.app.http.metrics_handler import prometheus_metrics
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
from notifications_api.app.http.users import users_bulk_import, users_estimate
from notifications_api.app.middleware import PrometheusMiddleware
from notifications_api.infra.config import GlobalConfig
from notifications_api.infra.postgres import (
    create_async_engine_from_config,
    create_session_factory,
    provide_engine,
    provide_session,
)
from notifications_api.usecase.campaigns import (
    CancelCampaignUsecase,
    CreateCampaignUsecase,
    GetCampaignErrorsUsecase,
    GetCampaignResultsUsecase,
    GetCampaignStatsUsecase,
    GetCampaignTasksUsecase,
    GetCampaignUsecase,
    ListCampaignsUsecase,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


def provide_config() -> GlobalConfig:
    return GlobalConfig.load()


def provide_campaign_repository(session: AsyncSession) -> PostgresCampaignRepository:
    return PostgresCampaignRepository(session)


def provide_outbox_publisher(session: AsyncSession) -> PostgresOutboxPublisher:
    return PostgresOutboxPublisher(session)


def provide_create_campaign_usecase(
    session: AsyncSession,
    campaign_repository: PostgresCampaignRepository,
    outbox_publisher: PostgresOutboxPublisher,
) -> CreateCampaignUsecase:
    return CreateCampaignUsecase(
        session=session,
        campaign_repository=campaign_repository,
        outbox_publisher=outbox_publisher,
    )


def provide_cancel_campaign_usecase(
    session: AsyncSession,
    campaign_repository: PostgresCampaignRepository,
    outbox_publisher: PostgresOutboxPublisher,
) -> CancelCampaignUsecase:
    return CancelCampaignUsecase(
        session=session,
        campaign_repository=campaign_repository,
        outbox_publisher=outbox_publisher,
    )


def provide_get_campaign_usecase(campaign_repository: PostgresCampaignRepository) -> GetCampaignUsecase:
    return GetCampaignUsecase(campaign_repository=campaign_repository)


def provide_list_campaigns_usecase(campaign_repository: PostgresCampaignRepository) -> ListCampaignsUsecase:
    return ListCampaignsUsecase(campaign_repository=campaign_repository)


def provide_get_campaign_stats_usecase(
    campaign_repository: PostgresCampaignRepository,
    get_campaign_usecase: GetCampaignUsecase,
) -> GetCampaignStatsUsecase:
    return GetCampaignStatsUsecase(
        campaign_repository=campaign_repository,
        get_campaign_usecase=get_campaign_usecase,
    )


def provide_get_campaign_tasks_usecase(
    campaign_repository: PostgresCampaignRepository,
    get_campaign_usecase: GetCampaignUsecase,
) -> GetCampaignTasksUsecase:
    return GetCampaignTasksUsecase(
        campaign_repository=campaign_repository,
        get_campaign_usecase=get_campaign_usecase,
    )


def provide_get_campaign_results_usecase(
    campaign_repository: PostgresCampaignRepository,
    get_campaign_usecase: GetCampaignUsecase,
) -> GetCampaignResultsUsecase:
    return GetCampaignResultsUsecase(
        campaign_repository=campaign_repository,
        get_campaign_usecase=get_campaign_usecase,
    )


def provide_get_campaign_errors_usecase(
    campaign_repository: PostgresCampaignRepository,
    get_campaign_usecase: GetCampaignUsecase,
) -> GetCampaignErrorsUsecase:
    return GetCampaignErrorsUsecase(
        campaign_repository=campaign_repository,
        get_campaign_usecase=get_campaign_usecase,
    )


@asynccontextmanager
async def app_lifespan(app: Litestar) -> AsyncGenerator[None, None]:
    config = GlobalConfig.load()
    engine = create_async_engine_from_config(config.postgres)
    app.state.engine = engine
    app.state.session_factory = create_session_factory(engine)
    cancel_processor_stop = asyncio.Event()
    cancel_processor = CampaignCancelProcessor(session_factory=app.state.session_factory)
    cancel_processor_task = asyncio.create_task(cancel_processor.run(cancel_processor_stop))
    app.state.cancel_processor_stop = cancel_processor_stop
    app.state.cancel_processor_task = cancel_processor_task
    try:
        yield
    finally:
        app.state.cancel_processor_stop.set()
        await app.state.cancel_processor_task
        engine_to_close: AsyncEngine = app.state.engine
        await engine_to_close.dispose()


app = Litestar(
    middleware=[PrometheusMiddleware],
    openapi_config=OpenAPIConfig(
        title="Notifications API",
        version="1.0.0",
        path="/schema",
        render_plugins=[SwaggerRenderPlugin()],
    ),
    route_handlers=[
        health,
        healthz,
        readyz,
        prometheus_metrics,
        login,
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
        users_estimate,
        users_bulk_import,
    ],
    dependencies={
        "config": Provide(provide_config, sync_to_thread=False),
        "engine": Provide(provide_engine, sync_to_thread=False),
        "session": Provide(provide_session),
        "manager": Provide(provide_manager),
        "campaign_repository": Provide(provide_campaign_repository, sync_to_thread=False),
        "outbox_publisher": Provide(provide_outbox_publisher, sync_to_thread=False),
        "create_campaign_usecase": Provide(provide_create_campaign_usecase, sync_to_thread=False),
        "cancel_campaign_usecase": Provide(provide_cancel_campaign_usecase, sync_to_thread=False),
        "get_campaign_usecase": Provide(provide_get_campaign_usecase, sync_to_thread=False),
        "list_campaigns_usecase": Provide(provide_list_campaigns_usecase, sync_to_thread=False),
        "get_campaign_stats_usecase": Provide(provide_get_campaign_stats_usecase, sync_to_thread=False),
        "get_campaign_tasks_usecase": Provide(provide_get_campaign_tasks_usecase, sync_to_thread=False),
        "get_campaign_results_usecase": Provide(provide_get_campaign_results_usecase, sync_to_thread=False),
        "get_campaign_errors_usecase": Provide(provide_get_campaign_errors_usecase, sync_to_thread=False),
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
