from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from litestar import Litestar
from litestar.di import Provide

from notifications_api.app.http.health import health
from notifications_api.infra.config import GlobalConfig
from notifications_api.infra.postgres import (
    create_async_engine_from_config,
    create_session_factory,
    provide_engine,
    provide_session,
)

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
    route_handlers=[health],
    dependencies={
        "config": Provide(provide_config),
        "engine": Provide(provide_engine),
        "session": Provide(provide_session),
    },
    lifespan=[app_lifespan],
)
