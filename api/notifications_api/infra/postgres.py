from collections.abc import AsyncGenerator
from typing import cast

from litestar.datastructures import State
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from notifications_api.infra.config import PostgresConfig


class AsyncSessionFactory(async_sessionmaker[AsyncSession]): ...


def create_async_engine_from_config(config: PostgresConfig) -> AsyncEngine:
    if config.pool_max_overflow is not None:
        return create_async_engine(
            config.url,
            pool_size=config.pool_size,
            pool_pre_ping=True,
            future=True,
            echo=config.should_log_sql or False,
            max_overflow=config.pool_max_overflow,
        )

    return create_async_engine(
        config.url,
        pool_size=config.pool_size,
        pool_pre_ping=True,
        future=True,
        echo=config.should_log_sql or False,
    )


def create_session_factory(engine: AsyncEngine) -> AsyncSessionFactory:
    return AsyncSessionFactory(engine, class_=AsyncSession, expire_on_commit=False)


def provide_engine(state: State) -> AsyncEngine:
    return cast("AsyncEngine", state.engine)


async def provide_session(state: State) -> AsyncGenerator[AsyncSession, None]:
    session_factory = cast("AsyncSessionFactory", state.session_factory)
    async with session_factory() as session:
        yield session
