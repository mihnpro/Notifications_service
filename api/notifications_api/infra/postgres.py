from collections.abc import AsyncGenerator
from typing import cast

from litestar.datastructures import State
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class PostgresConfig(BaseModel):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    username: str = Field(default="postgres")
    password: str = Field(default="postgres")
    database: str = Field(default="notifications")

    should_log_sql: bool | None = Field(default=False)
    pool_size: int = Field(default=10)
    pool_max_overflow: int | None = Field(default=20)

    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


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
