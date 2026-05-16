from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from recover.config import DbConfig


def create_engine(cfg: DbConfig) -> AsyncEngine:
    return create_async_engine(
        cfg.url,
        pool_size=cfg.max_connections,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_timeout=cfg.acquire_timeout_ms / 1000.0,
        future=True,
    )


async def ping(engine: AsyncEngine) -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
