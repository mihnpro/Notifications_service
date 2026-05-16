from __future__ import annotations

import os
from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from testcontainers.postgres import PostgresContainer


DDL = """
CREATE TABLE IF NOT EXISTS campaigns (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS campaign_stats (
    campaign_id UUID PRIMARY KEY,
    total_tasks INT NOT NULL DEFAULT 0,
    queued INT NOT NULL DEFAULT 0,
    sending INT NOT NULL DEFAULT 0,
    succeeded INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    retry_scheduled INT NOT NULL DEFAULT 0,
    dead_lettered INT NOT NULL DEFAULT 0,
    cancelled INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS delivery_tasks (
    id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL,
    region_id TEXT NOT NULL,
    queue_group TEXT NOT NULL,
    priority TEXT NOT NULL,
    channel_code TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    available_at TIMESTAMPTZ,
    lease_token TEXT,
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    region_id TEXT NOT NULL,
    status TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    exchange TEXT NOT NULL,
    routing_key TEXT NOT NULL,
    payload JSONB,
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    locked_by TEXT,
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (region_id, dedupe_key)
);

CREATE TABLE IF NOT EXISTS dlq_items (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    region_id TEXT,
    channel_code TEXT,
    reason_code TEXT NOT NULL,
    error_message TEXT,
    status TEXT NOT NULL,
    last_replayed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS dlq_items_open_task_idx
    ON dlq_items (task_id) WHERE status = 'open';
CREATE UNIQUE INDEX IF NOT EXISTS dlq_items_task_idx ON dlq_items (task_id);

CREATE TABLE IF NOT EXISTS campaign_region_runs (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    fanout_lock_until TIMESTAMPTZ
);
"""


@pytest_asyncio.fixture(scope="session")
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    with PostgresContainer("postgres:16-alpine") as pg:
        host = pg.get_container_host_ip()
        port = pg.get_exposed_port(5432)
        user = pg.username
        pw = pg.password
        db = pg.dbname
        dsn = f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{db}"
        engine = create_async_engine(dsn, pool_pre_ping=True)
        async with engine.begin() as conn:
            for stmt in DDL.split(";"):
                s = stmt.strip()
                if s:
                    await conn.execute(text(s))
        yield engine
        await engine.dispose()


@pytest_asyncio.fixture
async def db(pg_engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    async with pg_engine.begin() as conn:
        for tbl in (
            "outbox_events",
            "dlq_items",
            "delivery_attempts",
            "delivery_tasks",
            "campaign_stats",
            "campaigns",
            "campaign_region_runs",
        ):
            await conn.execute(text(f"TRUNCATE TABLE {tbl}"))
    yield pg_engine
