from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from loadtests_lib import api, prom, seed
from loadtests_lib.env import ENV

log = logging.getLogger("loadtests")


@pytest.fixture(scope="session")
def env():
    return ENV


@pytest_asyncio.fixture(scope="session")
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture(scope="session")
async def auth_token(http_client: httpx.AsyncClient) -> str:
    log.info("bootstrapping manager + logging in at %s", ENV.api_url)
    await api.bootstrap_manager(ENV.pg_dsn, ENV.manager_login, ENV.manager_password)
    # Some warm-up / wait — API may still be coming up under CI.
    last_exc: Exception | None = None
    for _ in range(20):
        try:
            return await api.login(http_client, ENV.manager_login, ENV.manager_password)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            await asyncio.sleep(1.0)
    raise RuntimeError(f"could not log in after 20 tries: {last_exc!r}")


@pytest_asyncio.fixture(scope="session")
async def seeded_users() -> list:
    log.info("seeding %d users via COPY", ENV.total_users)
    started = time.monotonic()
    ids = await seed.seed_users(ENV.pg_dsn, ENV.total_users, "lt-")
    log.info("seeded %d users in %.1fs", len(ids), time.monotonic() - started)
    return ids


@pytest_asyncio.fixture
async def prom_baseline(http_client: httpx.AsyncClient) -> prom.PromSnapshot:
    return await prom.snapshot(http_client)
