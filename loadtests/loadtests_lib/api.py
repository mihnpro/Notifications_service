"""Thin API client. Bootstraps the manager via DB, logs in, and exposes /campaigns ops."""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass

import asyncpg
import httpx

from loadtests_lib.env import ENV


def _hash_password(password: str) -> str:
    """PBKDF2 hash compatible with notifications_api.app.http.auth.hash_password."""
    import base64
    import hashlib
    import os
    iterations = 390_000
    salt = os.urandom(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=32)
    enc = lambda b: base64.b64encode(b).decode("ascii")
    return f"pbkdf2_sha256${iterations}${enc(salt)}${enc(derived)}"


async def bootstrap_manager(dsn: str, login: str, password: str) -> None:
    """Mirror api/scripts/seed_users.py::bootstrap_manager but pure async, no api deps."""
    conn = await asyncpg.connect(dsn.replace("postgres://", "postgresql://"))
    try:
        await conn.execute(
            """
            INSERT INTO managers (login, password_hash, status)
            VALUES ($1, $2, 'active')
            ON CONFLICT (login) DO UPDATE
              SET password_hash = EXCLUDED.password_hash, status='active'
            """,
            login, _hash_password(password),
        )
    finally:
        await conn.close()


async def login(client: httpx.AsyncClient, login_: str, password: str) -> str:
    r = await client.post(
        f"{ENV.api_url}/auth/login",
        json={"login": login_, "password": password},
    )
    r.raise_for_status()
    token = r.json()["accessToken"]
    return token


@dataclass
class CampaignStats:
    total: int
    queued: int
    sending: int
    succeeded: int
    failed: int
    retry_scheduled: int
    dead_lettered: int
    cancelled: int

    @property
    def terminal(self) -> int:
        return self.succeeded + self.failed + self.dead_lettered + self.cancelled

    @property
    def in_flight(self) -> int:
        return self.queued + self.sending + self.retry_scheduled


def _normalize_stats(payload: dict) -> CampaignStats:
    # camelCase from API; tolerate snake_case too.
    def g(*keys: str) -> int:
        for k in keys:
            if k in payload and payload[k] is not None:
                return int(payload[k])
        return 0

    return CampaignStats(
        total=g("totalTasks", "total_tasks", "total"),
        queued=g("queued"),
        sending=g("sending"),
        succeeded=g("succeeded"),
        failed=g("failed"),
        retry_scheduled=g("retryScheduled", "retry_scheduled"),
        dead_lettered=g("deadLettered", "dead_lettered"),
        cancelled=g("cancelled"),
    )


async def create_campaign(
    client: httpx.AsyncClient,
    token: str,
    *,
    name: str,
    channels: list[str],
    priority: str = "normal",
    region: str = "default",
) -> uuid.UUID:
    body = {
        "name": name,
        "regionIds": [region],
        "message": {"title": "load-test", "body": "hi"},
        "recipientSelector": {"type": "all"},
        "channels": channels,
        "priority": priority,
    }
    r = await client.post(
        f"{ENV.api_url}/campaigns",
        json=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
    )
    r.raise_for_status()
    return uuid.UUID(r.json()["campaignId"])


async def get_stats(client: httpx.AsyncClient, token: str, cid: uuid.UUID) -> CampaignStats:
    r = await client.get(
        f"{ENV.api_url}/campaigns/{cid}/stats",
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()
    return _normalize_stats(r.json())


async def wait_for_terminal(
    client: httpx.AsyncClient,
    token: str,
    cid: uuid.UUID,
    *,
    expected_total: int,
    timeout_s: int,
    poll_s: float,
) -> tuple[CampaignStats, float]:
    """Poll until every task is in a terminal state, OR until timeout.

    Returns (final_stats, wall_clock_s). Does NOT raise on timeout — caller decides.
    """
    started = time.monotonic()
    deadline = started + timeout_s
    stats = await get_stats(client, token, cid)
    while time.monotonic() < deadline:
        stats = await get_stats(client, token, cid)
        if stats.total >= expected_total and stats.in_flight == 0:
            break
        await asyncio.sleep(poll_s)
    return stats, time.monotonic() - started
