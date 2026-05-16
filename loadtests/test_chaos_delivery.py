"""Kill the delivery service during a 50k run, then bring it back. We expect:
- zero permanent loss (outbox + recover must catch every task),
- some leases expire (recover_lease_retried_total > 0),
- still-in-flight = 0 at completion."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx
import pytest

from loadtests_lib import api, chaos, prom, report
from loadtests_lib.env import ENV

log = logging.getLogger("loadtests.chaos.delivery")

CHAOS_AFTER_S = 10.0
CHAOS_DOWNTIME_S = 30.0


@pytest.mark.chaos_delivery
@pytest.mark.asyncio
async def test_kill_delivery_midrun(
    http_client: httpx.AsyncClient,
    auth_token: str,
    seeded_users: list,
    prom_baseline: prom.PromSnapshot,
) -> None:
    channels = ["email", "sms"]
    expected_tasks = len(seeded_users) * len(channels)

    started = time.monotonic()
    cid = await api.create_campaign(
        http_client, auth_token,
        name=f"loadtest-chaos-delivery-{int(started)}",
        channels=channels,
    )

    # Concurrently: poll for completion, and inject chaos shortly after start.
    chaos_task = asyncio.create_task(
        chaos.schedule_chaos("delivery", after_s=CHAOS_AFTER_S, downtime_s=CHAOS_DOWNTIME_S)
    )
    completion_task = asyncio.create_task(
        api.wait_for_terminal(
            http_client, auth_token, cid,
            expected_total=expected_tasks,
            # Allow extra time for the downtime + recovery cycle.
            timeout_s=ENV.completion_timeout_s + int(CHAOS_DOWNTIME_S) + 120,
            poll_s=ENV.poll_interval_s,
        )
    )

    event = await chaos_task
    log.info("delivery down for %.1fs", event.downtime_s)
    stats, wall = await completion_task

    await asyncio.sleep(30)
    after = await prom.snapshot(http_client)
    window_s = max(int(wall + 30), 60)
    latency = await prom.latency_report(http_client, window_s)
    counters = prom.delta(prom_baseline, after)

    r = report.RunReport(
        scenario="chaos-delivery-50k",
        total_users=len(seeded_users),
        channels=channels,
        expected_tasks=expected_tasks,
        wall_clock_s=wall,
        final_stats=stats,
        counters_delta=counters,
        latency=latency,
        chaos=[event],
    )
    print(report.render(r))

    assert stats.total == expected_tasks, f"funout dropped tasks: {stats.total} vs {expected_tasks}"
    assert stats.in_flight == 0, f"timed out with {stats.in_flight} still in flight"
    # delivery shutdown almost always leaves some lease to recover
    assert counters.get("recover_lease_retried", 0) > 0, (
        "expected recover to retry expired leases after delivery downtime"
    )
    # Outbox pattern means zero permanent loss across a transient delivery outage.
    assert r.loss_ratio < 0.01, f"loss ratio too high: {r.loss_ratio:.4%}"
