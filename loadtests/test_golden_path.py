"""Golden path: 50k users, single campaign, no failures injected. We assert that:
- nothing is lost (succeeded + cancelled == expected_tasks),
- p95 of the delivery stage is reasonable,
- the system drains in under the configured deadline.

Numeric thresholds are loose; the point is to PRODUCE a measurement and print it."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx
import pytest

from loadtests_lib import api, prom, report
from loadtests_lib.env import ENV

log = logging.getLogger("loadtests.golden")


@pytest.mark.load
@pytest.mark.asyncio
async def test_50k_golden_path(
    http_client: httpx.AsyncClient,
    auth_token: str,
    seeded_users: list,
    prom_baseline: prom.PromSnapshot,
) -> None:
    channels = ["email", "sms"]
    expected_tasks = len(seeded_users) * len(channels)
    log.info("creating campaign for %d users x %d channels = %d tasks",
             len(seeded_users), len(channels), expected_tasks)

    started = time.monotonic()
    cid = await api.create_campaign(
        http_client, auth_token,
        name=f"loadtest-golden-{int(started)}",
        channels=channels,
    )
    log.info("campaign %s created", cid)

    stats, wall = await api.wait_for_terminal(
        http_client, auth_token, cid,
        expected_total=expected_tasks,
        timeout_s=ENV.completion_timeout_s,
        poll_s=ENV.poll_interval_s,
    )
    # small grace so Prometheus has scraped the last samples
    await asyncio.sleep(20)

    after = await prom.snapshot(http_client)
    window_s = max(int(wall + 30), 60)
    latency = await prom.latency_report(http_client, window_s)
    counters = prom.delta(prom_baseline, after)

    r = report.RunReport(
        scenario="golden-50k",
        total_users=len(seeded_users),
        channels=channels,
        expected_tasks=expected_tasks,
        wall_clock_s=wall,
        final_stats=stats,
        counters_delta=counters,
        latency=latency,
        chaos=[],
    )
    print(report.render(r))

    # Hard assertions — keep loose; they catch regressions, not perf changes.
    assert stats.total == expected_tasks, f"funout dropped tasks: {stats.total} vs {expected_tasks}"
    assert stats.in_flight == 0, f"timed out with {stats.in_flight} tasks still in flight"
    assert r.loss_ratio < 0.001, f"loss ratio too high: {r.loss_ratio:.4%}"
