"""Stop RabbitMQ during a 50k run. This is the harshest test of the outbox pattern:
publisher cannot drain while RMQ is down, so outbox_events backlog grows. When RMQ
returns, publisher drains the backlog and delivery resumes — we expect zero loss."""
from __future__ import annotations

import asyncio
import logging
import time

import httpx
import pytest

from loadtests_lib import api, chaos, prom, report
from loadtests_lib.env import ENV

log = logging.getLogger("loadtests.chaos.rabbitmq")

CHAOS_AFTER_S = 8.0
CHAOS_DOWNTIME_S = 45.0


@pytest.mark.chaos_rabbitmq
@pytest.mark.asyncio
async def test_kill_rabbitmq_midrun(
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
        name=f"loadtest-chaos-rmq-{int(started)}",
        channels=channels,
    )

    chaos_task = asyncio.create_task(
        chaos.schedule_chaos("rabbitmq", after_s=CHAOS_AFTER_S, downtime_s=CHAOS_DOWNTIME_S)
    )
    completion_task = asyncio.create_task(
        api.wait_for_terminal(
            http_client, auth_token, cid,
            expected_total=expected_tasks,
            timeout_s=ENV.completion_timeout_s + int(CHAOS_DOWNTIME_S) + 180,
            poll_s=ENV.poll_interval_s,
        )
    )

    event = await chaos_task
    log.info("rabbitmq down for %.1fs", event.downtime_s)
    stats, wall = await completion_task

    await asyncio.sleep(30)
    after = await prom.snapshot(http_client)
    window_s = max(int(wall + 30), 60)
    latency = await prom.latency_report(http_client, window_s)
    counters = prom.delta(prom_baseline, after)

    r = report.RunReport(
        scenario="chaos-rabbitmq-50k",
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
    # Outbox semantics: publisher must eventually drain backlog after RMQ recovers.
    assert r.loss_ratio < 0.01, f"loss ratio too high: {r.loss_ratio:.4%}"
