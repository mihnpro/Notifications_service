"""Prometheus query helper. Computes p95s via histogram_quantile and reads counters
both as absolute values and as deltas over the test window."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from loadtests_lib.env import ENV


@dataclass
class PromSnapshot:
    """Captured at a single point in time. Used to compute deltas over a run."""
    taken_at: float
    counters: dict[str, float] = field(default_factory=dict)


async def query(client: httpx.AsyncClient, expr: str) -> float | None:
    """Return the first scalar value of an instant query, or None if no data."""
    r = await client.get(f"{ENV.prom_url}/api/v1/query", params={"query": expr})
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        return None
    result = data["data"]["result"]
    if not result:
        return None
    return float(result[0]["value"][1])


async def query_all(client: httpx.AsyncClient, expr: str) -> list[dict[str, Any]]:
    r = await client.get(f"{ENV.prom_url}/api/v1/query", params={"query": expr})
    r.raise_for_status()
    return r.json().get("data", {}).get("result", [])


COUNTER_EXPRS: dict[str, str] = {
    # API
    "api_requests": "sum(api_http_requests_total)",
    # Funout
    "fanout_runs_ack": 'sum(fanout_runs_total{outcome="ack"})',
    "fanout_tasks_inserted": "sum(fanout_tasks_inserted_total)",
    # Publisher (outbox -> RMQ)
    "publisher_published": "sum(publisher_relay_published_total)",
    "publisher_failed": "sum(publisher_relay_failed_total)",
    "publisher_retried": "sum(publisher_relay_retried_total)",
    "publisher_recovered_stale": "sum(publisher_relay_recovered_stale_total)",
    # Delivery
    "delivery_succeeded": 'sum(delivery_tasks_finalized_total{outcome="succeeded"})',
    "delivery_dead_lettered": 'sum(delivery_tasks_finalized_total{outcome="dead_lettered"})',
    "delivery_retry_scheduled": 'sum(delivery_tasks_finalized_total{outcome="retry_scheduled"})',
    "delivery_provider_success": 'sum(delivery_provider_calls_total{outcome="success"})',
    "delivery_provider_transient": 'sum(delivery_provider_calls_total{outcome="transient"})',
    "delivery_provider_permanent": 'sum(delivery_provider_calls_total{outcome="permanent"})',
    "delivery_provider_timeout": 'sum(delivery_provider_calls_total{outcome="timeout"})',
    # Recover
    "recover_lease_retried": "sum(recover_lease_retried_total)",
    "recover_lease_deadlettered": "sum(recover_lease_deadlettered_total)",
    "recover_outbox_unstuck": "sum(recover_outbox_unstuck_total)",
    "recover_retry_repush": "sum(recover_retry_repush_total)",
    "recover_campaigns_finalized": "sum(recover_campaigns_finalized_total)",
}


async def snapshot(client: httpx.AsyncClient) -> PromSnapshot:
    snap = PromSnapshot(taken_at=time.time())
    for name, expr in COUNTER_EXPRS.items():
        v = await query(client, expr)
        snap.counters[name] = v if v is not None else 0.0
    return snap


def delta(before: PromSnapshot, after: PromSnapshot) -> dict[str, float]:
    return {k: max(0.0, after.counters.get(k, 0.0) - before.counters.get(k, 0.0)) for k in COUNTER_EXPRS}


# `[Xs]` window must cover the whole test, hence parameterised.
def quantile_expr(metric: str, q: float, window_s: int, by: tuple[str, ...] = ()) -> str:
    by_clause = f"by (le, {', '.join(by)})" if by else "by (le)"
    return f"histogram_quantile({q}, sum(rate({metric}_bucket[{window_s}s])) {by_clause})"


async def p95(client: httpx.AsyncClient, metric: str, window_s: int) -> float | None:
    """p95 in seconds. Returns None if no samples."""
    return await query(client, quantile_expr(metric, 0.95, window_s))


async def latency_report(client: httpx.AsyncClient, window_s: int) -> dict[str, float | None]:
    """p50/p95/p99 for the four pipeline-stage histograms we care about."""
    out: dict[str, float | None] = {}
    for metric in (
        "api_http_request_duration_seconds",
        "fanout_run_duration_seconds",
        "delivery_task_duration_seconds",
        "delivery_provider_call_duration_seconds",
    ):
        for q in (0.5, 0.95, 0.99):
            out[f"{metric}_p{int(q * 100)}"] = await query(
                client, quantile_expr(metric, q, window_s)
            )
    return out
