"""Pretty-print test results so a human can read the answer at a glance."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tabulate import tabulate

from loadtests_lib.api import CampaignStats
from loadtests_lib.chaos import ChaosEvent


@dataclass
class RunReport:
    scenario: str
    total_users: int
    channels: list[str]
    expected_tasks: int
    wall_clock_s: float
    final_stats: CampaignStats
    counters_delta: dict[str, float]
    latency: dict[str, float | None]
    chaos: list[ChaosEvent]

    @property
    def delivered(self) -> int:
        return self.final_stats.succeeded

    @property
    def lost(self) -> int:
        return (
            self.final_stats.dead_lettered
            + self.final_stats.failed
            + max(0, self.expected_tasks - self.final_stats.total)
        )

    @property
    def in_flight(self) -> int:
        return self.final_stats.in_flight

    @property
    def loss_ratio(self) -> float:
        return self.lost / max(self.expected_tasks, 1)

    @property
    def delivery_rate_msg_per_s(self) -> float:
        return self.delivered / max(self.wall_clock_s, 0.001)


def _fmt_ms(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v * 1000:.1f} ms"


def render(report: RunReport) -> str:
    rows: list[list[Any]] = [
        ["scenario", report.scenario],
        ["channels", ",".join(report.channels)],
        ["users", report.total_users],
        ["expected tasks", report.expected_tasks],
        ["actual tasks in DB", report.final_stats.total],
        ["wall clock", f"{report.wall_clock_s:.1f} s"],
        ["throughput", f"{report.delivery_rate_msg_per_s:.0f} msg/s"],
        ["delivered (succeeded)", report.delivered],
        ["failed", report.final_stats.failed],
        ["dead_lettered", report.final_stats.dead_lettered],
        ["cancelled", report.final_stats.cancelled],
        ["still in-flight at deadline", report.in_flight],
        ["LOSS (failed+dead_lettered+missing)", report.lost],
        ["loss ratio", f"{report.loss_ratio * 100:.3f} %"],
    ]
    lat_rows = [[k, _fmt_ms(v)] for k, v in report.latency.items()]
    counter_rows = [[k, f"{int(v)}"] for k, v in sorted(report.counters_delta.items())]
    chaos_rows = (
        [[c.service, f"down {c.downtime_s:.1f}s @ t={c.down_at:.0f}"] for c in report.chaos]
        if report.chaos else [["—", "no chaos"]]
    )
    return (
        "\n=== RESULT ===\n"
        + tabulate(rows, tablefmt="github")
        + "\n\n--- Pipeline latencies (window covers the run) ---\n"
        + tabulate(lat_rows, headers=["metric", "value"], tablefmt="github")
        + "\n\n--- Counter deltas over the run ---\n"
        + tabulate(counter_rows, headers=["metric", "delta"], tablefmt="github")
        + "\n\n--- Chaos events ---\n"
        + tabulate(chaos_rows, headers=["service", "window"], tablefmt="github")
        + "\n"
    )
