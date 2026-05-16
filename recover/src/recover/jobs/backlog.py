from __future__ import annotations

import time

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.shutdown import Shutdown
from recover.state import Registry

log = structlog.get_logger("recover.backlog")

_TICK_INTERVAL_SECONDS = 10.0


class BacklogJob:
    name = "backlog"

    def __init__(self, engine: AsyncEngine, regions: list[str]) -> None:
        self.engine = engine
        self.regions = regions

    async def run(self, shutdown: Shutdown, registry: Registry, leader: bool = True) -> None:
        while not shutdown.is_set:
            started = time.monotonic()
            outcome = "ok"
            rows = 0
            try:
                snap = await repo.backlog_snapshot(self.engine, self.regions)
                metrics.backlog_stuck_outbox.set(snap.stuck_outbox)
                metrics.backlog_expired_leases.set(snap.expired_leases)
                metrics.backlog_retry_ready.set(snap.retry_ready)
                metrics.backlog_completable_campaigns.set(snap.completable_campaigns)
                rows = snap.stuck_outbox + snap.expired_leases + snap.retry_ready + snap.completable_campaigns
            except Exception as exc:
                outcome = "error"
                metrics.backlog_errors_total.inc()
                log.exception("backlog.snapshot.error", error=str(exc))
            finally:
                dur = time.monotonic() - started
                metrics.job_tick_seconds.labels(job=self.name).observe(dur)
                metrics.job_runs_total.labels(job=self.name, outcome=outcome).inc()
                registry.record(self.name, rows, outcome, leader)

            if await shutdown.sleep(_TICK_INTERVAL_SECONDS):
                break
        log.info("job.stopped", job=self.name)
