from __future__ import annotations

import time
from typing import Protocol

import structlog

from recover import metrics
from recover.config import JobConfig
from recover.shutdown import Shutdown
from recover.state import Registry


log = structlog.get_logger("recover.job")


class JobLoop(Protocol):
    name: str
    cfg: JobConfig

    async def tick(self) -> int:
        """Run one batch. Return number of rows acted on."""
        ...


def _sleep_seconds(cfg: JobConfig, rows: int) -> float:
    if rows == 0:
        ms = cfg.idle_interval_ms
    elif rows >= cfg.batch_size:
        ms = cfg.busy_interval_ms
    else:
        ms = cfg.mid_interval_ms
    return max(ms, 1) / 1000.0


async def run_job_loop(job: JobLoop, shutdown: Shutdown, registry: Registry, leader: bool = True) -> None:
    """Drive a job's tick on idle/busy/mid cadence until shutdown."""
    if not job.cfg.enabled:
        log.info("job.disabled", job=job.name)
        return

    while not shutdown.is_set:
        started = time.monotonic()
        outcome = "ok"
        rows = 0
        try:
            rows = await job.tick()
        except Exception as exc:
            outcome = "error"
            log.exception("job.tick.error", job=job.name, error=str(exc))
        finally:
            dur = time.monotonic() - started
            metrics.job_tick_seconds.labels(job=job.name).observe(dur)
            metrics.job_runs_total.labels(job=job.name, outcome=outcome).inc()
            if rows > 0:
                metrics.job_rows_total.labels(job=job.name).inc(rows)
            registry.record(job.name, rows, outcome, leader)

        if await shutdown.sleep(_sleep_seconds(job.cfg, rows)):
            break

    log.info("job.stopped", job=job.name)
