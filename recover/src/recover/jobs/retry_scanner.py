from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.config import JobConfig

log = structlog.get_logger("recover.retry_scanner")


class RetryScannerJob:
    name = "retry_scanner"

    def __init__(
        self,
        engine: AsyncEngine,
        cfg: JobConfig,
        regions: list[str],
        grace_seconds: int,
    ) -> None:
        self.engine = engine
        self.cfg = cfg
        self.regions = regions
        self.grace_seconds = grace_seconds
        self.sem = asyncio.Semaphore(max(cfg.concurrency, 1))

    async def _one(self, t: repo.RetryReady) -> int:
        async with self.sem:
            try:
                inserted = await repo.enqueue_retry_repush(self.engine, t)
            except Exception as exc:
                metrics.retry_repush_errors_total.inc()
                log.exception("retry.repush.error", task_id=str(t.id), error=str(exc))
                return 0
        if inserted:
            metrics.retry_repush_total.inc()
            return 1
        metrics.retry_repush_dedup_total.inc()
        return 0

    async def tick(self) -> int:
        ready = await repo.fetch_retry_ready(
            self.engine, self.cfg.batch_size, self.regions, self.grace_seconds
        )
        if not ready:
            return 0
        results = await asyncio.gather(*(self._one(t) for t in ready))
        return sum(results)
