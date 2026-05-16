from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.config import JobConfig
from recover.repo import FailedOutboxOutcome

log = structlog.get_logger("recover.outbox_failed")


class OutboxFailedScannerJob:
    name = "outbox_failed_scanner"

    def __init__(self, engine: AsyncEngine, cfg: JobConfig, regions: list[str]) -> None:
        self.engine = engine
        self.cfg = cfg
        self.regions = regions
        self.sem = asyncio.Semaphore(max(cfg.concurrency, 1))

    async def _one(self, row: repo.FailedOutbox) -> int:
        async with self.sem:
            try:
                outcome = await repo.dead_letter_failed_outbox(self.engine, row)
            except Exception as exc:
                metrics.outbox_failed_errors_total.inc()
                log.exception("outbox_failed.error", outbox_id=str(row.id), error=str(exc))
                return 0
        if outcome is FailedOutboxOutcome.DEAD_LETTERED:
            metrics.outbox_failed_deadlettered_total.inc()
            return 1
        if outcome is FailedOutboxOutcome.TASK_MISSING:
            metrics.outbox_failed_orphan_total.inc()
            return 1
        metrics.outbox_failed_skipped_total.inc()
        return 0

    async def tick(self) -> int:
        rows = await repo.fetch_failed_outbox(self.engine, self.cfg.batch_size, self.regions)
        if not rows:
            return 0
        results = await asyncio.gather(*(self._one(r) for r in rows))
        return sum(results)
