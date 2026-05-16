from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.config import JobConfig
from recover.repo import LeaseRecoveryOutcome

log = structlog.get_logger("recover.lease")


class LeaseRecoveryJob:
    name = "lease_recovery"

    def __init__(
        self,
        engine: AsyncEngine,
        cfg: JobConfig,
        regions: list[str],
        buckets_seconds: list[int],
    ) -> None:
        self.engine = engine
        self.cfg = cfg
        self.regions = regions
        self.buckets = buckets_seconds
        self.sem = asyncio.Semaphore(max(cfg.concurrency, 1))

    async def _one(self, lease: repo.ExpiredLease) -> int:
        async with self.sem:
            try:
                result = await repo.recover_expired_lease(self.engine, lease, self.buckets)
            except Exception as exc:
                metrics.lease_errors_total.inc()
                log.exception("lease.recover.error", task_id=str(lease.id), error=str(exc))
                return 0

        if result.outcome is LeaseRecoveryOutcome.RETRIED:
            assert result.bucket is not None
            metrics.lease_retried_total.labels(bucket=result.bucket.label).inc()
            return 1
        if result.outcome is LeaseRecoveryOutcome.DEAD_LETTERED:
            metrics.lease_deadlettered_total.inc()
            return 1
        metrics.lease_skipped_total.inc()
        return 0

    async def tick(self) -> int:
        leases = await repo.fetch_expired_leases(self.engine, self.cfg.batch_size, self.regions)
        if not leases:
            return 0
        results = await asyncio.gather(*(self._one(l) for l in leases))
        return sum(results)
