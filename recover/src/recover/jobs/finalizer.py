from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.config import JobConfig

log = structlog.get_logger("recover.finalizer")


class CampaignFinalizerJob:
    name = "campaign_finalizer"

    def __init__(self, engine: AsyncEngine, cfg: JobConfig) -> None:
        self.engine = engine
        self.cfg = cfg
        self.sem = asyncio.Semaphore(max(cfg.concurrency, 1))

    async def _one(self, c: repo.CompletableCampaign) -> int:
        async with self.sem:
            try:
                finalized = await repo.finalize_campaign(self.engine, c)
            except Exception as exc:
                metrics.campaigns_finalize_errors_total.inc()
                log.exception("finalizer.error", campaign_id=str(c.campaign_id), error=str(exc))
                return 0
        if finalized:
            metrics.campaigns_finalized_total.inc()
            return 1
        metrics.campaigns_finalize_raced_total.inc()
        return 0

    async def tick(self) -> int:
        items = await repo.fetch_completable_campaigns(self.engine, self.cfg.batch_size)
        if not items:
            return 0
        results = await asyncio.gather(*(self._one(c) for c in items))
        return sum(results)
