from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine

from recover import metrics, repo
from recover.config import JobConfig


class OutboxRecoveryJob:
    name = "outbox_recovery"

    def __init__(self, engine: AsyncEngine, cfg: JobConfig, regions: list[str]) -> None:
        self.engine = engine
        self.cfg = cfg
        self.regions = regions

    async def tick(self) -> int:
        n = await repo.reset_stuck_outbox(self.engine, self.cfg.batch_size, self.regions)
        if n > 0:
            metrics.outbox_unstuck_total.inc(n)
        return n
