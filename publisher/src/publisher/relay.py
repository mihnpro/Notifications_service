from __future__ import annotations

import asyncio
import random

import structlog

from publisher.broker import Broker
from publisher.config import PublisherConfig
from publisher.domain.outbox import OutboxRow
from publisher.repository import OutboxRepository

log = structlog.get_logger(__name__)


def _backoff_seconds(attempt: int) -> float:
    base = min(2 ** attempt, 300)
    return base + random.uniform(0, base * 0.2)


class Relay:
    def __init__(self, cfg: PublisherConfig, repo: OutboxRepository, broker: Broker) -> None:
        self._cfg = cfg
        self._repo = repo
        self._broker = broker
        self._stop = asyncio.Event()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        log.info("relay.start", worker_id=self._cfg.worker_id, batch_size=self._cfg.batch_size)
        tick = 0
        while not self._stop.is_set():
            try:
                tick += 1
                if tick % self._cfg.recover_every_n_ticks == 0:
                    recovered = await self._repo.recover_stale()
                    if recovered:
                        log.info("relay.recovered_stale", count=recovered)

                rows = await self._repo.claim_batch(self._cfg.batch_size)
                if not rows:
                    await self._sleep(self._cfg.poll_interval_sec)
                    continue

                await self._process_batch(rows)
            except Exception:
                log.exception("relay.loop_error")
                await self._sleep(self._cfg.poll_interval_sec)
        log.info("relay.stopped")

    async def _process_batch(self, rows: list[OutboxRow]) -> None:
        published_ids = []
        for row in rows:
            try:
                await self._broker.publish(row)
                published_ids.append(row.id)
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"
                if row.attempt_count >= self._cfg.max_attempts:
                    log.error("relay.permanent_fail", id=str(row.id), attempts=row.attempt_count, error=err)
                    await self._repo.mark_failed(row.id, err)
                else:
                    backoff = _backoff_seconds(row.attempt_count)
                    log.warning("relay.retry", id=str(row.id), attempts=row.attempt_count, backoff=backoff, error=err)
                    await self._repo.mark_retry(row.id, err, backoff)

        if published_ids:
            await self._repo.delete_by_ids(published_ids)
            log.info("relay.batch_published", count=len(published_ids))

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass
