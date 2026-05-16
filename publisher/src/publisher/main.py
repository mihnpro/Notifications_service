from __future__ import annotations

import asyncio
import logging
import signal

import structlog

from publisher.broker import Broker
from publisher.config import PublisherConfig
from publisher.db import create_engine
from publisher.relay import Relay
from publisher.repository import OutboxRepository
from publisher.topology import declare_topology


def _configure_logging(level: str) -> None:
    logging.basicConfig(format="%(message)s", level=level.upper())
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper(), logging.INFO)),
    )


async def _run() -> None:
    cfg = PublisherConfig()
    _configure_logging(cfg.log_level)
    log = structlog.get_logger("publisher")

    engine = create_engine(cfg.pg_dsn, cfg.pg_pool_min_size, cfg.pg_pool_max_size)
    broker = Broker(cfg.rabbitmq_url, cfg.publish_timeout_sec)
    await broker.connect()
    log.info("broker.connected", url=cfg.rabbitmq_url)
    await declare_topology(broker.connection)
    log.info("topology.declared")

    repo = OutboxRepository(engine, cfg.worker_id, cfg.lock_ttl_sec)
    relay = Relay(cfg, repo, broker)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, relay.request_stop)

    try:
        await relay.run()
    finally:
        log.info("shutdown.begin")
        await broker.close()
        await engine.dispose()
        log.info("shutdown.done")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
