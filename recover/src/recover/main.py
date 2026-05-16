from __future__ import annotations

import argparse
import asyncio
import sys

import structlog
import uvicorn

from recover import chaos, repo
from recover.api.app import build_app
from recover.api.deps import AppState
from recover.config import RecoverConfig
from recover.db import create_engine
from recover.jobs import (
    BacklogJob,
    CampaignFinalizerJob,
    LeaseRecoveryJob,
    OutboxFailedScannerJob,
    OutboxRecoveryJob,
    RetryScannerJob,
    run_job_loop,
)
from recover.logging_setup import configure_logging
from recover.rmq import connect as rmq_connect
from recover.shutdown import Shutdown
from recover.state import Registry


log = structlog.get_logger("recover.main")


def _load_cfg() -> RecoverConfig:
    return RecoverConfig()  # type: ignore[call-arg]


def _setup_logging(cfg: RecoverConfig) -> None:
    configure_logging(cfg.telemetry.log_level, cfg.telemetry.log_format)


async def _serve() -> int:
    cfg = _load_cfg()
    _setup_logging(cfg)
    log.info("recover.starting", instance_id=cfg.instance_id, regions=cfg.regions.ids)

    shutdown = Shutdown()
    shutdown.install()

    engine = create_engine(cfg.db)
    registry = Registry()
    state = AppState(cfg=cfg, engine=engine, registry=registry, rmq_ready=False)

    rmq_conn = None
    if cfg.rmq.enabled:
        try:
            rmq_conn = await rmq_connect(cfg.rmq)
            state.rmq_ready = True
            log.info("recover.rmq.connected")
        except Exception as exc:
            log.warning("recover.rmq.connect_failed", error=str(exc))

    regions = list(cfg.regions.ids)
    jobs = [
        OutboxRecoveryJob(engine, cfg.jobs.outbox_recovery, regions),
        LeaseRecoveryJob(engine, cfg.jobs.lease_recovery, regions, cfg.retry.buckets_seconds),
        RetryScannerJob(engine, cfg.jobs.retry_scanner, regions, cfg.retry.repush_grace_seconds),
        CampaignFinalizerJob(engine, cfg.jobs.campaign_finalizer),
        OutboxFailedScannerJob(engine, cfg.jobs.outbox_failed_scanner, regions),
    ]
    backlog = BacklogJob(engine, regions)

    app = build_app(state)
    config = uvicorn.Config(
        app,
        host=cfg.server.host,
        port=cfg.server.port,
        log_config=None,
        access_log=False,
        lifespan="off",
    )
    server = uvicorn.Server(config)

    async def _serve_http() -> None:
        try:
            await server.serve()
        finally:
            shutdown.request()

    async def _watch_shutdown() -> None:
        await shutdown.wait()
        server.should_exit = True

    tasks: list[asyncio.Task] = [
        asyncio.create_task(run_job_loop(job, shutdown, registry), name=f"job:{job.name}")
        for job in jobs
    ]
    tasks.append(asyncio.create_task(backlog.run(shutdown, registry), name="job:backlog"))
    tasks.append(asyncio.create_task(_serve_http(), name="http"))
    tasks.append(asyncio.create_task(_watch_shutdown(), name="watch_shutdown"))

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        if rmq_conn is not None:
            try:
                await rmq_conn.close()
            except Exception:
                pass
        await engine.dispose()
        log.info("recover.shutdown.complete")
    return 0


async def _once(job_name: str) -> int:
    cfg = _load_cfg()
    _setup_logging(cfg)
    engine = create_engine(cfg.db)
    regions = list(cfg.regions.ids)
    try:
        if job_name == "outbox_recovery":
            job: object = OutboxRecoveryJob(engine, cfg.jobs.outbox_recovery, regions)
        elif job_name == "lease_recovery":
            job = LeaseRecoveryJob(engine, cfg.jobs.lease_recovery, regions, cfg.retry.buckets_seconds)
        elif job_name == "retry_scanner":
            job = RetryScannerJob(engine, cfg.jobs.retry_scanner, regions, cfg.retry.repush_grace_seconds)
        elif job_name == "campaign_finalizer":
            job = CampaignFinalizerJob(engine, cfg.jobs.campaign_finalizer)
        elif job_name == "outbox_failed_scanner":
            job = OutboxFailedScannerJob(engine, cfg.jobs.outbox_failed_scanner, regions)
        elif job_name == "backlog":
            snap = await repo.backlog_snapshot(engine, regions)
            log.info("backlog", **snap.__dict__)
            return 0
        else:
            log.error("unknown_job", job=job_name)
            return 2
        rows = await job.tick()  # type: ignore[attr-defined]
        log.info("once.done", job=job_name, rows=rows)
        return 0
    finally:
        await engine.dispose()


async def _chaos(kind: str, count: int) -> int:
    cfg = _load_cfg()
    _setup_logging(cfg)
    engine = create_engine(cfg.db)
    try:
        if kind == "stuck-leases":
            n = await chaos.inject_stuck_leases(engine, count)
        elif kind == "stuck-outbox":
            n = await chaos.inject_stuck_outbox(engine, count)
        elif kind == "orphan-retry":
            n = await chaos.inject_orphan_retry(engine, count)
        else:
            log.error("unknown_chaos", kind=kind)
            return 2
        log.info("chaos.injected", kind=kind, rows=n)
        return 0
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(prog="recover")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("serve", help="Run all jobs + HTTP API")

    once = sub.add_parser("once", help="Run a single job tick and exit")
    once.add_argument("--job", required=True)

    ch = sub.add_parser("chaos", help="Inject chaos for local testing")
    ch.add_argument("kind", choices=["stuck-leases", "stuck-outbox", "orphan-retry"])
    ch.add_argument("--count", type=int, default=10)

    args = parser.parse_args()
    if args.cmd == "serve":
        rc = asyncio.run(_serve())
    elif args.cmd == "once":
        rc = asyncio.run(_once(args.job))
    elif args.cmd == "chaos":
        rc = asyncio.run(_chaos(args.kind, args.count))
    else:
        parser.print_help()
        rc = 2
    sys.exit(rc)


if __name__ == "__main__":
    main()
