from __future__ import annotations

from typing import Any
from uuid import UUID

from litestar import Litestar, MediaType, Request, Response, get, post
from litestar.di import Provide
from litestar.exceptions import HTTPException
from litestar.params import Parameter
from litestar.status_codes import (
    HTTP_202_ACCEPTED,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_410_GONE,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from recover import metrics, repo
from recover.api.deps import AppState
from recover.repo import DlqReplayOutcome, ForceRetryOutcome, RunRecoveryOutcome


async def _state(request: Request[Any, Any, Any]) -> AppState:
    return request.app.state.app_state  # type: ignore[no-any-return]


@get("/healthz", media_type=MediaType.TEXT, sync_to_thread=False)
def healthz() -> str:
    return "ok"


@get("/readyz", media_type=MediaType.TEXT)
async def readyz(app_state: AppState) -> Response[str]:
    from recover.db import ping

    db_ok = await ping(app_state.engine)
    if not db_ok:
        return Response("db_unavailable", media_type=MediaType.TEXT, status_code=HTTP_500_INTERNAL_SERVER_ERROR)
    if app_state.cfg.rmq.enabled and not app_state.rmq_ready:
        return Response("rmq_unavailable", media_type=MediaType.TEXT, status_code=HTTP_500_INTERNAL_SERVER_ERROR)
    return Response("ready", media_type=MediaType.TEXT)


@get("/metrics", media_type="text/plain; version=0.0.4", sync_to_thread=False)
def metrics_endpoint() -> bytes:
    return metrics.render()


@get("/recovery/dashboard")
async def dashboard(app_state: AppState) -> dict:
    return {
        "instance_id": app_state.cfg.instance_id,
        "regions": list(app_state.cfg.regions.ids),
        "jobs": [s.to_dict() for s in app_state.registry.snapshot()],
    }


@get("/v1/recovery/status")
async def recovery_status(app_state: AppState) -> dict:
    snap = await repo.backlog_snapshot(app_state.engine, list(app_state.cfg.regions.ids))
    return {
        "instance_id": app_state.cfg.instance_id,
        "backlog": {
            "stuck_outbox": snap.stuck_outbox,
            "expired_leases": snap.expired_leases,
            "retry_ready": snap.retry_ready,
            "completable_campaigns": snap.completable_campaigns,
        },
        "jobs": [s.to_dict() for s in app_state.registry.snapshot()],
    }


@post("/v1/tasks/{task_id:uuid}/retry", status_code=HTTP_202_ACCEPTED)
async def force_retry(task_id: UUID, app_state: AppState) -> dict:
    result = await repo.force_retry_task(
        app_state.engine, task_id, app_state.cfg.retry.force_retry_max_attempts_ceiling
    )
    if result.outcome is ForceRetryOutcome.SCHEDULED:
        return {"task_id": str(task_id), "outcome": "scheduled"}
    if result.outcome is ForceRetryOutcome.NOT_FOUND:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="task_not_found")
    raise HTTPException(
        status_code=HTTP_409_CONFLICT,
        detail=f"not_retryable:{result.current_status or 'unknown'}",
    )


@get("/v1/dlq")
async def list_dlq(
    app_state: AppState,
    status: str | None = Parameter(query="status", default=None),
    limit: int = Parameter(query="limit", default=50, ge=1, le=500),
) -> dict:
    items = await repo.list_dlq(app_state.engine, status, limit)
    return {"items": [i.to_dict() for i in items]}


@post("/v1/dlq/{dlq_id:uuid}/replay", status_code=HTTP_202_ACCEPTED)
async def replay_dlq(dlq_id: UUID, app_state: AppState) -> dict:
    result = await repo.replay_dlq(
        app_state.engine, dlq_id, app_state.cfg.retry.force_retry_max_attempts_ceiling
    )
    if result.outcome is DlqReplayOutcome.REPLAYED:
        return {"dlq_id": str(dlq_id), "outcome": "replayed"}
    if result.outcome is DlqReplayOutcome.NOT_FOUND:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="dlq_not_found")
    if result.outcome is DlqReplayOutcome.TASK_MISSING:
        raise HTTPException(status_code=HTTP_410_GONE, detail="task_missing")
    raise HTTPException(
        status_code=HTTP_409_CONFLICT, detail=f"not_open:{result.status or 'unknown'}"
    )


@post("/v1/runs/{run_id:uuid}/recover", status_code=HTTP_202_ACCEPTED)
async def recover_run(run_id: UUID, app_state: AppState) -> dict:
    result = await repo.recover_run(app_state.engine, run_id)
    if result.outcome is RunRecoveryOutcome.RESET:
        return {"run_id": str(run_id), "outcome": "reset"}
    if result.outcome is RunRecoveryOutcome.NOT_FOUND:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="run_not_found")
    raise HTTPException(
        status_code=HTTP_409_CONFLICT, detail=f"not_stuck:{result.status or 'unknown'}"
    )


def build_app(app_state: AppState) -> Litestar:
    app = Litestar(
        route_handlers=[
            healthz,
            readyz,
            metrics_endpoint,
            dashboard,
            recovery_status,
            force_retry,
            list_dlq,
            replay_dlq,
            recover_run,
        ],
        dependencies={"app_state": Provide(_state)},
        openapi_config=None,
    )
    app.state.app_state = app_state
    return app
