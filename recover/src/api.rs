use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::config::Config;
use crate::repo::{self, DlqReplayOutcome, ForceRetryOutcome, RunRecoveryOutcome};
use crate::server::AppState;

fn ceiling(cfg: &Arc<Config>) -> i32 {
    cfg.retry.force_retry_max_attempts_ceiling
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/tasks/:id/retry", post(force_retry_task))
        .route("/v1/dlq", get(list_dlq))
        .route("/v1/dlq/:id/replay", post(replay_dlq))
        .route("/v1/runs/:id/recover", post(recover_run))
        .route("/v1/recovery/status", get(recovery_status))
}

async fn force_retry_task(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    match repo::force_retry_task(&state.pool, id, ceiling(&state.cfg)).await {
        Ok(ForceRetryOutcome::Scheduled) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "task_id": id,
                "status": "retry_scheduled",
                "message": "task re-queued via outbox"
            })),
        )
            .into_response(),
        Ok(ForceRetryOutcome::NotRetryable { current_status }) => (
            StatusCode::CONFLICT,
            Json(json!({
                "task_id": id,
                "error": "not_retryable",
                "current_status": current_status,
                "allowed_from": ["failed", "dead_lettered", "cancelled", "retry_scheduled"]
            })),
        )
            .into_response(),
        Ok(ForceRetryOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "task_id": id, "error": "not_found" })),
        )
            .into_response(),
        Err(e) => internal_error("force_retry_task", e),
    }
}

async fn replay_dlq(State(state): State<AppState>, Path(id): Path<Uuid>) -> impl IntoResponse {
    match repo::replay_dlq(&state.pool, id, ceiling(&state.cfg)).await {
        Ok(DlqReplayOutcome::Replayed) => (
            StatusCode::ACCEPTED,
            Json(json!({ "dlq_id": id, "status": "replayed" })),
        )
            .into_response(),
        Ok(DlqReplayOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "dlq_id": id, "error": "not_found" })),
        )
            .into_response(),
        Ok(DlqReplayOutcome::TaskMissing) => (
            StatusCode::GONE,
            Json(json!({ "dlq_id": id, "error": "task_missing" })),
        )
            .into_response(),
        Ok(DlqReplayOutcome::NotOpen { status }) => (
            StatusCode::CONFLICT,
            Json(json!({ "dlq_id": id, "error": "not_open", "status": status })),
        )
            .into_response(),
        Err(e) => internal_error("replay_dlq", e),
    }
}

#[derive(Deserialize)]
pub struct ListDlqQuery {
    pub status: Option<String>,
    pub limit: Option<i32>,
}

async fn list_dlq(
    State(state): State<AppState>,
    Query(q): Query<ListDlqQuery>,
) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    match repo::list_dlq(&state.pool, q.status.as_deref(), limit).await {
        Ok(items) => (
            StatusCode::OK,
            Json(json!({ "items": items, "limit": limit })),
        )
            .into_response(),
        Err(e) => internal_error("list_dlq", e),
    }
}

async fn recover_run(State(state): State<AppState>, Path(id): Path<Uuid>) -> impl IntoResponse {
    match repo::recover_run(&state.pool, id).await {
        Ok(RunRecoveryOutcome::Reset) => (
            StatusCode::ACCEPTED,
            Json(json!({ "run_id": id, "status": "fanout_pending" })),
        )
            .into_response(),
        Ok(RunRecoveryOutcome::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "run_id": id, "error": "not_found" })),
        )
            .into_response(),
        Ok(RunRecoveryOutcome::NotStuck { status }) => (
            StatusCode::CONFLICT,
            Json(json!({ "run_id": id, "error": "not_stuck", "status": status })),
        )
            .into_response(),
        Err(e) => internal_error("recover_run", e),
    }
}

async fn recovery_status(State(state): State<AppState>) -> impl IntoResponse {
    let jobs = state.registry.snapshot();
    let db_ok = crate::db::ping(&state.pool).await;
    (
        StatusCode::OK,
        Json(json!({
            "db_ok": db_ok,
            "jobs": jobs
        })),
    )
}

fn internal_error(op: &'static str, e: anyhow::Error) -> axum::response::Response {
    tracing::error!(op, error = %e, "api error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "internal", "op": op })),
    )
        .into_response()
}
