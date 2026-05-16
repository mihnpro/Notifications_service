use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::api;
use crate::config::Config;
use crate::state::Registry;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub metrics: PrometheusHandle,
    pub registry: Registry,
    pub cfg: Arc<Config>,
}

pub async fn serve(bind: String, state: AppState, shutdown: CancellationToken) -> Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .route("/recovery/dashboard", get(dashboard))
        .merge(api::routes())
        .with_state(state);

    let addr: SocketAddr = bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "http server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;

    Ok(())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if crate::db::ping(&state.pool).await {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "db ping failed")
    }
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn dashboard(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({ "jobs": state.registry.snapshot() }))
}
