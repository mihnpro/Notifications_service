use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

use crate::config::DbConfig;

pub async fn connect(cfg: &DbConfig) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(cfg.max_connections)
        .acquire_timeout(Duration::from_millis(cfg.acquire_timeout_ms))
        .test_before_acquire(true)
        .connect(&cfg.url)
        .await
        .with_context(|| format!("connect postgres at {}", cfg.url))
}

pub async fn ping(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .is_ok()
}
