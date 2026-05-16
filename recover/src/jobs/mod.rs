pub mod backlog;
pub mod finalizer;
pub mod lease;
pub mod outbox;
pub mod retry_scanner;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, JobConfig};
use crate::state::Registry;

#[derive(Clone)]
pub struct ServiceContext {
    pub pool: PgPool,
    pub cfg: Arc<Config>,
    pub registry: Registry,
}

impl ServiceContext {
    pub fn new(pool: PgPool, cfg: Config, registry: Registry) -> Self {
        Self {
            pool,
            cfg: Arc::new(cfg),
            registry,
        }
    }

    pub fn regions(&self) -> &[String] {
        &self.cfg.regions.ids
    }
}

pub enum JobOutcome {
    Idle,
    Processed(u64),
}

#[async_trait]
pub trait Job: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn config(&self) -> &JobConfig;
    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome>;
}

pub async fn run_job<J: Job>(job: J, ctx: ServiceContext, shutdown: CancellationToken) {
    let name = job.name();
    let cfg = job.config().clone();
    if !cfg.enabled {
        tracing::info!(job = name, "job disabled, skipping");
        return;
    }
    tracing::info!(
        job = name,
        batch = cfg.batch_size,
        idle_ms = cfg.idle_interval_ms,
        busy_ms = cfg.busy_interval_ms,
        "job started"
    );

    let mut next_delay = Duration::from_millis(cfg.idle_interval_ms);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!(job = name, "job stopping");
                return;
            }
            _ = tokio::time::sleep_until(Instant::now() + next_delay) => {}
        }

        let started = Instant::now();
        let outcome = job.tick(&ctx).await;
        let elapsed = started.elapsed();

        match outcome {
            Ok(JobOutcome::Processed(rows)) if rows > 0 => {
                ctx.registry.record(name, rows, "ok", true);
                metrics::counter!("recover_job_runs_total", "job" => name, "outcome" => "ok").increment(1);
                metrics::counter!("recover_job_rows_total", "job" => name).increment(rows);
                metrics::histogram!("recover_job_tick_seconds", "job" => name)
                    .record(elapsed.as_secs_f64());
                tracing::debug!(job = name, rows, ?elapsed, "tick processed");
                next_delay = if rows as u32 >= cfg.batch_size {
                    Duration::from_millis(cfg.busy_interval_ms)
                } else {
                    Duration::from_millis(cfg.mid_interval_ms)
                };
            }
            Ok(_) => {
                ctx.registry.record(name, 0, "idle", true);
                metrics::counter!("recover_job_runs_total", "job" => name, "outcome" => "idle").increment(1);
                metrics::histogram!("recover_job_tick_seconds", "job" => name)
                    .record(elapsed.as_secs_f64());
                next_delay = Duration::from_millis(cfg.idle_interval_ms);
            }
            Err(e) => {
                ctx.registry.record(name, 0, "error", true);
                metrics::counter!("recover_job_runs_total", "job" => name, "outcome" => "error").increment(1);
                tracing::error!(job = name, error = %e, "tick failed");
                next_delay = Duration::from_millis(cfg.idle_interval_ms);
            }
        }
    }
}
