use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::config::JobConfig;
use crate::jobs::{Job, JobOutcome, ServiceContext};
use crate::repo;

pub struct RetryScanner {
    pub cfg: JobConfig,
}

#[async_trait]
impl Job for RetryScanner {
    fn name(&self) -> &'static str {
        "retry_scanner"
    }

    fn config(&self) -> &JobConfig {
        &self.cfg
    }

    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome> {
        let tasks = repo::fetch_retry_ready(
            &ctx.pool,
            self.cfg.batch_size as i32,
            ctx.regions(),
            ctx.cfg.retry.repush_grace_seconds,
        )
        .await?;

        if tasks.is_empty() {
            return Ok(JobOutcome::Idle);
        }

        let emitted = Arc::new(AtomicU64::new(0));
        let dedup = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let pool = ctx.pool.clone();
        let concurrency = self.cfg.concurrency.max(1);

        stream::iter(tasks)
            .for_each_concurrent(concurrency, |t| {
                let pool = pool.clone();
                let emitted = emitted.clone();
                let dedup = dedup.clone();
                let errors = errors.clone();
                async move {
                    match repo::enqueue_retry_repush(&pool, &t).await {
                        Ok(true) => {
                            emitted.fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                task = %t.id,
                                attempt = t.attempt_count,
                                "retry repush emitted to main queue"
                            );
                            metrics::counter!("recover_retry_repush_total").increment(1);
                        }
                        Ok(false) => {
                            dedup.fetch_add(1, Ordering::Relaxed);
                            metrics::counter!("recover_retry_repush_dedup_total").increment(1);
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(task = %t.id, error = %e, "retry repush failed");
                            metrics::counter!("recover_retry_repush_errors_total").increment(1);
                        }
                    }
                }
            })
            .await;

        let emitted = emitted.load(Ordering::Relaxed);
        let dedup = dedup.load(Ordering::Relaxed);
        let errors = errors.load(Ordering::Relaxed);
        tracing::debug!(emitted, dedup, errors, "retry scanner batch");
        if emitted > 0 {
            Ok(JobOutcome::Processed(emitted))
        } else {
            Ok(JobOutcome::Idle)
        }
    }
}
