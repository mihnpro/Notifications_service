use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::config::JobConfig;
use crate::jobs::{Job, JobOutcome, ServiceContext};
use crate::repo::{self, FailedOutboxOutcome};

/// Drains `outbox_events.status='failed'` (publisher exhausted retries or
/// hit an unroutable message) into `dlq_items`, dead-letters the underlying
/// task and removes the outbox row. Without this job those rows pile up
/// silently and the message is lost from operator surfaces.
pub struct OutboxFailedScanner {
    pub cfg: JobConfig,
}

#[async_trait]
impl Job for OutboxFailedScanner {
    fn name(&self) -> &'static str {
        "outbox_failed_scanner"
    }

    fn config(&self) -> &JobConfig {
        &self.cfg
    }

    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome> {
        let rows =
            repo::fetch_failed_outbox(&ctx.pool, self.cfg.batch_size as i32, ctx.regions())
                .await?;
        if rows.is_empty() {
            return Ok(JobOutcome::Idle);
        }

        let dead_lettered = Arc::new(AtomicU64::new(0));
        let task_missing = Arc::new(AtomicU64::new(0));
        let skipped = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let pool = ctx.pool.clone();
        let concurrency = self.cfg.concurrency.max(1);

        stream::iter(rows)
            .for_each_concurrent(concurrency, |row| {
                let pool = pool.clone();
                let dead_lettered = dead_lettered.clone();
                let task_missing = task_missing.clone();
                let skipped = skipped.clone();
                let errors = errors.clone();
                async move {
                    match repo::dead_letter_failed_outbox(&pool, &row).await {
                        Ok(FailedOutboxOutcome::DeadLettered) => {
                            dead_lettered.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                outbox_id = %row.id,
                                dedupe_key = %row.dedupe_key,
                                "outbox failed → dlq_items + task dead_lettered"
                            );
                            metrics::counter!("recover_outbox_failed_deadlettered_total")
                                .increment(1);
                        }
                        Ok(FailedOutboxOutcome::TaskMissing) => {
                            task_missing.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                outbox_id = %row.id,
                                dedupe_key = %row.dedupe_key,
                                "outbox failed but task not found, row purged"
                            );
                            metrics::counter!("recover_outbox_failed_orphan_total").increment(1);
                        }
                        Ok(FailedOutboxOutcome::Skipped) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            metrics::counter!("recover_outbox_failed_skipped_total").increment(1);
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(
                                outbox_id = %row.id,
                                error = %e,
                                "outbox failed scanner: row processing failed"
                            );
                            metrics::counter!("recover_outbox_failed_errors_total").increment(1);
                        }
                    }
                }
            })
            .await;

        let dead_lettered = dead_lettered.load(Ordering::Relaxed);
        let task_missing = task_missing.load(Ordering::Relaxed);
        let skipped = skipped.load(Ordering::Relaxed);
        let errors = errors.load(Ordering::Relaxed);
        let processed = dead_lettered + task_missing;

        tracing::debug!(
            dead_lettered,
            task_missing,
            skipped,
            errors,
            "outbox failed scanner batch"
        );

        if processed > 0 {
            Ok(JobOutcome::Processed(processed))
        } else {
            Ok(JobOutcome::Idle)
        }
    }
}
