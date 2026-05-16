use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::config::JobConfig;
use crate::jobs::{Job, JobOutcome, ServiceContext};
use crate::repo::{self, LeaseRecoveryOutcome};

pub struct LeaseRecovery {
    pub cfg: JobConfig,
}

#[async_trait]
impl Job for LeaseRecovery {
    fn name(&self) -> &'static str {
        "lease_recovery"
    }

    fn config(&self) -> &JobConfig {
        &self.cfg
    }

    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome> {
        let leases =
            repo::fetch_expired_leases(&ctx.pool, self.cfg.batch_size as i32, ctx.regions())
                .await?;
        if leases.is_empty() {
            return Ok(JobOutcome::Idle);
        }

        let retried = Arc::new(AtomicU64::new(0));
        let dead_lettered = Arc::new(AtomicU64::new(0));
        let skipped = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        let buckets = ctx.cfg.retry.buckets_seconds.clone();
        let pool = ctx.pool.clone();
        let concurrency = self.cfg.concurrency.max(1);

        stream::iter(leases)
            .for_each_concurrent(concurrency, |lease| {
                let pool = pool.clone();
                let buckets = buckets.clone();
                let retried = retried.clone();
                let dead_lettered = dead_lettered.clone();
                let skipped = skipped.clone();
                let errors = errors.clone();
                async move {
                    match repo::recover_expired_lease(&pool, &lease, &buckets).await {
                        Ok(LeaseRecoveryOutcome::Retried { bucket }) => {
                            retried.fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                task = %lease.id,
                                attempt = lease.attempt_count,
                                bucket = bucket.label,
                                "lease recovered → retry_scheduled"
                            );
                            metrics::counter!(
                                "recover_lease_retried_total",
                                "bucket" => bucket.label
                            )
                            .increment(1);
                        }
                        Ok(LeaseRecoveryOutcome::DeadLettered) => {
                            dead_lettered.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                task = %lease.id,
                                attempt = lease.attempt_count,
                                "lease recovery → dead_lettered (max_attempts exhausted)"
                            );
                            metrics::counter!("recover_lease_deadlettered_total").increment(1);
                        }
                        Ok(LeaseRecoveryOutcome::Skipped) => {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            metrics::counter!("recover_lease_skipped_total").increment(1);
                        }
                        Err(e) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tracing::error!(task = %lease.id, error = %e, "lease recovery failed");
                            metrics::counter!("recover_lease_errors_total").increment(1);
                        }
                    }
                }
            })
            .await;

        let retried = retried.load(Ordering::Relaxed);
        let dead_lettered = dead_lettered.load(Ordering::Relaxed);
        let skipped = skipped.load(Ordering::Relaxed);
        let errors = errors.load(Ordering::Relaxed);
        let total = retried + dead_lettered;

        tracing::debug!(retried, dead_lettered, skipped, errors, "lease recovery batch");
        if total > 0 {
            Ok(JobOutcome::Processed(total))
        } else {
            Ok(JobOutcome::Idle)
        }
    }
}
