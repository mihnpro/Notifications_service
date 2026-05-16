use anyhow::Result;
use async_trait::async_trait;

use crate::config::JobConfig;
use crate::jobs::{Job, JobOutcome, ServiceContext};
use crate::repo;

pub struct OutboxRecovery {
    pub cfg: JobConfig,
}

#[async_trait]
impl Job for OutboxRecovery {
    fn name(&self) -> &'static str {
        "outbox_recovery"
    }

    fn config(&self) -> &JobConfig {
        &self.cfg
    }

    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome> {
        let rows = repo::reset_stuck_outbox(
            &ctx.pool,
            self.cfg.batch_size as i32,
            ctx.regions(),
        )
        .await?;
        if rows > 0 {
            tracing::info!(rows, "outbox events unstuck");
            metrics::counter!("recover_outbox_unstuck_total").increment(rows);
            Ok(JobOutcome::Processed(rows))
        } else {
            Ok(JobOutcome::Idle)
        }
    }
}
