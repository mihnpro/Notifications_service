use anyhow::Result;
use async_trait::async_trait;

use crate::config::JobConfig;
use crate::jobs::{Job, JobOutcome, ServiceContext};
use crate::repo;

pub struct CampaignFinalizer {
    pub cfg: JobConfig,
}

#[async_trait]
impl Job for CampaignFinalizer {
    fn name(&self) -> &'static str {
        "campaign_finalizer"
    }

    fn config(&self) -> &JobConfig {
        &self.cfg
    }

    async fn tick(&self, ctx: &ServiceContext) -> Result<JobOutcome> {
        let campaigns =
            repo::fetch_completable_campaigns(&ctx.pool, self.cfg.batch_size as i32).await?;
        if campaigns.is_empty() {
            return Ok(JobOutcome::Idle);
        }

        let mut finalized: u64 = 0;
        for c in &campaigns {
            match repo::finalize_campaign(&ctx.pool, c).await {
                Ok(true) => {
                    finalized += 1;
                    tracing::info!(
                        campaign = %c.campaign_id,
                        total = c.total_tasks,
                        succeeded = c.succeeded,
                        failed = c.failed,
                        dead_lettered = c.dead_lettered,
                        cancelled = c.cancelled,
                        "campaign finalized"
                    );
                    metrics::counter!("recover_campaigns_finalized_total").increment(1);
                }
                Ok(false) => {
                    metrics::counter!("recover_campaigns_finalize_raced_total").increment(1);
                }
                Err(e) => {
                    tracing::error!(campaign = %c.campaign_id, error = %e, "finalize failed");
                    metrics::counter!("recover_campaigns_finalize_errors_total").increment(1);
                }
            }
        }
        if finalized > 0 {
            Ok(JobOutcome::Processed(finalized))
        } else {
            Ok(JobOutcome::Idle)
        }
    }
}
