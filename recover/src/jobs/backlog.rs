use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::jobs::ServiceContext;
use crate::repo;

const INTERVAL: Duration = Duration::from_secs(10);

pub async fn run(ctx: ServiceContext, shutdown: CancellationToken) {
    tracing::info!(interval_secs = INTERVAL.as_secs(), "backlog gauge started");
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                tracing::info!("backlog gauge stopping");
                return;
            }
            _ = tokio::time::sleep(INTERVAL) => {}
        }

        match repo::backlog_snapshot(&ctx.pool, ctx.regions()).await {
            Ok(s) => {
                metrics::gauge!("recover_backlog_stuck_outbox").set(s.stuck_outbox as f64);
                metrics::gauge!("recover_backlog_expired_leases")
                    .set(s.expired_leases as f64);
                metrics::gauge!("recover_backlog_retry_ready").set(s.retry_ready as f64);
                metrics::gauge!("recover_backlog_completable_campaigns")
                    .set(s.completable_campaigns as f64);
                if s.stuck_outbox + s.expired_leases + s.retry_ready > 0 {
                    tracing::debug!(
                        stuck_outbox = s.stuck_outbox,
                        expired_leases = s.expired_leases,
                        retry_ready = s.retry_ready,
                        completable_campaigns = s.completable_campaigns,
                        "backlog snapshot"
                    );
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "backlog snapshot failed");
                metrics::counter!("recover_backlog_errors_total").increment(1);
            }
        }
    }
}
