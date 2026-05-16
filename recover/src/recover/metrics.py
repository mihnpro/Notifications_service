"""Prometheus metrics. Names mirror the Rust service verbatim — dashboards/alerts depend on it."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

# Per-job execution
job_runs_total = Counter(
    "recover_job_runs_total", "Recover job run count", ["job", "outcome"], registry=REGISTRY
)
job_rows_total = Counter(
    "recover_job_rows_total", "Rows processed by a recover job", ["job"], registry=REGISTRY
)
job_tick_seconds = Histogram(
    "recover_job_tick_seconds", "Per-tick duration", ["job"], registry=REGISTRY
)

# Outbox recovery
outbox_unstuck_total = Counter(
    "recover_outbox_unstuck_total", "Outbox rows unstuck from publishing", registry=REGISTRY
)

# Lease recovery
lease_retried_total = Counter(
    "recover_lease_retried_total", "Leases recovered to retry_scheduled", ["bucket"], registry=REGISTRY
)
lease_deadlettered_total = Counter(
    "recover_lease_deadlettered_total", "Leases dead-lettered (max_attempts)", registry=REGISTRY
)
lease_skipped_total = Counter(
    "recover_lease_skipped_total", "Leases skipped (race)", registry=REGISTRY
)
lease_errors_total = Counter(
    "recover_lease_errors_total", "Lease recovery errors", registry=REGISTRY
)

# Retry scanner
retry_repush_total = Counter(
    "recover_retry_repush_total", "Retry repushes emitted", registry=REGISTRY
)
retry_repush_dedup_total = Counter(
    "recover_retry_repush_dedup_total", "Retry repush dedupes", registry=REGISTRY
)
retry_repush_errors_total = Counter(
    "recover_retry_repush_errors_total", "Retry repush errors", registry=REGISTRY
)

# Campaign finalizer
campaigns_finalized_total = Counter(
    "recover_campaigns_finalized_total", "Campaigns finalized", registry=REGISTRY
)
campaigns_finalize_raced_total = Counter(
    "recover_campaigns_finalize_raced_total", "Campaign finalize races", registry=REGISTRY
)
campaigns_finalize_errors_total = Counter(
    "recover_campaigns_finalize_errors_total", "Campaign finalize errors", registry=REGISTRY
)

# Outbox failed scanner
outbox_failed_deadlettered_total = Counter(
    "recover_outbox_failed_deadlettered_total", "Failed outbox rows dead-lettered", registry=REGISTRY
)
outbox_failed_orphan_total = Counter(
    "recover_outbox_failed_orphan_total", "Failed outbox rows orphaned", registry=REGISTRY
)
outbox_failed_skipped_total = Counter(
    "recover_outbox_failed_skipped_total", "Failed outbox rows skipped (race)", registry=REGISTRY
)
outbox_failed_errors_total = Counter(
    "recover_outbox_failed_errors_total", "Outbox-failed scanner errors", registry=REGISTRY
)

# Backlog gauges
backlog_stuck_outbox = Gauge(
    "recover_backlog_stuck_outbox", "Stuck outbox rows backlog", registry=REGISTRY
)
backlog_expired_leases = Gauge(
    "recover_backlog_expired_leases", "Expired leases backlog", registry=REGISTRY
)
backlog_retry_ready = Gauge(
    "recover_backlog_retry_ready", "Retry-ready tasks backlog", registry=REGISTRY
)
backlog_completable_campaigns = Gauge(
    "recover_backlog_completable_campaigns", "Completable campaigns backlog", registry=REGISTRY
)
backlog_errors_total = Counter(
    "recover_backlog_errors_total", "Backlog snapshot errors", registry=REGISTRY
)


def render() -> bytes:
    return generate_latest(REGISTRY)
