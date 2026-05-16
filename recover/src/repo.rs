use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::retry_bucket::{self, Bucket};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ExpiredLease {
    pub id: Uuid,
    pub campaign_id: Uuid,
    pub region_id: String,
    pub queue_group: String,
    pub priority: String,
    pub channel_code: String,
    pub attempt_count: i32,
    pub max_attempts: i32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RetryReady {
    pub id: Uuid,
    pub campaign_id: Uuid,
    pub region_id: String,
    pub queue_group: String,
    pub priority: String,
    pub channel_code: String,
    pub attempt_count: i32,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CompletableCampaign {
    pub campaign_id: Uuid,
    pub total_tasks: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub dead_lettered: i64,
    pub cancelled: i64,
    pub current_status: String,
}

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct DlqItemRow {
    pub id: Uuid,
    pub task_id: Uuid,
    pub campaign_id: Uuid,
    pub reason_code: String,
    pub status: String,
    pub last_replayed_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct TaskRow {
    pub id: Uuid,
    pub campaign_id: Uuid,
    pub region_id: String,
    pub queue_group: String,
    pub priority: String,
    pub channel_code: String,
    pub status: String,
    pub attempt_count: i32,
    pub max_attempts: i32,
    pub available_at: Option<DateTime<Utc>>,
}

fn region_filter(regions: &[String]) -> Option<Vec<String>> {
    if regions.iter().any(|r| r == "*") {
        None
    } else {
        Some(regions.to_vec())
    }
}

#[derive(Debug, Clone)]
pub struct BacklogSnapshot {
    pub stuck_outbox: i64,
    pub expired_leases: i64,
    pub retry_ready: i64,
    pub completable_campaigns: i64,
}

pub async fn backlog_snapshot(pool: &PgPool, regions: &[String]) -> Result<BacklogSnapshot> {
    let region_arg = region_filter(regions);
    let row: (i64, i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT
            (SELECT COUNT(*) FROM outbox_events
              WHERE status = 'publishing'
                AND locked_until IS NOT NULL
                AND locked_until < NOW()
                AND ($1::text[] IS NULL OR region_id = ANY($1))) AS stuck_outbox,
            (SELECT COUNT(*) FROM delivery_tasks
              WHERE status = 'sending'
                AND lease_until IS NOT NULL
                AND lease_until < NOW()
                AND ($1::text[] IS NULL OR region_id = ANY($1))) AS expired_leases,
            (SELECT COUNT(*) FROM delivery_tasks
              WHERE status = 'retry_scheduled'
                AND available_at IS NOT NULL
                AND available_at < NOW() - INTERVAL '30 seconds'
                AND ($1::text[] IS NULL OR region_id = ANY($1))) AS retry_ready,
            (SELECT COUNT(*) FROM campaign_stats s
              JOIN campaigns c ON c.id = s.campaign_id
              WHERE s.total_tasks > 0
                AND (s.queued + s.sending + s.retry_scheduled) = 0
                AND c.status IN ('running','cancelling')) AS completable_campaigns
        "#,
    )
    .bind(region_arg.as_deref())
    .fetch_one(pool)
    .await
    .context("backlog_snapshot")?;
    Ok(BacklogSnapshot {
        stuck_outbox: row.0,
        expired_leases: row.1,
        retry_ready: row.2,
        completable_campaigns: row.3,
    })
}

pub async fn reset_stuck_outbox(pool: &PgPool, batch_size: i32, regions: &[String]) -> Result<u64> {
    let region_arg = region_filter(regions);
    let res = sqlx::query(
        r#"
        WITH stuck AS (
            SELECT id FROM outbox_events
            WHERE status = 'publishing'
              AND locked_until IS NOT NULL
              AND locked_until < NOW()
              AND ($2::text[] IS NULL OR region_id = ANY($2))
            ORDER BY locked_until ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox_events o
        SET status = 'pending',
            locked_by = NULL,
            locked_until = NULL,
            attempt_count = attempt_count + 1,
            next_attempt_at = NOW(),
            last_error = COALESCE(last_error, 'recovered_from_stuck_publishing')
        FROM stuck
        WHERE o.id = stuck.id
        "#,
    )
    .bind(batch_size)
    .bind(region_arg.as_deref())
    .execute(pool)
    .await
    .context("reset_stuck_outbox")?;
    Ok(res.rows_affected())
}

pub async fn fetch_expired_leases(
    pool: &PgPool,
    batch_size: i32,
    regions: &[String],
) -> Result<Vec<ExpiredLease>> {
    let region_arg = region_filter(regions);
    let rows = sqlx::query_as::<_, ExpiredLease>(
        r#"
        SELECT id, campaign_id, region_id, queue_group, priority, channel_code,
               attempt_count, max_attempts
        FROM delivery_tasks
        WHERE status = 'sending'
          AND lease_until IS NOT NULL
          AND lease_until < NOW()
          AND ($2::text[] IS NULL OR region_id = ANY($2))
        ORDER BY lease_until ASC
        LIMIT $1
        "#,
    )
    .bind(batch_size)
    .bind(region_arg.as_deref())
    .fetch_all(pool)
    .await
    .context("fetch_expired_leases")?;
    Ok(rows)
}

pub enum LeaseRecoveryOutcome {
    Retried { bucket: Bucket },
    DeadLettered,
    Skipped,
}

pub async fn recover_expired_lease(
    pool: &PgPool,
    lease: &ExpiredLease,
    buckets_seconds: &[u64],
) -> Result<LeaseRecoveryOutcome> {
    let dead_letter = lease.attempt_count >= lease.max_attempts;

    let mut tx = pool.begin().await.context("begin tx")?;

    if dead_letter {
        let updated = sqlx::query(
            r#"
            UPDATE delivery_tasks
            SET status = 'dead_lettered',
                lease_token = NULL,
                lease_until = NULL,
                lease_owner = NULL,
                completed_at = NOW()
            WHERE id = $1
              AND status = 'sending'
              AND lease_until < NOW()
            "#,
        )
        .bind(lease.id)
        .execute(&mut *tx)
        .await
        .context("dead-letter task")?;
        if updated.rows_affected() == 0 {
            tx.rollback().await.ok();
            return Ok(LeaseRecoveryOutcome::Skipped);
        }
        mark_attempt_stale(&mut tx, lease.id).await?;
        bump_stats(&mut tx, lease.campaign_id, "dead_lettered").await?;
        insert_dlq(
            &mut tx,
            lease.id,
            lease.campaign_id,
            &lease.region_id,
            &lease.channel_code,
            "lease_expired_max_attempts",
        )
        .await?;
        tx.commit().await.context("commit dead-letter")?;
        return Ok(LeaseRecoveryOutcome::DeadLettered);
    }

    let bucket = retry_bucket::for_attempt(lease.attempt_count, buckets_seconds);
    let available_at = Utc::now() + chrono::Duration::from_std(bucket.delay).unwrap_or_default();

    let updated = sqlx::query(
        r#"
        UPDATE delivery_tasks
        SET status = 'retry_scheduled',
            lease_token = NULL,
            lease_until = NULL,
            lease_owner = NULL,
            available_at = $2
        WHERE id = $1
          AND status = 'sending'
          AND lease_until < NOW()
        "#,
    )
    .bind(lease.id)
    .bind(available_at)
    .execute(&mut *tx)
    .await
    .context("transition to retry_scheduled")?;

    if updated.rows_affected() == 0 {
        tx.rollback().await.ok();
        return Ok(LeaseRecoveryOutcome::Skipped);
    }

    mark_attempt_stale(&mut tx, lease.id).await?;
    bump_stats(&mut tx, lease.campaign_id, "retry_scheduled").await?;

    let dedupe_key = format!(
        "lease_recovery_retry:{}:{}",
        lease.id, lease.attempt_count
    );
    let routing_key =
        retry_bucket::retry_routing_key(&lease.region_id, &lease.queue_group, bucket.label);
    let payload = json!({
        "task_id": lease.id,
        "campaign_id": lease.campaign_id,
        "region_id": lease.region_id,
        "queue_group": lease.queue_group,
        "priority": lease.priority,
        "channel_code": lease.channel_code,
        "attempt_count": lease.attempt_count,
        "reason": "lease_expired",
        "scheduled_at": Utc::now(),
        "available_at": available_at,
    });

    insert_outbox(
        &mut tx,
        &lease.region_id,
        "TaskRetryScheduled",
        retry_bucket::EXCHANGE_RETRY,
        &routing_key,
        &dedupe_key,
        &payload,
    )
    .await?;

    tx.commit().await.context("commit lease retry")?;
    Ok(LeaseRecoveryOutcome::Retried { bucket })
}

pub async fn fetch_retry_ready(
    pool: &PgPool,
    batch_size: i32,
    regions: &[String],
    grace_seconds: i64,
) -> Result<Vec<RetryReady>> {
    let region_arg = region_filter(regions);
    let rows = sqlx::query_as::<_, RetryReady>(
        r#"
        SELECT t.id, t.campaign_id, t.region_id, t.queue_group, t.priority,
               t.channel_code, t.attempt_count
        FROM delivery_tasks t
        WHERE t.status = 'retry_scheduled'
          AND t.available_at IS NOT NULL
          AND t.available_at < NOW() - make_interval(secs => $3)
          AND ($2::text[] IS NULL OR t.region_id = ANY($2))
          AND NOT EXISTS (
              SELECT 1 FROM outbox_events o
              WHERE (
                    o.dedupe_key = 'retry_repush:' || t.id::text || ':' || t.attempt_count::text
                 OR o.dedupe_key = 'lease_recovery_retry:' || t.id::text || ':' || t.attempt_count::text
              )
              AND o.status IN ('pending','publishing')
          )
        ORDER BY t.available_at ASC
        LIMIT $1
        "#,
    )
    .bind(batch_size)
    .bind(region_arg.as_deref())
    .bind(grace_seconds as f64)
    .fetch_all(pool)
    .await
    .context("fetch_retry_ready")?;
    Ok(rows)
}

pub async fn enqueue_retry_repush(pool: &PgPool, t: &RetryReady) -> Result<bool> {
    let routing_key =
        retry_bucket::main_routing_key(&t.region_id, &t.queue_group, &t.priority);
    let dedupe_key = format!("retry_repush:{}:{}", t.id, t.attempt_count);
    let payload = json!({
        "task_id": t.id,
        "campaign_id": t.campaign_id,
        "region_id": t.region_id,
        "queue_group": t.queue_group,
        "priority": t.priority,
        "channel_code": t.channel_code,
        "attempt_count": t.attempt_count,
        "reason": "retry_repush",
        "scheduled_at": Utc::now(),
    });
    let mut tx = pool.begin().await.context("begin tx")?;
    let inserted = insert_outbox(
        &mut tx,
        &t.region_id,
        "TaskRetryRepush",
        retry_bucket::EXCHANGE_DIRECT,
        &routing_key,
        &dedupe_key,
        &payload,
    )
    .await?;
    tx.commit().await.context("commit retry repush")?;
    Ok(inserted)
}

pub async fn fetch_completable_campaigns(
    pool: &PgPool,
    batch_size: i32,
) -> Result<Vec<CompletableCampaign>> {
    let rows = sqlx::query_as::<_, CompletableCampaign>(
        r#"
        SELECT s.campaign_id,
               s.total_tasks,
               s.succeeded,
               s.failed,
               s.dead_lettered,
               s.cancelled,
               c.status AS current_status
        FROM campaign_stats s
        JOIN campaigns c ON c.id = s.campaign_id
        WHERE s.total_tasks > 0
          AND (s.queued + s.sending + s.retry_scheduled) = 0
          AND c.status IN ('running','cancelling')
        ORDER BY c.created_at ASC NULLS LAST
        LIMIT $1
        "#,
    )
    .bind(batch_size)
    .fetch_all(pool)
    .await
    .context("fetch_completable_campaigns")?;
    Ok(rows)
}

pub async fn finalize_campaign(pool: &PgPool, c: &CompletableCampaign) -> Result<bool> {
    let new_status: &str = if c.current_status == "cancelling"
        || (c.cancelled == c.total_tasks && c.total_tasks > 0)
    {
        "cancelled"
    } else if c.failed == 0 && c.dead_lettered == 0 && c.cancelled == 0 {
        "completed"
    } else if c.succeeded == 0 {
        "failed"
    } else {
        "partially_failed"
    };

    let res = sqlx::query(
        r#"
        UPDATE campaigns
        SET status = $2,
            completed_at = NOW()
        WHERE id = $1
          AND status = $3
        "#,
    )
    .bind(c.campaign_id)
    .bind(new_status)
    .bind(&c.current_status)
    .execute(pool)
    .await
    .context("finalize_campaign")?;
    Ok(res.rows_affected() > 0)
}

pub async fn fetch_task(pool: &PgPool, id: Uuid) -> Result<Option<TaskRow>> {
    let row = sqlx::query_as::<_, TaskRow>(
        r#"
        SELECT id, campaign_id, region_id, queue_group, priority, channel_code,
               status, attempt_count, max_attempts, available_at
        FROM delivery_tasks
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .context("fetch_task")?;
    Ok(row)
}

pub enum ForceRetryOutcome {
    Scheduled,
    NotRetryable { current_status: String },
    NotFound,
}

pub async fn force_retry_task(
    pool: &PgPool,
    id: Uuid,
    ceiling: i32,
) -> Result<ForceRetryOutcome> {
    let task = match fetch_task(pool, id).await? {
        Some(t) => t,
        None => return Ok(ForceRetryOutcome::NotFound),
    };

    let retryable = matches!(
        task.status.as_str(),
        "failed" | "dead_lettered" | "cancelled" | "retry_scheduled"
    );
    if !retryable {
        return Ok(ForceRetryOutcome::NotRetryable {
            current_status: task.status,
        });
    }

    let mut tx = pool.begin().await.context("begin tx")?;

    let new_max = (task.attempt_count + 1).max(task.max_attempts).min(ceiling);
    let updated = sqlx::query(
        r#"
        UPDATE delivery_tasks
        SET status = 'retry_scheduled',
            available_at = NOW(),
            lease_token = NULL,
            lease_until = NULL,
            lease_owner = NULL,
            completed_at = NULL,
            max_attempts = $2
        WHERE id = $1
          AND status IN ('failed','dead_lettered','cancelled','retry_scheduled')
        "#,
    )
    .bind(task.id)
    .bind(new_max)
    .execute(&mut *tx)
    .await
    .context("force-retry update task")?;

    if updated.rows_affected() == 0 {
        tx.rollback().await.ok();
        return Ok(ForceRetryOutcome::NotRetryable {
            current_status: task.status,
        });
    }

    let prev_status = task.status.as_str();
    if matches!(prev_status, "failed" | "dead_lettered" | "cancelled") {
        sqlx::query(&format!(
            "UPDATE campaign_stats SET {col} = GREATEST({col} - 1, 0), retry_scheduled = retry_scheduled + 1, updated_at = NOW() WHERE campaign_id = $1",
            col = prev_status
        ))
        .bind(task.campaign_id)
        .execute(&mut *tx)
        .await
        .context("rebalance stats for force retry")?;
    }

    sqlx::query(
        r#"
        UPDATE dlq_items
        SET status = 'replayed',
            last_replayed_at = NOW()
        WHERE task_id = $1
          AND status = 'open'
        "#,
    )
    .bind(task.id)
    .execute(&mut *tx)
    .await
    .context("close dlq for force retry")?;

    let routing_key =
        retry_bucket::main_routing_key(&task.region_id, &task.queue_group, &task.priority);
    let dedupe_key = format!("manual_retry:{}:{}", task.id, Utc::now().timestamp_millis());
    let payload = json!({
        "task_id": task.id,
        "campaign_id": task.campaign_id,
        "region_id": task.region_id,
        "queue_group": task.queue_group,
        "priority": task.priority,
        "channel_code": task.channel_code,
        "attempt_count": task.attempt_count,
        "reason": "manual_retry",
        "scheduled_at": Utc::now(),
    });
    insert_outbox(
        &mut tx,
        &task.region_id,
        "TaskRetryScheduled",
        retry_bucket::EXCHANGE_DIRECT,
        &routing_key,
        &dedupe_key,
        &payload,
    )
    .await?;

    tx.commit().await.context("commit force retry")?;
    Ok(ForceRetryOutcome::Scheduled)
}

pub enum DlqReplayOutcome {
    Replayed,
    NotFound,
    NotOpen { status: String },
    TaskMissing,
}

pub async fn replay_dlq(pool: &PgPool, dlq_id: Uuid, ceiling: i32) -> Result<DlqReplayOutcome> {
    let dlq: Option<(Uuid, Uuid, String)> = sqlx::query_as(
        r#"SELECT id, task_id, status FROM dlq_items WHERE id = $1"#,
    )
    .bind(dlq_id)
    .fetch_optional(pool)
    .await
    .context("fetch dlq")?;

    let (dlq_id, task_id, status) = match dlq {
        Some(d) => d,
        None => return Ok(DlqReplayOutcome::NotFound),
    };
    if status != "open" {
        return Ok(DlqReplayOutcome::NotOpen { status });
    }

    let task = match fetch_task(pool, task_id).await? {
        Some(t) => t,
        None => return Ok(DlqReplayOutcome::TaskMissing),
    };

    let mut tx = pool.begin().await.context("begin tx")?;

    let new_max = (task.attempt_count + 1).max(task.max_attempts).min(ceiling);
    let updated = sqlx::query(
        r#"
        UPDATE delivery_tasks
        SET status = 'retry_scheduled',
            available_at = NOW(),
            lease_token = NULL,
            lease_until = NULL,
            lease_owner = NULL,
            completed_at = NULL,
            max_attempts = $2
        WHERE id = $1
        "#,
    )
    .bind(task.id)
    .bind(new_max)
    .execute(&mut *tx)
    .await
    .context("dlq replay update task")?;
    if updated.rows_affected() == 0 {
        tx.rollback().await.ok();
        return Ok(DlqReplayOutcome::TaskMissing);
    }

    let prev_status = task.status.as_str();
    if matches!(prev_status, "failed" | "dead_lettered" | "cancelled") {
        sqlx::query(&format!(
            "UPDATE campaign_stats SET {col} = GREATEST({col} - 1, 0), retry_scheduled = retry_scheduled + 1, updated_at = NOW() WHERE campaign_id = $1",
            col = prev_status
        ))
        .bind(task.campaign_id)
        .execute(&mut *tx)
        .await
        .context("rebalance stats for dlq replay")?;
    }

    sqlx::query(
        r#"
        UPDATE dlq_items
        SET status = 'replayed',
            last_replayed_at = NOW()
        WHERE id = $1
        "#,
    )
    .bind(dlq_id)
    .execute(&mut *tx)
    .await
    .context("update dlq status")?;

    let routing_key =
        retry_bucket::main_routing_key(&task.region_id, &task.queue_group, &task.priority);
    let dedupe_key = format!("dlq_replay:{}:{}", dlq_id, Utc::now().timestamp_millis());
    let payload = json!({
        "task_id": task.id,
        "campaign_id": task.campaign_id,
        "region_id": task.region_id,
        "queue_group": task.queue_group,
        "priority": task.priority,
        "channel_code": task.channel_code,
        "attempt_count": task.attempt_count,
        "reason": "dlq_replay",
        "dlq_id": dlq_id,
        "scheduled_at": Utc::now(),
    });
    insert_outbox(
        &mut tx,
        &task.region_id,
        "TaskRetryScheduled",
        retry_bucket::EXCHANGE_DIRECT,
        &routing_key,
        &dedupe_key,
        &payload,
    )
    .await?;

    tx.commit().await.context("commit dlq replay")?;
    Ok(DlqReplayOutcome::Replayed)
}

pub async fn list_dlq(
    pool: &PgPool,
    status: Option<&str>,
    limit: i32,
) -> Result<Vec<DlqItemRow>> {
    let rows = sqlx::query_as::<_, DlqItemRow>(
        r#"
        SELECT id, task_id, campaign_id, reason_code, status, last_replayed_at,
               created_at
        FROM dlq_items
        WHERE ($1::text IS NULL OR status = $1)
        ORDER BY created_at DESC NULLS LAST
        LIMIT $2
        "#,
    )
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("list_dlq")?;
    Ok(rows)
}

pub enum RunRecoveryOutcome {
    Reset,
    NotFound,
    NotStuck { status: String },
}

pub async fn recover_run(pool: &PgPool, run_id: Uuid) -> Result<RunRecoveryOutcome> {
    let run: Option<(Uuid, String)> = sqlx::query_as(
        r#"SELECT id, status FROM campaign_region_runs WHERE id = $1"#,
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await
    .context("fetch run")?;

    let (_id, status) = match run {
        Some(r) => r,
        None => return Ok(RunRecoveryOutcome::NotFound),
    };

    if status != "fanout_running" && status != "fanout_failed" {
        return Ok(RunRecoveryOutcome::NotStuck { status });
    }

    let updated = sqlx::query(
        r#"
        UPDATE campaign_region_runs
        SET status = 'fanout_pending',
            fanout_lock_until = NULL
        WHERE id = $1
          AND status IN ('fanout_running','fanout_failed')
        "#,
    )
    .bind(run_id)
    .execute(pool)
    .await
    .context("recover_run update")?;
    if updated.rows_affected() == 0 {
        return Ok(RunRecoveryOutcome::NotStuck { status });
    }
    Ok(RunRecoveryOutcome::Reset)
}

async fn mark_attempt_stale(tx: &mut Transaction<'_, Postgres>, task_id: Uuid) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE delivery_attempts
        SET status = 'stale',
            completed_at = NOW()
        WHERE task_id = $1
          AND status = 'started'
        "#,
    )
    .bind(task_id)
    .execute(&mut **tx)
    .await
    .context("mark_attempt_stale")?;
    Ok(())
}

async fn bump_stats(
    tx: &mut Transaction<'_, Postgres>,
    campaign_id: Uuid,
    new_status: &str,
) -> Result<()> {
    let sql = format!(
        "UPDATE campaign_stats
         SET sending = GREATEST(sending - 1, 0),
             {col} = {col} + 1,
             updated_at = NOW()
         WHERE campaign_id = $1",
        col = new_status
    );
    sqlx::query(&sql)
        .bind(campaign_id)
        .execute(&mut **tx)
        .await
        .context("bump_stats")?;
    Ok(())
}

async fn insert_dlq(
    tx: &mut Transaction<'_, Postgres>,
    task_id: Uuid,
    campaign_id: Uuid,
    region_id: &str,
    channel_code: &str,
    reason_code: &str,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO dlq_items (id, task_id, campaign_id, region_id, channel_code,
                               reason_code, status, created_at)
        VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, 'open', NOW())
        ON CONFLICT (task_id) WHERE status = 'open' DO NOTHING
        "#,
    )
    .bind(task_id)
    .bind(campaign_id)
    .bind(region_id)
    .bind(channel_code)
    .bind(reason_code)
    .execute(&mut **tx)
    .await
    .context("insert_dlq")?;
    Ok(())
}

async fn insert_outbox(
    tx: &mut Transaction<'_, Postgres>,
    region_id: &str,
    event_type: &str,
    exchange: &str,
    routing_key: &str,
    dedupe_key: &str,
    payload: &serde_json::Value,
) -> Result<bool> {
    let res = sqlx::query(
        r#"
        INSERT INTO outbox_events
            (id, region_id, status, dedupe_key, event_type, exchange,
             routing_key, payload, attempt_count, next_attempt_at, created_at)
        VALUES (gen_random_uuid(), $1, 'pending', $2, $3, $4, $5, $6, 0, NOW(), NOW())
        ON CONFLICT (region_id, dedupe_key) DO NOTHING
        "#,
    )
    .bind(region_id)
    .bind(dedupe_key)
    .bind(event_type)
    .bind(exchange)
    .bind(routing_key)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .context("insert_outbox")?;
    Ok(res.rows_affected() > 0)
}
