use anyhow::{Context, Result};
use sqlx::PgPool;

pub async fn inject_stuck_leases(pool: &PgPool, count: u32) -> Result<u64> {
    let res = sqlx::query(
        r#"
        WITH victims AS (
            SELECT id FROM delivery_tasks
            WHERE status IN ('queued','retry_scheduled')
              AND lease_until IS NULL
            ORDER BY random()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE delivery_tasks t
        SET status = 'sending',
            lease_owner = 'chaos-injector',
            lease_token = gen_random_uuid(),
            lease_until = NOW() - INTERVAL '1 minute',
            attempt_count = attempt_count + 1
        FROM victims
        WHERE t.id = victims.id
        "#,
    )
    .bind(count as i64)
    .execute(pool)
    .await
    .context("inject_stuck_leases")?;
    Ok(res.rows_affected())
}

pub async fn inject_stuck_outbox(pool: &PgPool, count: u32) -> Result<u64> {
    let res = sqlx::query(
        r#"
        WITH victims AS (
            SELECT id FROM outbox_events
            WHERE status = 'pending'
            ORDER BY random()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox_events o
        SET status = 'publishing',
            locked_by = 'chaos-injector',
            locked_until = NOW() - INTERVAL '5 minutes'
        FROM victims
        WHERE o.id = victims.id
        "#,
    )
    .bind(count as i64)
    .execute(pool)
    .await
    .context("inject_stuck_outbox")?;
    Ok(res.rows_affected())
}

pub async fn inject_orphan_retry(pool: &PgPool, count: u32) -> Result<u64> {
    let res = sqlx::query(
        r#"
        WITH victims AS (
            SELECT id FROM delivery_tasks
            WHERE status = 'queued'
            ORDER BY random()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE delivery_tasks t
        SET status = 'retry_scheduled',
            available_at = NOW() - INTERVAL '10 minutes'
        FROM victims
        WHERE t.id = victims.id
        "#,
    )
    .bind(count as i64)
    .execute(pool)
    .await
    .context("inject_orphan_retry")?;
    Ok(res.rows_affected())
}
