//! Integration tests for recover ↔ outbox contract over the multi-queue topology.
//!
//! Verifies that after our topology rework recover still emits the correct
//! outbox rows on:
//!   1. lease expiry → `notification.direct` + main routing key (bucket label in payload)
//!   2. retry_scanner repush → `notification.direct` + main routing key
//! and that both paths are idempotent under repeated ticks.

use std::time::Duration;

use chrono::Utc;
use serde_json::Value as JsonValue;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use recover::repo::{
    self, enqueue_retry_repush, fetch_expired_leases, fetch_retry_ready, recover_expired_lease,
    LeaseRecoveryOutcome,
};

const BUCKETS: [u64; 4] = [30, 60, 300, 900];
const REGION: &str = "default";
const QUEUE_GROUP: &str = "email";
const PRIORITY: &str = "normal";

async fn start_pg() -> (testcontainers::ContainerAsync<Postgres>, PgPool) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let dsn = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&dsn)
        .await
        .expect("connect pg pool");

    setup_schema(&pool).await;
    (container, pool)
}

/// Minimal subset of the real schema. We drop FK/CHECK constraints so we can
/// insert rows without seeding channels/users/campaigns parents — this test
/// targets repo.rs state machine, not referential integrity.
async fn setup_schema(pool: &PgPool) {
    let ddl = r#"
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE TABLE delivery_tasks (
        id uuid PRIMARY KEY,
        campaign_id uuid NOT NULL,
        campaign_region_run_id uuid NOT NULL DEFAULT gen_random_uuid(),
        region_id text NOT NULL DEFAULT 'default',
        user_id uuid NOT NULL DEFAULT gen_random_uuid(),
        user_channel_id uuid NOT NULL DEFAULT gen_random_uuid(),
        channel_id uuid NOT NULL DEFAULT gen_random_uuid(),
        channel_code text NOT NULL,
        queue_group text NOT NULL,
        recipient_address_snapshot text NOT NULL DEFAULT '',
        message_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
        idempotency_key text NOT NULL DEFAULT gen_random_uuid()::text,
        status text NOT NULL,
        priority text NOT NULL DEFAULT 'normal',
        attempt_count int NOT NULL DEFAULT 0,
        max_attempts int NOT NULL DEFAULT 5,
        available_at timestamptz NOT NULL DEFAULT NOW(),
        lease_owner text,
        lease_token uuid,
        lease_until timestamptz,
        provider_code text,
        provider_request_id text,
        last_error_code text,
        last_error_message text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        started_at timestamptz,
        completed_at timestamptz
    );

    CREATE TABLE delivery_attempts (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        task_id uuid NOT NULL,
        campaign_id uuid NOT NULL,
        attempt_no int NOT NULL DEFAULT 1,
        worker_id text,
        channel_code text NOT NULL DEFAULT '',
        provider_code text,
        status text NOT NULL,
        error_type text,
        error_code text,
        error_message text,
        provider_request_id text,
        started_at timestamptz NOT NULL DEFAULT NOW(),
        completed_at timestamptz
    );

    CREATE TABLE campaign_stats (
        campaign_id uuid PRIMARY KEY,
        total_tasks bigint NOT NULL DEFAULT 0,
        queued bigint NOT NULL DEFAULT 0,
        sending bigint NOT NULL DEFAULT 0,
        succeeded bigint NOT NULL DEFAULT 0,
        failed bigint NOT NULL DEFAULT 0,
        retry_scheduled bigint NOT NULL DEFAULT 0,
        dead_lettered bigint NOT NULL DEFAULT 0,
        cancelled bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT NOW()
    );

    CREATE TABLE outbox_events (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        region_id text NOT NULL DEFAULT 'default',
        event_type text NOT NULL,
        payload jsonb NOT NULL,
        exchange text NOT NULL DEFAULT 'notification.direct',
        routing_key text NOT NULL,
        status text NOT NULL DEFAULT 'pending',
        dedupe_key text NOT NULL,
        locked_by text,
        locked_until timestamptz,
        attempt_count int NOT NULL DEFAULT 0,
        next_attempt_at timestamptz NOT NULL DEFAULT NOW(),
        last_error text,
        created_at timestamptz NOT NULL DEFAULT NOW(),
        published_at timestamptz
    );
    CREATE UNIQUE INDEX uq_outbox_region_dedupe ON outbox_events (region_id, dedupe_key);

    CREATE TABLE dlq_items (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        task_id uuid NOT NULL,
        campaign_id uuid NOT NULL,
        region_id text NOT NULL DEFAULT 'default',
        channel_code text NOT NULL,
        reason_code text NOT NULL,
        error_code text,
        error_message text,
        status text NOT NULL DEFAULT 'open',
        created_at timestamptz NOT NULL DEFAULT NOW(),
        last_replayed_at timestamptz
    );
    CREATE UNIQUE INDEX uq_dlq_task ON dlq_items (task_id);
    "#;

    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("setup ddl failed for `{stmt}`: {e}"));
    }
}

struct SeededTask {
    id: Uuid,
    campaign_id: Uuid,
}

async fn seed_sending_task_with_expired_lease(pool: &PgPool, attempt_count: i32) -> SeededTask {
    let id = Uuid::new_v4();
    let campaign_id = Uuid::new_v4();

    sqlx::query(
        r#"
        INSERT INTO delivery_tasks
            (id, campaign_id, region_id, channel_code, queue_group, priority,
             status, attempt_count, max_attempts, available_at,
             lease_owner, lease_token, lease_until)
        VALUES ($1, $2, $3, 'email', $4, $5,
                'sending', $6, 5, NOW() - INTERVAL '5 minutes',
                'worker-zombie', gen_random_uuid(), NOW() - INTERVAL '1 minute')
        "#,
    )
    .bind(id)
    .bind(campaign_id)
    .bind(REGION)
    .bind(QUEUE_GROUP)
    .bind(PRIORITY)
    .bind(attempt_count)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO campaign_stats (campaign_id, sending) VALUES ($1, 1)",
    )
    .bind(campaign_id)
    .execute(pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO delivery_attempts (task_id, campaign_id, status) VALUES ($1, $2, 'started')",
    )
    .bind(id)
    .bind(campaign_id)
    .execute(pool)
    .await
    .unwrap();

    SeededTask { id, campaign_id }
}

async fn task_status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query("SELECT status FROM delivery_tasks WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<String, _>(0)
}

async fn outbox_for_task(pool: &PgPool, task_id: Uuid) -> Vec<(String, String, String, JsonValue)> {
    sqlx::query(
        r#"
        SELECT exchange, routing_key, dedupe_key, payload
        FROM outbox_events
        WHERE (payload->>'task_id') = $1::text
        ORDER BY created_at ASC
        "#,
    )
    .bind(task_id.to_string())
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.get::<String, _>(0),
            row.get::<String, _>(1),
            row.get::<String, _>(2),
            row.get::<JsonValue, _>(3),
        )
    })
    .collect()
}

// ---------- tests ----------

#[tokio::test]
async fn lease_expired_emits_retry_outbox_with_bucket_routing_key() {
    let (_pg, pool) = start_pg().await;
    let task = seed_sending_task_with_expired_lease(&pool, 1).await;

    let leases = fetch_expired_leases(&pool, 10, &[REGION.to_string()])
        .await
        .expect("fetch expired leases");
    assert_eq!(leases.len(), 1);

    let outcome = recover_expired_lease(&pool, &leases[0], &BUCKETS)
        .await
        .expect("recover lease");
    assert!(matches!(outcome, LeaseRecoveryOutcome::Retried { .. }));

    assert_eq!(task_status(&pool, task.id).await, "retry_scheduled");

    let outbox = outbox_for_task(&pool, task.id).await;
    assert_eq!(outbox.len(), 1, "expected single retry outbox row");
    let (exchange, routing_key, dedupe, payload) = &outbox[0];
    assert_eq!(exchange, "notification.direct");
    assert_eq!(routing_key, "notification.default.email.normal");
    assert_eq!(dedupe, &format!("lease_recovery_retry:{}:1", task.id));
    // bucket label carried in payload for observability (attempt_count=1 → 30s)
    assert_eq!(payload["retry_bucket"], "30s");

    // stats: sending--, retry_scheduled++
    let (sending, retry_scheduled): (i64, i64) = sqlx::query_as(
        "SELECT sending, retry_scheduled FROM campaign_stats WHERE campaign_id = $1",
    )
    .bind(task.campaign_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sending, 0);
    assert_eq!(retry_scheduled, 1);

    // delivery_attempts row marked stale
    let stale: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM delivery_attempts WHERE task_id = $1 AND status = 'stale'",
    )
    .bind(task.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stale, 1);
}

#[tokio::test]
async fn lease_recovery_is_idempotent_on_repeat_ticks() {
    let (_pg, pool) = start_pg().await;
    let task = seed_sending_task_with_expired_lease(&pool, 2).await;

    // First tick: should retry into bucket 1m (attempt_count=2 → index 1).
    let leases = fetch_expired_leases(&pool, 10, &[REGION.to_string()])
        .await
        .unwrap();
    recover_expired_lease(&pool, &leases[0], &BUCKETS)
        .await
        .unwrap();

    // Subsequent ticks must not find this task (it's now retry_scheduled)
    // and must not write another outbox row.
    let again = fetch_expired_leases(&pool, 10, &[REGION.to_string()])
        .await
        .unwrap();
    assert!(again.is_empty(), "lease must not re-appear after recovery");

    let outbox = outbox_for_task(&pool, task.id).await;
    assert_eq!(outbox.len(), 1, "no duplicate outbox row after re-tick");
    assert_eq!(outbox[0].0, "notification.direct");
    assert_eq!(outbox[0].1, "notification.default.email.normal");
    assert_eq!(
        outbox[0].3["retry_bucket"], "1m",
        "attempt_count=2 must map to 1m bucket"
    );
}

#[tokio::test]
async fn lease_dead_lettered_when_attempts_exhausted() {
    let (_pg, pool) = start_pg().await;
    let task = seed_sending_task_with_expired_lease(&pool, 5).await; // attempt_count == max_attempts

    let leases = fetch_expired_leases(&pool, 10, &[REGION.to_string()])
        .await
        .unwrap();
    let outcome = recover_expired_lease(&pool, &leases[0], &BUCKETS)
        .await
        .unwrap();
    assert!(matches!(outcome, LeaseRecoveryOutcome::DeadLettered));

    assert_eq!(task_status(&pool, task.id).await, "dead_lettered");

    // No retry outbox row, but a dlq_items row exists.
    let outbox = outbox_for_task(&pool, task.id).await;
    assert!(outbox.is_empty(), "no retry outbox on dead-letter");

    let dlq: (String, String) = sqlx::query_as(
        "SELECT status, reason_code FROM dlq_items WHERE task_id = $1",
    )
    .bind(task.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(dlq.0, "open");
    assert_eq!(dlq.1, "lease_expired_max_attempts");
}

#[tokio::test]
async fn retry_scanner_repushes_via_main_exchange_with_per_priority_routing() {
    let (_pg, pool) = start_pg().await;

    // Task already past the grace window in retry_scheduled.
    let id = Uuid::new_v4();
    let campaign_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO delivery_tasks
            (id, campaign_id, region_id, channel_code, queue_group, priority,
             status, attempt_count, max_attempts, available_at)
        VALUES ($1, $2, $3, 'email', $4, 'high',
                'retry_scheduled', 1, 5, NOW() - INTERVAL '5 minutes')
        "#,
    )
    .bind(id)
    .bind(campaign_id)
    .bind(REGION)
    .bind(QUEUE_GROUP)
    .execute(&pool)
    .await
    .unwrap();

    let ready = fetch_retry_ready(&pool, 10, &[REGION.to_string()], 30)
        .await
        .unwrap();
    assert_eq!(ready.len(), 1);

    let inserted = enqueue_retry_repush(&pool, &ready[0]).await.unwrap();
    assert!(inserted, "first repush must insert");

    let outbox = outbox_for_task(&pool, id).await;
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].0, "notification.direct");
    assert_eq!(outbox[0].1, "notification.default.email.high");
    assert_eq!(outbox[0].2, format!("retry_repush:{id}:1"));

    // Second call must not duplicate (dedupe by region_id, dedupe_key).
    let inserted_again = enqueue_retry_repush(&pool, &ready[0]).await.unwrap();
    assert!(!inserted_again, "duplicate repush must be a no-op");

    // And fetch_retry_ready must now skip it because a pending outbox row exists.
    let ready_again = fetch_retry_ready(&pool, 10, &[REGION.to_string()], 30)
        .await
        .unwrap();
    assert!(
        ready_again.is_empty(),
        "scanner must not surface tasks that already have a pending repush"
    );
}

#[tokio::test]
async fn outbox_failed_scanner_drains_into_dlq() {
    let (_pg, pool) = start_pg().await;

    let task_id = Uuid::new_v4();
    let campaign_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO delivery_tasks
            (id, campaign_id, region_id, channel_code, queue_group, priority,
             status, attempt_count, max_attempts)
        VALUES ($1, $2, $3, 'email', $4, $5, 'queued', 0, 5)
        "#,
    )
    .bind(task_id)
    .bind(campaign_id)
    .bind(REGION)
    .bind(QUEUE_GROUP)
    .bind(PRIORITY)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO campaign_stats (campaign_id, queued) VALUES ($1, 1)")
        .bind(campaign_id)
        .execute(&pool)
        .await
        .unwrap();

    let payload = serde_json::json!({
        "task_id": task_id.to_string(),
        "campaign_id": campaign_id.to_string(),
        "region_id": REGION,
        "queue_group": QUEUE_GROUP,
        "priority": PRIORITY,
        "channel_code": "email",
    });
    let outbox_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO outbox_events
            (id, region_id, status, dedupe_key, event_type, exchange, routing_key,
             payload, last_error)
        VALUES ($1, $2, 'failed', $3, 'DeliveryTaskCreated',
                'notification.direct', 'notification.default.broken.normal',
                $4, 'unroutable: routing_key=notification.default.broken.normal')
        "#,
    )
    .bind(outbox_id)
    .bind(REGION)
    .bind(format!("publish_failed:{task_id}"))
    .bind(&payload)
    .execute(&pool)
    .await
    .unwrap();

    let rows = repo::fetch_failed_outbox(&pool, 10, &[REGION.to_string()])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    let outcome = repo::dead_letter_failed_outbox(&pool, &rows[0])
        .await
        .unwrap();
    assert!(matches!(outcome, repo::FailedOutboxOutcome::DeadLettered));

    // outbox row purged
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE id = $1")
        .bind(outbox_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);

    // task → dead_lettered
    assert_eq!(task_status(&pool, task_id).await, "dead_lettered");

    // dlq row with unroutable reason code
    let (reason, error): (String, Option<String>) = sqlx::query_as(
        "SELECT reason_code, error_message FROM dlq_items WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(reason, "publish_unroutable");
    assert!(error.as_deref().unwrap().contains("unroutable"));

    // touch time for the inserted row should be recent
    let now_ts = Utc::now().timestamp();
    let created_ts: chrono::DateTime<Utc> =
        sqlx::query_scalar("SELECT created_at FROM dlq_items WHERE task_id = $1")
            .bind(task_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!((created_ts.timestamp() - now_ts).abs() < 60);
}
