#!/usr/bin/env sh
# 07_recover.sh — Integration smoke tests for the Recovery service
#
# Tests all recovery jobs and manual API endpoints in conjunction with the
# rest of the stack (Postgres, Publisher, Delivery, API).
#
# What is tested:
#   TC-R01  GET /v1/recovery/status          — health gate: db_ok=true, jobs listed
#   TC-R02  outbox_recovery job (auto)       — stuck publishing outbox → lock cleared / published
#   TC-R03  lease_recovery → retry (auto)    — expired sending lease → delivery_attempt stale-marked + outbox row created
#   TC-R04  lease_recovery → dead_letter (auto) — max_attempts exhausted → dead_lettered + dlq entry
#   TC-R05  retry_scanner → repush (auto)    — past-due retry_scheduled → task picked up by delivery
#   TC-R06  campaign_finalizer (auto)        — all tasks terminal → campaign completed/failed
#   TC-R07  outbox_failed_scanner (auto)     — status=failed outbox → dlq + task dead_lettered
#   TC-R08  POST /v1/tasks/:id/retry         — force-retry dead_lettered task via API
#   TC-R09  GET /v1/dlq + POST /v1/dlq/:id/replay — list and replay a DLQ entry
#   TC-R10  POST /v1/runs/:id/recover        — unstick a stuck fanout run
#
# Requirements:
#   - All stack services running (docker compose up)
#   - RECOVER_URL (default: http://localhost:8081)
#   - postgres container accessible via docker exec
#
# Tunable env vars:
#   RECOVER_URL               default: http://localhost:8081
#   RECOVERY_JOB_WAIT_SEC     default: 20
#   POSTGRES_CONTAINER        default: notifications_postgres
#   POSTGRES_USER             default: notifications
#   POSTGRES_DB               default: notifications

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

RECOVER_URL="${RECOVER_URL:-http://localhost:8081}"
JOB_WAIT="${RECOVERY_JOB_WAIT_SEC:-20}"
PG_CONTAINER="${POSTGRES_CONTAINER:-notifications_postgres}"
PG_USER="${POSTGRES_USER:-notifications}"
PG_DB="${POSTGRES_DB:-notifications}"

log "=== Recovery service smoke ==="
log "RECOVER_URL=${RECOVER_URL}  JOB_WAIT=${JOB_WAIT}s"

# ── Helpers ───────────────────────────────────────────────────────────────────

# Execute SQL bypassing FK triggers (notifications is superuser).
# This lets us inject synthetic delivery_tasks / outbox_events rows
# without seeding the full parent tree (users, channels, campaigns, etc.).
pg_exec() {
    docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A \
        -c "SET session_replication_role = 'replica'; $1"
}

# Read-only queries don't need FK bypass.
pg_read() {
    docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -A -c "$1"
}

recover_get() {
    path="$1"
    LAST_BODY=$(mktemp)
    LAST_STATUS=$(curl -sS -o "$LAST_BODY" -w "%{http_code}" "${RECOVER_URL}${path}")
}

recover_post() {
    path="$1"
    body="${2:-}"
    LAST_BODY=$(mktemp)
    if [ -n "$body" ]; then
        LAST_STATUS=$(curl -sS -o "$LAST_BODY" -w "%{http_code}" \
            -X POST -H "Content-Type: application/json" \
            --data "$body" "${RECOVER_URL}${path}")
    else
        LAST_STATUS=$(curl -sS -o "$LAST_BODY" -w "%{http_code}" \
            -X POST "${RECOVER_URL}${path}")
    fi
}

# Wait for a SQL predicate to return an expected value.
# Usage: wait_for_sql <sql> <expected> <label> <timeout_sec>
wait_for_sql() {
    sql="$1"
    expected="$2"
    label="$3"
    timeout="$4"
    elapsed=0
    actual=""
    while [ "$elapsed" -lt "$timeout" ]; do
        actual=$(pg_read "$sql" | tr -d '[:space:]')
        if [ "$actual" = "$expected" ]; then
            log "  [ok] ${label}: ${actual}"
            return 0
        fi
        log "  waiting ${label}: got=${actual} want=${expected} elapsed=${elapsed}s"
        sleep 3
        elapsed=$((elapsed + 3))
    done
    fail "${label}: timed out after ${timeout}s (last value=${actual})"
}

uuid4() { python3 -c "import uuid; print(uuid.uuid4())"; }

# ── TC-R01: Health gate ───────────────────────────────────────────────────────
log "--- TC-R01: GET /v1/recovery/status ---"
recover_get "/v1/recovery/status"
assert_status 200 "TC-R01 GET /v1/recovery/status"

db_ok=$(python3 - "$LAST_BODY" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(str(d.get("db_ok","")).lower())
PY
)
if [ "$db_ok" != "true" ]; then
    fail "TC-R01: db_ok is not true (got: ${db_ok})"
fi
jobs_count=$(python3 - "$LAST_BODY" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(len(d.get("jobs", {})))
PY
)
if [ "$jobs_count" -lt 1 ]; then
    fail "TC-R01: no jobs reported in /v1/recovery/status"
fi
log "TC-R01 passed: db_ok=true, ${jobs_count} jobs reported"

# ── TC-R02: outbox_recovery — stuck publishing outbox reset ───────────────────
# Recovery resets the lock; Publisher then publishes and DELETES the row.
# We verify recovery acted by checking the zombie lock is cleared (row gone or lock removed).
log "--- TC-R02: outbox_recovery job (stuck outbox → zombie lock cleared) ---"
OBX_ID=$(uuid4)
pg_exec "
INSERT INTO outbox_events
    (id, region_id, status, dedupe_key, event_type, exchange, routing_key, payload,
     locked_by, locked_until, attempt_count, next_attempt_at)
VALUES
    ('${OBX_ID}', 'default', 'publishing', 'smoke-stuck-${OBX_ID}',
     'TestEvent', 'notification.direct', 'notification.default.email.normal',
     '{\"smoke\": true}',
     'zombie-worker-smoke', NOW() - INTERVAL '10 minutes', 3, NOW())
" > /dev/null

# Wait until the zombie lock is gone (recovery cleared it → publisher published → deleted row)
wait_for_sql \
    "SELECT COUNT(*) FROM outbox_events WHERE id='${OBX_ID}' AND locked_by='zombie-worker-smoke'" \
    "0" \
    "TC-R02 zombie lock cleared" \
    "$JOB_WAIT"

pg_exec "DELETE FROM outbox_events WHERE id='${OBX_ID}'" > /dev/null
log "TC-R02 passed"

# ── TC-R03: lease_recovery → retry_scheduled ─────────────────────────────────
# Proxy for recovery having run: delivery_attempt marked stale (done inside recover's tx).
# We bypass FK constraints since delivery_tasks has many required FKs.
log "--- TC-R03: lease_recovery → retry_scheduled (delivery_attempt stale proxy) ---"
CAMP3=$(uuid4); TASK3=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP3}', gen_random_uuid(), 'smoke-lease-retry-${CAMP3}', 'running', '{}', '{}', '{}')" > /dev/null

pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, sending) VALUES ('${CAMP3}', 1, 1)" > /dev/null

RUN3=$(uuid4)
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN3}', '${CAMP3}', 'default', 'fanout_completed')" > /dev/null

pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority,
     attempt_count, max_attempts, available_at,
     lease_owner, lease_token, lease_until)
VALUES
    ('${TASK3}', '${CAMP3}', '${RUN3}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK3}', 'sending', 'normal',
     1, 5, NOW() - INTERVAL '5 minutes',
     'zombie-worker', gen_random_uuid(), NOW() - INTERVAL '2 minutes')
" > /dev/null

pg_exec "INSERT INTO delivery_attempts (task_id, campaign_id, attempt_no, channel_code, status) VALUES ('${TASK3}', '${CAMP3}', 1, 'email', 'started')" > /dev/null

# Proxy: recovery's mark_attempt_stale is called atomically before outbox creation
wait_for_sql \
    "SELECT COUNT(*) FROM delivery_attempts WHERE task_id='${TASK3}' AND status='stale'" \
    "1" \
    "TC-R03 delivery_attempt marked stale" \
    "$JOB_WAIT"

# Also verify the task left 'sending' (recovery transitioned it)
task3_status=$(pg_read "SELECT status FROM delivery_tasks WHERE id='${TASK3}'" | tr -d '[:space:]')
if [ "$task3_status" = "sending" ]; then
    fail "TC-R03: task is still in 'sending' after recovery should have acted"
fi
log "  task transitioned from sending → ${task3_status}"

# Cleanup
pg_exec "DELETE FROM outbox_events WHERE payload->>'task_id'='${TASK3}'" > /dev/null
pg_exec "DELETE FROM delivery_attempts WHERE task_id='${TASK3}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK3}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP3}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN3}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP3}'" > /dev/null
log "TC-R03 passed"

# ── TC-R04: lease_recovery → dead_lettered (max attempts) ────────────────────
log "--- TC-R04: lease_recovery → dead_lettered (max_attempts exhausted) ---"
CAMP4=$(uuid4); TASK4=$(uuid4); RUN4=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP4}', gen_random_uuid(), 'smoke-lease-dlq-${CAMP4}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, sending) VALUES ('${CAMP4}', 1, 1)" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN4}', '${CAMP4}', 'default', 'fanout_completed')" > /dev/null
pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority,
     attempt_count, max_attempts, available_at,
     lease_owner, lease_token, lease_until)
VALUES
    ('${TASK4}', '${CAMP4}', '${RUN4}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK4}', 'sending', 'normal',
     5, 5, NOW() - INTERVAL '5 minutes',
     'zombie-worker', gen_random_uuid(), NOW() - INTERVAL '2 minutes')
" > /dev/null
pg_exec "INSERT INTO delivery_attempts (task_id, campaign_id, attempt_no, channel_code, status) VALUES ('${TASK4}', '${CAMP4}', 1, 'email', 'started')" > /dev/null

wait_for_sql \
    "SELECT status FROM delivery_tasks WHERE id='${TASK4}'" \
    "dead_lettered" \
    "TC-R04 task status=dead_lettered" \
    "$JOB_WAIT"

dlq4_reason=$(pg_read "SELECT reason_code FROM dlq_items WHERE task_id='${TASK4}'" | tr -d '[:space:]')
if [ "$dlq4_reason" != "lease_expired_max_attempts" ]; then
    fail "TC-R04: expected reason_code=lease_expired_max_attempts, got '${dlq4_reason}'"
fi
dlq4_status=$(pg_read "SELECT status FROM dlq_items WHERE task_id='${TASK4}'" | tr -d '[:space:]')
if [ "$dlq4_status" != "open" ]; then
    fail "TC-R04: expected dlq status=open, got '${dlq4_status}'"
fi

# Cleanup
pg_exec "DELETE FROM dlq_items WHERE task_id='${TASK4}'" > /dev/null
pg_exec "DELETE FROM delivery_attempts WHERE task_id='${TASK4}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK4}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP4}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN4}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP4}'" > /dev/null
log "TC-R04 passed"

# ── TC-R05: retry_scanner → repush via outbox ─────────────────────────────────
# retry_scanner creates outbox → publisher publishes and DELETES the outbox row →
# delivery worker picks up task. We verify the full chain by waiting for task to
# leave 'retry_scheduled' (delivery picked it up after repush).
log "--- TC-R05: retry_scanner → repush (task leaves retry_scheduled) ---"
CAMP5=$(uuid4); TASK5=$(uuid4); RUN5=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP5}', gen_random_uuid(), 'smoke-retry-scanner-${CAMP5}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, retry_scheduled) VALUES ('${CAMP5}', 1, 1)" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN5}', '${CAMP5}', 'default', 'fanout_completed')" > /dev/null
pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority,
     attempt_count, max_attempts, available_at)
VALUES
    ('${TASK5}', '${CAMP5}', '${RUN5}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK5}', 'retry_scheduled', 'normal',
     1, 5, NOW() - INTERVAL '10 minutes')
" > /dev/null

# Wait: retry_scanner emits outbox → publisher sends to RMQ → delivery changes status
wait_for_sql \
    "SELECT COUNT(*) FROM delivery_tasks WHERE id='${TASK5}' AND status='retry_scheduled'" \
    "0" \
    "TC-R05 task left retry_scheduled (repush delivered to worker)" \
    "$((JOB_WAIT + 30))"

final5=$(pg_read "SELECT status FROM delivery_tasks WHERE id='${TASK5}'" | tr -d '[:space:]')
log "  task final status after repush: ${final5}"

# Cleanup
pg_exec "DELETE FROM outbox_events WHERE payload->>'task_id'='${TASK5}'" > /dev/null
pg_exec "DELETE FROM delivery_attempts WHERE task_id='${TASK5}'" > /dev/null
pg_exec "DELETE FROM dlq_items WHERE task_id='${TASK5}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK5}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP5}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN5}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP5}'" > /dev/null
log "TC-R05 passed"

# ── TC-R06: campaign_finalizer — terminal campaign completion ─────────────────
log "--- TC-R06: campaign_finalizer → campaign finalized ---"
CAMP6=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes, created_at)
VALUES ('${CAMP6}', gen_random_uuid(), 'smoke-finalizer-${CAMP6}', 'running', '{}', '{}', '{}', NOW() - INTERVAL '1 hour')" > /dev/null

# total=3, succeeded=2, failed=1, in_flight=0 → finalizer must set status='partially_failed'
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, queued, sending, succeeded, failed, retry_scheduled, dead_lettered, cancelled)
VALUES ('${CAMP6}', 3, 0, 0, 2, 1, 0, 0, 0)" > /dev/null

wait_for_sql \
    "SELECT status FROM campaigns WHERE id='${CAMP6}'" \
    "partially_failed" \
    "TC-R06 campaign finalized to partially_failed" \
    "$((JOB_WAIT + 10))"

# Cleanup
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP6}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP6}'" > /dev/null
log "TC-R06 passed"

# ── TC-R07: outbox_failed_scanner — failed outbox → dlq + dead_lettered ──────
# outbox_failed_scanner itself deletes the outbox row (unlike publisher).
# We verify: outbox deleted + task dead_lettered + dlq entry with unroutable reason.
log "--- TC-R07: outbox_failed_scanner → dlq + dead_lettered ---"
CAMP7=$(uuid4); TASK7=$(uuid4); RUN7=$(uuid4); OBX7=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP7}', gen_random_uuid(), 'smoke-failed-obx-${CAMP7}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, queued) VALUES ('${CAMP7}', 1, 1)" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN7}', '${CAMP7}', 'default', 'fanout_completed')" > /dev/null
pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority, attempt_count, max_attempts)
VALUES
    ('${TASK7}', '${CAMP7}', '${RUN7}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK7}', 'queued', 'normal', 0, 5)
" > /dev/null
pg_exec "
INSERT INTO outbox_events
    (id, region_id, status, dedupe_key, event_type, exchange, routing_key,
     payload, last_error, attempt_count, next_attempt_at)
VALUES
    ('${OBX7}', 'default', 'failed', 'smoke-fail-${TASK7}',
     'DeliveryTaskCreated', 'notification.direct', 'notification.default.broken.normal',
     '{\"task_id\": \"${TASK7}\", \"campaign_id\": \"${CAMP7}\", \"region_id\": \"default\", \"queue_group\": \"email\", \"priority\": \"normal\", \"channel_code\": \"email\"}',
     'unroutable: routing_key=notification.default.broken.normal',
     10, NOW())
" > /dev/null

wait_for_sql \
    "SELECT status FROM delivery_tasks WHERE id='${TASK7}'" \
    "dead_lettered" \
    "TC-R07 task dead_lettered by outbox_failed_scanner" \
    "$((JOB_WAIT + 10))"

obx7_remaining=$(pg_read "SELECT COUNT(*) FROM outbox_events WHERE id='${OBX7}'" | tr -d '[:space:]')
if [ "$obx7_remaining" != "0" ]; then
    fail "TC-R07: outbox row should be purged by scanner, still ${obx7_remaining} row(s)"
fi

dlq7_reason=$(pg_read "SELECT reason_code FROM dlq_items WHERE task_id='${TASK7}'" | tr -d '[:space:]')
if [ "$dlq7_reason" != "publish_unroutable" ]; then
    fail "TC-R07: expected reason_code=publish_unroutable, got '${dlq7_reason}'"
fi

# Cleanup
pg_exec "DELETE FROM dlq_items WHERE task_id='${TASK7}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK7}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP7}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN7}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP7}'" > /dev/null
log "TC-R07 passed"

# ── TC-R08: POST /v1/tasks/:id/retry — force retry a dead_lettered task ──────
log "--- TC-R08: POST /v1/tasks/:id/retry (force retry) ---"
CAMP8=$(uuid4); TASK8=$(uuid4); RUN8=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP8}', gen_random_uuid(), 'smoke-force-retry-${CAMP8}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, dead_lettered) VALUES ('${CAMP8}', 1, 1)" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN8}', '${CAMP8}', 'default', 'fanout_completed')" > /dev/null
pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority,
     attempt_count, max_attempts, completed_at)
VALUES
    ('${TASK8}', '${CAMP8}', '${RUN8}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK8}', 'dead_lettered', 'normal',
     5, 5, NOW() - INTERVAL '1 hour')
" > /dev/null
pg_exec "INSERT INTO dlq_items (task_id, campaign_id, region_id, channel_code, reason_code, status)
VALUES ('${TASK8}', '${CAMP8}', 'default', 'email', 'lease_expired_max_attempts', 'open')" > /dev/null

recover_post "/v1/tasks/${TASK8}/retry"
assert_status 202 "TC-R08 POST /v1/tasks/:id/retry"

task8_status=$(pg_read "SELECT status FROM delivery_tasks WHERE id='${TASK8}'" | tr -d '[:space:]')
if [ "$task8_status" != "retry_scheduled" ]; then
    fail "TC-R08: expected status=retry_scheduled, got '${task8_status}'"
fi

# Outbox row must have been created for the force retry
wait_for_sql \
    "SELECT COUNT(*) FROM outbox_events WHERE payload->>'task_id'='${TASK8}'" \
    "1" \
    "TC-R08 force-retry outbox row created" \
    "15"

dlq8_status=$(pg_read "SELECT status FROM dlq_items WHERE task_id='${TASK8}'" | tr -d '[:space:]')
if [ "$dlq8_status" != "replayed" ]; then
    fail "TC-R08: expected dlq status=replayed, got '${dlq8_status}'"
fi

# Idempotency: second call on retry_scheduled → 409
recover_post "/v1/tasks/${TASK8}/retry"
assert_status 409 "TC-R08 second force retry on retry_scheduled → 409 conflict"

# Cleanup (delete in dependency order)
pg_exec "DELETE FROM outbox_events WHERE payload->>'task_id'='${TASK8}'" > /dev/null
pg_exec "DELETE FROM dlq_items WHERE task_id='${TASK8}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK8}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP8}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN8}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP8}'" > /dev/null
log "TC-R08 passed"

# ── TC-R09: GET /v1/dlq + POST /v1/dlq/:id/replay ────────────────────────────
log "--- TC-R09: GET /v1/dlq + POST /v1/dlq/:id/replay ---"
CAMP9=$(uuid4); TASK9=$(uuid4); RUN9=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP9}', gen_random_uuid(), 'smoke-dlq-replay-${CAMP9}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_stats (campaign_id, total_tasks, dead_lettered) VALUES ('${CAMP9}', 1, 1)" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status) VALUES ('${RUN9}', '${CAMP9}', 'default', 'fanout_completed')" > /dev/null
pg_exec "
INSERT INTO delivery_tasks
    (id, campaign_id, campaign_region_run_id, region_id, user_id, user_channel_id,
     channel_id, channel_code, queue_group, recipient_address_snapshot,
     message_snapshot, idempotency_key, status, priority,
     attempt_count, max_attempts, completed_at)
VALUES
    ('${TASK9}', '${CAMP9}', '${RUN9}', 'default',
     gen_random_uuid(), gen_random_uuid(), gen_random_uuid(),
     'email', 'email', 'smoke@test.local',
     '{}', 'smoke-idem-${TASK9}', 'dead_lettered', 'normal',
     5, 5, NOW())
" > /dev/null

DLQ9=$(pg_exec "INSERT INTO dlq_items (task_id, campaign_id, region_id, channel_code, reason_code, status)
VALUES ('${TASK9}', '${CAMP9}', 'default', 'email', 'smoke_test', 'open') RETURNING id" \
| tr -d '[:space:]')

# List DLQ — our entry must appear
recover_get "/v1/dlq?status=open&limit=500"
assert_status 200 "TC-R09 GET /v1/dlq"

found=$(python3 - "$LAST_BODY" "$TASK9" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
target = sys.argv[2]
items = d.get("items", [])
print(sum(1 for i in items if i.get("task_id") == target))
PY
)
if [ "$found" -lt 1 ]; then
    fail "TC-R09: GET /v1/dlq did not contain task_id=${TASK9}"
fi

# Replay
recover_post "/v1/dlq/${DLQ9}/replay"
assert_status 202 "TC-R09 POST /v1/dlq/:id/replay"

task9_status=$(pg_read "SELECT status FROM delivery_tasks WHERE id='${TASK9}'" | tr -d '[:space:]')
if [ "$task9_status" != "retry_scheduled" ]; then
    fail "TC-R09: expected status=retry_scheduled after dlq replay, got '${task9_status}'"
fi
dlq9_final=$(pg_read "SELECT status FROM dlq_items WHERE id='${DLQ9}'" | tr -d '[:space:]')
if [ "$dlq9_final" != "replayed" ]; then
    fail "TC-R09: expected dlq status=replayed, got '${dlq9_final}'"
fi

# Second replay on already-replayed entry → 409
recover_post "/v1/dlq/${DLQ9}/replay"
assert_status 409 "TC-R09 second replay on replayed DLQ entry → 409"

# Non-existent DLQ → 404
FAKE_DLQ=$(uuid4)
recover_post "/v1/dlq/${FAKE_DLQ}/replay"
assert_status 404 "TC-R09 replay non-existent DLQ entry → 404"

# Cleanup
pg_exec "DELETE FROM outbox_events WHERE payload->>'task_id'='${TASK9}'" > /dev/null
pg_exec "DELETE FROM dlq_items WHERE id='${DLQ9}'" > /dev/null
pg_exec "DELETE FROM delivery_tasks WHERE id='${TASK9}'" > /dev/null
pg_exec "DELETE FROM campaign_stats WHERE campaign_id='${CAMP9}'" > /dev/null
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN9}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP9}'" > /dev/null
log "TC-R09 passed"

# ── TC-R10: POST /v1/runs/:id/recover — unstick a fanout run ─────────────────
log "--- TC-R10: POST /v1/runs/:id/recover ---"
CAMP10=$(uuid4); RUN10=$(uuid4)

pg_exec "INSERT INTO campaigns (id, manager_id, name, status, message_snapshot, recipient_selector, selected_channel_codes)
VALUES ('${CAMP10}', gen_random_uuid(), 'smoke-run-recover-${CAMP10}', 'running', '{}', '{}', '{}')" > /dev/null
pg_exec "INSERT INTO campaign_region_runs (id, campaign_id, region_id, status, fanout_lock_owner, fanout_lock_until)
VALUES ('${RUN10}', '${CAMP10}', 'default', 'fanout_running', 'stuck-fanout-worker', NOW() - INTERVAL '30 minutes')" > /dev/null

recover_post "/v1/runs/${RUN10}/recover"
assert_status 202 "TC-R10 POST /v1/runs/:id/recover"

run10_status=$(pg_read "SELECT status FROM campaign_region_runs WHERE id='${RUN10}'" | tr -d '[:space:]')
if [ "$run10_status" != "fanout_pending" ]; then
    fail "TC-R10: expected status=fanout_pending, got '${run10_status}'"
fi
lock10=$(pg_read "SELECT fanout_lock_until IS NULL FROM campaign_region_runs WHERE id='${RUN10}'" | tr -d '[:space:]')
if [ "$lock10" != "t" ]; then
    fail "TC-R10: expected fanout_lock_until=NULL after recovery, got '${lock10}'"
fi

# Recovering a non-stuck run → 409
recover_post "/v1/runs/${RUN10}/recover"
assert_status 409 "TC-R10 recover already-reset run → 409"

# Non-existent run → 404
FAKE_RUN=$(uuid4)
recover_post "/v1/runs/${FAKE_RUN}/recover"
assert_status 404 "TC-R10 recover non-existent run → 404"

# Cleanup
pg_exec "DELETE FROM campaign_region_runs WHERE id='${RUN10}'" > /dev/null
pg_exec "DELETE FROM campaigns WHERE id='${CAMP10}'" > /dev/null
log "TC-R10 passed"

# ── Summary ───────────────────────────────────────────────────────────────────
log ""
log "=== Recovery smoke PASSED ==="
log "TC-R01 health gate                 OK"
log "TC-R02 outbox_recovery (auto)      OK"
log "TC-R03 lease_recovery → retry      OK"
log "TC-R04 lease_recovery → dlq        OK"
log "TC-R05 retry_scanner repush        OK"
log "TC-R06 campaign_finalizer          OK"
log "TC-R07 outbox_failed_scanner       OK"
log "TC-R08 force retry API             OK"
log "TC-R09 DLQ list + replay API       OK"
log "TC-R10 run recovery API            OK"
