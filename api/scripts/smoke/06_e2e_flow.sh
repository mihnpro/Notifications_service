#!/usr/bin/env sh
# 06_e2e_flow.sh — End-to-end pipeline smoke test
#
# Tests the full async flow:
#   API → outbox_events → publisher → RabbitMQ
#     → funout (creates delivery_tasks)
#     → publisher → RabbitMQ
#     → delivery worker → provider_mock
#
# Requires all services running: api, publisher, funout, delivery, provider_mock.
#
# Tunable env vars:
#   API_URL                  default: http://localhost:80
#   AUTH_TOKEN               default: 11111111-1111-1111-1111-111111111111
#   E2E_FANOUT_TIMEOUT_SEC   default: 60
#   E2E_DELIVERY_TIMEOUT_SEC default: 120
#   E2E_POLL_INTERVAL_SEC    default: 3

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

FANOUT_TIMEOUT_SEC="${E2E_FANOUT_TIMEOUT_SEC:-60}"
DELIVERY_TIMEOUT_SEC="${E2E_DELIVERY_TIMEOUT_SEC:-120}"
POLL_INTERVAL="${E2E_POLL_INTERVAL_SEC:-3}"

TS=$(date +%s)
CHANNEL_CODE="e2e-${TS}-$$"
EXTERNAL_ID="e2e-user-${TS}-$$"
ADDRESS="e2e-${TS}@smoke.test"

log "=== E2E flow smoke ==="
log "API_URL=${API_URL}"
log "channel=${CHANNEL_CODE}  user_external_id=${EXTERNAL_ID}"

# ── Step 1: health gate ───────────────────────────────────────────────────────
call_public GET /healthz
assert_status 200 "GET /healthz"

# ── Step 2: create channel ────────────────────────────────────────────────────
log "--- Step 2: create channel ---"
create_channel_body=$(cat <<JSON
{
  "code": "${CHANNEL_CODE}",
  "displayName": "E2E Smoke Channel",
  "globalState": "enabled",
  "adapterName": "stub",
  "queueGroup": "email",
  "providerCode": "smoke-mock",
  "disablePolicy": "retry_later"
}
JSON
)
call_mutate POST /channels "$create_channel_body" "e2e-channel"
assert_status 201 "POST /channels"
CHANNEL_ID=$(json_get "id")
log "Channel created: id=${CHANNEL_ID} code=${CHANNEL_CODE}"

# ── Step 3: create user with that channel ─────────────────────────────────────
log "--- Step 3: create user ---"
bulk_body=$(cat <<JSON
{
  "mode": "skip_duplicates",
  "items": [
    {
      "externalId": "${EXTERNAL_ID}",
      "status": "active",
      "channels": [
        {
          "channel": "${CHANNEL_CODE}",
          "address": "${ADDRESS}",
          "status": "active",
          "verified": true
        }
      ]
    }
  ]
}
JSON
)
call_mutate POST /users/bulk "$bulk_body" "e2e-user"
assert_status 200 "POST /users/bulk"
log "User created: external_id=${EXTERNAL_ID} address=${ADDRESS}"

# ── Step 4: create campaign targeting exactly our user ────────────────────────
log "--- Step 4: create campaign ---"
campaign_body=$(cat <<JSON
{
  "name": "E2E Smoke ${TS}",
  "regionIds": ["default"],
  "message": {
    "subject": "E2E Smoke Test",
    "body": "End-to-end smoke test — ${TS}"
  },
  "recipientSelector": {
    "type": "external_ids",
    "externalIds": ["${EXTERNAL_ID}"]
  },
  "channels": ["${CHANNEL_CODE}"],
  "priority": "normal"
}
JSON
)
call_mutate POST /campaigns "$campaign_body" "e2e-campaign"
assert_status 202 "POST /campaigns"
CAMPAIGN_ID=$(json_get "campaignId")
log "Campaign created: ${CAMPAIGN_ID}"

# ── Step 5: wait for fanout (totalTasks > 0) ──────────────────────────────────
log "--- Step 5: waiting for fanout (timeout=${FANOUT_TIMEOUT_SEC}s) ---"
elapsed=0
total_tasks=0
while [ "$elapsed" -lt "$FANOUT_TIMEOUT_SEC" ]; do
    call_auth GET "/campaigns/${CAMPAIGN_ID}/stats"
    assert_status 200 "GET /campaigns/${CAMPAIGN_ID}/stats (fanout poll)"
    total_tasks=$(json_get "stats.totalTasks")

    log "  fanout poll: totalTasks=${total_tasks} elapsed=${elapsed}s"

    if [ "$total_tasks" -gt 0 ]; then
        log "Fanout done: ${total_tasks} delivery task(s) created"
        break
    fi

    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
done

if [ "$total_tasks" -eq 0 ]; then
    fail "Fanout timed out after ${FANOUT_TIMEOUT_SEC}s — no delivery tasks were created. Check funout worker logs."
fi

# ── Step 6: wait for all delivery tasks to reach terminal state ───────────────
log "--- Step 6: waiting for delivery (timeout=${DELIVERY_TIMEOUT_SEC}s) ---"
elapsed=0
in_flight=1
while [ "$elapsed" -lt "$DELIVERY_TIMEOUT_SEC" ]; do
    call_auth GET "/campaigns/${CAMPAIGN_ID}/stats"
    assert_status 200 "GET /campaigns/${CAMPAIGN_ID}/stats (delivery poll)"

    queued=$(json_get "stats.queued")
    sending=$(json_get "stats.sending")
    retry=$(json_get "stats.retryScheduled")
    succeeded=$(json_get "stats.succeeded")
    dead=$(json_get "stats.deadLettered")
    failed=$(json_get "stats.failed")
    in_flight=$((queued + sending + retry))

    log "  delivery poll: succeeded=${succeeded} dead=${dead} failed=${failed} in_flight=${in_flight} elapsed=${elapsed}s"

    if [ "$in_flight" -eq 0 ]; then
        log "All tasks reached terminal state"
        break
    fi

    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
done

if [ "$in_flight" -gt 0 ]; then
    fail "Delivery timed out after ${DELIVERY_TIMEOUT_SEC}s — ${in_flight} task(s) still in flight. Check delivery worker logs."
fi

# ── Step 7: assert final state ────────────────────────────────────────────────
log "--- Step 7: assertions ---"
call_auth GET "/campaigns/${CAMPAIGN_ID}/stats"
assert_status 200 "GET /campaigns/${CAMPAIGN_ID}/stats (final)"

total=$(json_get "stats.totalTasks")
succeeded=$(json_get "stats.succeeded")
dead=$(json_get "stats.deadLettered")
failed=$(json_get "stats.failed")
terminal=$((succeeded + dead + failed))

log "Final stats: total=${total} succeeded=${succeeded} dead_lettered=${dead} failed=${failed}"

if [ "$terminal" -ne "$total" ]; then
    fail "Not all tasks in terminal state: terminal=${terminal} != total=${total}"
fi
if [ "$total" -ne 1 ]; then
    fail "Expected exactly 1 delivery task (1 user × 1 channel), got ${total}"
fi

# ── Step 8: print delivery task details ───────────────────────────────────────
log "--- Step 8: task details ---"
call_auth GET "/campaigns/${CAMPAIGN_ID}/tasks"
assert_status 200 "GET /campaigns/${CAMPAIGN_ID}/tasks"

task_status=$(json_get "items.0.status")
attempt_count=$(json_get "items.0.attemptCount")
log "Task: status=${task_status} attempts=${attempt_count}"

if [ "$task_status" != "succeeded" ] && [ "$task_status" != "dead_lettered" ] && [ "$task_status" != "failed" ]; then
    fail "Unexpected task terminal status: ${task_status}"
fi

# ── Step 9: check errors endpoint (should work regardless of outcome) ─────────
call_auth GET "/campaigns/${CAMPAIGN_ID}/errors"
assert_status 200 "GET /campaigns/${CAMPAIGN_ID}/errors"

log ""
log "=== E2E flow smoke PASSED ==="
log "Campaign ${CAMPAIGN_ID}: ${succeeded}/${total} succeeded, ${dead}/${total} dead-lettered, ${failed}/${total} failed"
log "Task final status: ${task_status} after ${attempt_count} attempt(s)"
