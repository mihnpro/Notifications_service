#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

load_state

if [ -z "${CHANNEL_CODE:-}" ]; then
  fail "CHANNEL_CODE is not set. Run 02_channels.sh first or export CHANNEL_CODE."
fi

log "Campaigns smoke with channel ${CHANNEL_CODE}"

create_body=$(cat <<JSON
{
  "name": "Smoke Campaign $(date +%s)",
  "regionIds": ["default"],
  "message": {
    "subject": "Smoke Subject",
    "body": "Smoke Body"
  },
  "recipientSelector": {
    "type": "all"
  },
  "channels": ["${CHANNEL_CODE}"],
  "priority": "normal"
}
JSON
)
call_mutate POST /campaigns "$create_body" "campaigns-create"
assert_status 202 "POST /campaigns"
CAMPAIGN_ID=$(json_get "campaignId")
REGION_RUN_ID=$(json_get "regionRuns.0.id")
save_state CAMPAIGN_ID "$CAMPAIGN_ID"
save_state REGION_RUN_ID "$REGION_RUN_ID"
log "Created campaign ${CAMPAIGN_ID}"

call_auth GET /campaigns
assert_status 200 "GET /campaigns"

call_auth GET "/campaigns/${CAMPAIGN_ID}"
assert_status 200 "GET /campaigns/{id}"

call_auth GET "/campaigns/${CAMPAIGN_ID}/stats"
assert_status 200 "GET /campaigns/{id}/stats"

call_auth GET "/campaigns/${CAMPAIGN_ID}/tasks"
assert_status 200 "GET /campaigns/{id}/tasks"

call_auth GET "/campaigns/${CAMPAIGN_ID}/results"
assert_status 200 "GET /campaigns/{id}/results"

call_auth GET "/campaigns/${CAMPAIGN_ID}/errors"
assert_status 200 "GET /campaigns/{id}/errors"

cancel_body='{"reason":"smoke-cancel"}'
call_mutate POST "/campaigns/${CAMPAIGN_ID}/cancel" "$cancel_body" "campaigns-cancel"
assert_status 202 "POST /campaigns/{id}/cancel"

log "Campaigns smoke passed"
