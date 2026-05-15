#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

load_state

log "DLQ smoke"

call_auth GET /dlq
assert_status 200 "GET /dlq"

filter_json='{}'
if [ -n "${CAMPAIGN_ID:-}" ]; then
  filter_json=$(cat <<JSON
{"campaignId":"${CAMPAIGN_ID}"}
JSON
)
fi

replay_body=$(cat <<JSON
{
  "regionId": "default",
  "filter": ${filter_json},
  "limit": 10,
  "additionalAttempts": 1,
  "reason": "smoke-replay"
}
JSON
)
call_mutate POST /dlq/replay "$replay_body" "dlq-replay"
assert_status 200 "POST /dlq/replay"

log "DLQ smoke passed"
