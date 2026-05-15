#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

load_state

if [ -z "${CHANNEL_CODE:-}" ]; then
  fail "CHANNEL_CODE is not set. Run 02_channels.sh first or export CHANNEL_CODE."
fi

log "Users bulk smoke"

address="+7900000$(date +%s | tail -c 5)"
external_id="smoke-user-$(date +%s)-$$"

bulk_body=$(cat <<JSON
{
  "mode": "skip_duplicates",
  "items": [
    {
      "externalId": "${external_id}",
      "status": "active",
      "channels": [
        {
          "channel": "${CHANNEL_CODE}",
          "address": "${address}",
          "status": "active",
          "verified": true
        }
      ]
    }
  ]
}
JSON
)
call_mutate POST /users/bulk "$bulk_body" "users-bulk"
assert_status 200 "POST /users/bulk"

log "Users bulk smoke passed"
