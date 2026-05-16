#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

load_state

CHANNEL_CODE="${CHANNEL_CODE:-smoke-whatsapp-$(date +%s)-$$}"
CHANNEL_DISPLAY="Smoke WhatsApp"

log "Channels smoke"

call_auth GET /channels
assert_status 200 "GET /channels"

create_body=$(cat <<JSON
{
  "code": "${CHANNEL_CODE}",
  "displayName": "${CHANNEL_DISPLAY}",
  "globalState": "enabled",
  "adapterName": "stub",
  "queueGroup": "messenger",
  "providerCode": "stub-whatsapp",
  "disablePolicy": "retry_later"
}
JSON
)
call_mutate POST /channels "$create_body" "channels-create"
assert_status 201 "POST /channels"
CHANNEL_ID=$(json_get "id")
save_state CHANNEL_ID "$CHANNEL_ID"
save_state CHANNEL_CODE "$CHANNEL_CODE"
log "Created channel ${CHANNEL_CODE} (${CHANNEL_ID})"

call_auth GET "/channels/${CHANNEL_ID}/regional-configs"
assert_status 200 "GET /channels/{id}/regional-configs"

patch_body=$(cat <<JSON
{
  "displayName": "Smoke WhatsApp Updated",
  "providerCode": "stub-whatsapp-v2"
}
JSON
)
call_mutate PATCH "/channels/${CHANNEL_ID}" "$patch_body" "channels-patch"
assert_status 200 "PATCH /channels/{id}"

call_mutate POST "/channels/${CHANNEL_ID}/disable" "" "channels-disable"
assert_status 200 "POST /channels/{id}/disable"

call_mutate POST "/channels/${CHANNEL_ID}/enable" "" "channels-enable"
assert_status 200 "POST /channels/{id}/enable"

regional_body=$(cat <<JSON
{
  "state": "enabled",
  "adapterVersion": "v1",
  "providerCode": "stub-whatsapp-v3",
  "configRef": "secret://notification/default/${CHANNEL_CODE}/stub",
  "rateLimits": {
    "rps": 25,
    "maxConcurrency": 40
  },
  "retryPolicy": {
    "maxAttempts": 5,
    "baseDelaySeconds": 30,
    "maxDelaySeconds": 1800
  },
  "disablePolicy": "retry_later"
}
JSON
)
call_mutate PUT "/channels/${CHANNEL_ID}/regional-configs/default" "$regional_body" "channels-regional-upsert"
assert_status 200 "PUT /channels/{id}/regional-configs/default"

log "Channels smoke passed"
