#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

reset_state

log "Running full smoke suite against ${API_URL}"
"$SCRIPT_DIR/01_health.sh"
"$SCRIPT_DIR/02_channels.sh"
"$SCRIPT_DIR/03_campaigns.sh"
"$SCRIPT_DIR/04_dlq.sh"
"$SCRIPT_DIR/05_users.sh"

log "All smoke scripts passed"
