#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck disable=SC1091
. "$SCRIPT_DIR/common.sh"

log "Health checks against ${API_URL}"

call_public GET /health
assert_status 200 "GET /health"

call_public GET /healthz
assert_status 200 "GET /healthz"

call_public GET /readyz
assert_status 200 "GET /readyz"

log "Health smoke passed"
