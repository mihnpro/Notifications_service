#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SMOKE_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
STATE_FILE="${SMOKE_STATE_FILE:-$SCRIPT_DIR/.smoke.env}"

API_URL="${API_URL:-http://localhost:8000}"
MANAGER_ID="${MANAGER_ID:-11111111-1111-1111-1111-111111111111}"
AUTH_TOKEN="${AUTH_TOKEN:-$MANAGER_ID}"
SMOKE_SEQ=1

LAST_STATUS=""
LAST_BODY=""

log() {
  printf '[smoke] %s\n' "$*"
}

fail() {
  printf '[smoke][FAIL] %s\n' "$*" >&2
  if [ -n "${LAST_BODY:-}" ] && [ -f "$LAST_BODY" ]; then
    printf '[smoke][FAIL] response body:\n' >&2
    cat "$LAST_BODY" >&2
    printf '\n' >&2
  fi
  exit 1
}

load_state() {
  if [ -f "$STATE_FILE" ]; then
    # shellcheck disable=SC1090
    . "$STATE_FILE"
  fi
}

save_state() {
  key="$1"
  value="$2"
  touch "$STATE_FILE"
  # last assignment wins when sourcing
  printf "export %s='%s'\n" "$key" "$value" >> "$STATE_FILE"
}

reset_state() {
  : > "$STATE_FILE"
}

new_idempotency_key() {
  label="$1"
  ts=$(date +%s)
  IDEMPOTENCY_KEY="smoke-${label}-${ts}-$$-${SMOKE_SEQ}"
  SMOKE_SEQ=$((SMOKE_SEQ + 1))
}

_call() {
  method="$1"
  path="$2"
  body="${3:-}"
  use_auth="${4:-0}"
  use_idem="${5:-0}"

  LAST_BODY=$(mktemp)
  url="${API_URL}${path}"

  set -- -sS -o "$LAST_BODY" -w "%{http_code}" -X "$method" "$url"
  if [ "$use_auth" -eq 1 ]; then
    set -- "$@" -H "Authorization: Bearer ${AUTH_TOKEN}"
  fi
  if [ "$use_idem" -eq 1 ]; then
    set -- "$@" -H "Idempotency-Key: ${IDEMPOTENCY_KEY}"
  fi
  if [ -n "$body" ]; then
    set -- "$@" -H "Content-Type: application/json" --data "$body"
  fi

  LAST_STATUS=$(curl "$@")
}

call_public() {
  _call "$1" "$2" "${3:-}" 0 0
}

call_auth() {
  _call "$1" "$2" "${3:-}" 1 0
}

call_mutate() {
  method="$1"
  path="$2"
  body="${3:-}"
  label="${4:-mutate}"
  new_idempotency_key "$label"
  _call "$method" "$path" "$body" 1 1
}

assert_status() {
  expected="$1"
  context="$2"
  if [ "$LAST_STATUS" != "$expected" ]; then
    fail "${context}: expected ${expected}, got ${LAST_STATUS}"
  fi
  log "${context}: status ${expected}"
}

json_get() {
  path="$1"
  python3 - "$LAST_BODY" "$path" <<'PY'
import json
import sys

body_path = sys.argv[1]
path = sys.argv[2]

with open(body_path, "r", encoding="utf-8") as f:
    data = json.load(f)

cur = data
if path:
    for part in path.split("."):
        if isinstance(cur, list):
            cur = cur[int(part)]
        else:
            cur = cur[part]

if isinstance(cur, (dict, list)):
    print(json.dumps(cur))
else:
    print(cur)
PY
}
