# Load & Chaos Tests

Three tests against a running docker-compose stack:

| Test | What it does | What it answers |
|---|---|---|
| `test_golden_path.py` | 50k users → 1 campaign (email+sms) → wait for terminal | p95 per stage, throughput, loss ratio in the happy path |
| `test_chaos_delivery.py` | Same, but stops `delivery` for 30 s mid-run | How many messages stall when consumers die, and whether `recover` heals the leases |
| `test_chaos_rabbitmq.py` | Same, but stops `rabbitmq` for 45 s mid-run | Whether the outbox pattern produces zero permanent loss across a broker outage |

Each test prints a results table:

```
=== RESULT ===
scenario                              | golden-50k
users                                 | 50000
expected tasks                        | 100000
delivered (succeeded)                 | 99987
failed                                | 12
dead_lettered                         | 1
still in-flight at deadline           | 0
LOSS (failed+dead_lettered+missing)   | 13
loss ratio                            | 0.013 %
throughput                            | 432 msg/s
...
--- Pipeline latencies ---
api_http_request_duration_seconds_p95     | 24.3 ms
fanout_run_duration_seconds_p95           | 1.2 s
delivery_task_duration_seconds_p95        | 68.0 ms
delivery_provider_call_duration_seconds_p95 | 41.0 ms
```

## Running

```bash
# 1. Bring up the full stack
cd deploy && docker compose up -d --build

# 2. Wait for healthy
docker compose ps

# 3. Run a single scenario
cd ../loadtests
uv sync
uv run pytest test_golden_path.py

# Or all three (chaos tests will stop/start containers, so run them serially)
uv run pytest -m load
uv run pytest -m chaos_delivery
uv run pytest -m chaos_rabbitmq
```

## Tunable via env

| Variable | Default | What it controls |
|---|---|---|
| `LT_API_URL` | `http://localhost:8000` | API base URL |
| `LT_PROM_URL` | `http://localhost:9090` | Prometheus base URL |
| `LT_PG_DSN` | `postgres://notifications:notifications@localhost:5432/notifications` | DB for user seeding + manager bootstrap |
| `LT_TOTAL_USERS` | `50000` | Reduce to e.g. `1000` for smoke runs |
| `LT_COMPLETION_TIMEOUT_S` | `300` | How long to wait for the campaign to drain |
| `LT_COMPOSE_DIR` | `../deploy` | Where to find `docker-compose.yml` for chaos |

## What "loss" means here

`loss = failed + dead_lettered + (expected_tasks - actual_tasks_in_db)`.

- `expected_tasks = users × channels`.
- `actual_tasks_in_db` should match `expected_tasks`; any shortfall means funout dropped them.
- A task in `retry_scheduled` that never reaches a terminal state before `LT_COMPLETION_TIMEOUT_S` shows up as `in_flight > 0` and fails the assertion separately.

## How p95 is computed

Each service exposes Prometheus histograms (`*_duration_seconds_bucket`). We query
`histogram_quantile(0.95, sum(rate(<metric>[<window>s])) by (le))` for a window that
covers the whole run.
