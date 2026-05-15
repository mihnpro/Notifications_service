# Notification Platform MVP — план задач для команды из 4 человек

Версия: 1.1  
Цель: расписать реализацию production-compatible MVP сервиса нотификаций силами команды из 4 человек.

---

## 0. MVP scope

MVP должен показать:

```text
1. Manager creates campaign through POST /campaigns.
2. Campaign targets all users or selected users.
3. Manager selects channels.
4. Channels can be added/disabled through DB/API config.
5. API returns 202 quickly; p95 POST /campaigns < 1500 ms.
6. Fan-out creates tasks asynchronously for 50k users.
7. RabbitMQ routes by region + queue_group + priority (for MVP region=default).
8. Provider stub sleeps random 2-300 seconds and returns random success/error.
9. Results show recipient, channel, message and timestamps from immutable snapshots.
10. Worker restart does not lose tasks.
11. Retry, DLQ, replay, recovery jobs and stats shards work.
```

Non-goals for MVP:

```text
Kafka/Debezium in runtime
multi-region physical DB split
distributed Redis rate limiter
real email/SMS/push provider integrations
advanced segment builder UI
exactly-once delivery
```

But MVP must keep contracts ready for those future steps.

## 0.1 MVP regional simplification

```text
MVP runs in single-region mode with region_id='default'.
region_id/regionId stays mandatory in DB entities and message contracts.
All API/routing/payload examples use regionId=default.
```

Not required in this MVP:

```text
multi-region physical DB split
regional RabbitMQ clusters
multi-region runtime policy/config operations
```

---

## 1. Команда и зоны ответственности

### Person A — Backend API + domain model owner

Primary ownership:

```text
HTTP API
request validation
idempotency
campaign/channel/user/DLQ endpoints
DB migrations ownership together with Person B
OpenAPI/contract docs
```

### Person B — Data layer + fan-out + stats owner

Primary ownership:

```text
PostgreSQL schema
repositories/transactions
fan-out worker
campaign_recipients/delivery_tasks creation
stats shards and reconciliation
seed/load data
```

### Person C — RabbitMQ + workers + delivery/retry/DLQ owner

Primary ownership:

```text
RabbitMQ topology
outbox publisher
delivery worker
lease heartbeat
provider stub and adapter interface
retry buckets
DLQ/replay execution
recovery jobs for leases/outbox/retry
```

### Person D — Manager UI / QA / DevOps / Observability owner

Primary ownership:

```text
Docker Compose / local environment
Manager UI or API demo client
E2E tests and load tests
metrics/logging dashboards
acceptance scenario scripts
documentation and demo flow
```

Person D can pair with A/B/C on backend tasks if UI is intentionally minimal.

---

## 2. Suggested repository layout

```text
/notification-platform
  /cmd
    /api
    /worker
    /outbox-publisher
    /recovery
    /seed
  /internal
    /api
    /domain
    /db
    /outbox
    /rabbitmq
    /fanout
    /delivery
    /channels
    /providers
    /stats
    /recovery
    /observability
  /migrations
  /deploy
    docker-compose.yml
    rabbitmq-definitions.json
  /docs
    production-compatible-mvp.md
    production-architecture.md
    mvp-team-plan.md
  /tests
    /integration
    /e2e
    /load
```

---

## 3. Milestones

### Milestone 1 — Foundation: schema, config, local stack

Goal:

```text
PostgreSQL + RabbitMQ + migrations + seed channels/users can run locally.
```

Expected duration:

```text
1-2 days for hackathon mode
```

Deliverables:

```text
Docker Compose
DB migrations
RabbitMQ exchanges/queues/retry buckets/DLQ
seed command for 50k users
base Go project structure
config loading
```

### Milestone 2 — API and campaign creation path

Goal:

```text
POST /campaigns creates campaign/run/outbox and returns 202 without fan-out.
```

Deliverables:

```text
Idempotency-Key handling
POST /campaigns
GET /campaigns/{id}
GET /campaigns/{id}/stats
channel endpoints or seed/config commands
user bulk seed/import endpoint or CLI
```

### Milestone 3 — Outbox + fan-out

Goal:

```text
CampaignRegionRunRequested is published and fan-out creates tasks for 50k users asynchronously.
```

Deliverables:

```text
Outbox publisher with publisher confirms
fan-out worker with run lock
recipient selector all/user_ids/external_ids
batch insert delivery_tasks and outbox events
stats total/queued increments
idempotent rerun after crash
```

### Milestone 4 — Delivery worker + provider stub

Goal:

```text
DeliveryTaskCreated messages are consumed, stub is called, attempts/results are saved.
```

Deliverables:

```text
Delivery worker
atomic lease + delivery_attempt creation
lease heartbeat
provider timeout 320s
GenericStubAdapter random 2-300s latency and random result
immutable delivery_results
stats transitions
```

### Milestone 5 — Retry, DLQ, recovery

Goal:

```text
Transient errors retry, exhausted tasks go to DB DLQ, recovery jobs repair lost signals/leases.
```

Deliverables:

```text
Retry buckets 30s/1m/5m/15m
TaskRetryScheduled outbox events
DB available_at guard
DLQ items and replay API
outbox recovery
lease recovery
retry recovery scanner
stats reconciliation
campaign finalizer
```

### Milestone 6 — UI/demo, load test, acceptance

Goal:

```text
End-to-end demo: 50k users, campaign, results, failures, worker restart, channel disable, replay.
```

Deliverables:

```text
Manager UI or scripted demo client
results/errors/DLQ pages or CLI views
load test for POST /campaigns p95
kill-worker scenario
acceptance checklist report
README and runbook
```

---

## 4. Detailed task breakdown

## 4.1 Person A — Backend API + domain model

### A1. API skeleton and routing

Tasks:

```text
- Create API service structure.
- Add health/readiness endpoints.
- Add request/response error envelope.
- Add auth placeholder / manager_id extraction stub.
- Add config for DB/RabbitMQ/feature flags.
```

Acceptance:

```text
GET /healthz returns 200
GET /readyz checks DB connectivity
API returns consistent JSON errors
```

Dependencies:

```text
None
```

---

### A2. Idempotency implementation

Tasks:

```text
- Implement idempotency_keys repository.
- Canonical JSON request hashing.
- Processing/completed/failed state handling.
- Conflict behavior for reused key with different body.
```

Acceptance:

```text
same Idempotency-Key + same body returns same response
same Idempotency-Key + different body returns 409
parallel same-key request returns 409 REQUEST_ALREADY_PROCESSING or stored response
```

Dependencies:

```text
B1 migrations
```

---

### A3. Campaign API

Tasks:

```text
- Implement POST /campaigns.
- Validate regionIds: in MVP only ["default"] is accepted.
- Validate recipientSelector types: all, user_ids, external_ids, segment optional.
- Validate selected channels and config for regionId=default.
- Insert campaigns and one campaign_region_run (regionId=default).
- Insert one CampaignRegionRunRequested outbox event (regionId=default).
- Return 202.
```

Acceptance:

```text
POST /campaigns does not query all users
POST /campaigns does not create delivery_tasks
POST /campaigns does not call provider
POST /campaigns inserts one outbox event with regionId=default
```

Dependencies:

```text
A2 idempotency
B1 schema
C1 outbox model agreement
```

---

### A4. Campaign read endpoints

Tasks:

```text
- GET /campaigns
- GET /campaigns/{id}
- GET /campaigns/{id}/stats
- GET /campaigns/{id}/tasks
- GET /campaigns/{id}/results
- GET /campaigns/{id}/errors
- Cursor pagination
```

Acceptance:

```text
stats endpoint reads campaign_stats_shards only
results endpoint shows recipient/channel/message/timestamps from snapshots
errors endpoint can filter by channel/errorCode
pagination stable under new inserts
```

Dependencies:

```text
B4 stats
C4 delivery results/attempts
```

---

### A5. Channel API/config

Tasks:

```text
- GET /channels
- POST /channels
- PATCH /channels/{id}
- enable/disable endpoints or admin seed command
- default-region config upsert (kept in final contract shape)
- enforce queue_group choices
```

Acceptance:

```text
can add whatsapp with queue_group=messenger
new channel is accepted by POST /campaigns after default-region config is enabled
channel disabled is rejected for new campaign or handled by selected policy
```

Dependencies:

```text
B1 schema
C2 RabbitMQ queue groups
```

---

### A6. DLQ API

Tasks:

```text
- GET /dlq with filters.
- POST /dlq/replay.
- Validate replay limit/additionalAttempts.
- Call service that resets tasks and inserts outbox events.
```

Acceptance:

```text
can filter DLQ by campaign/channel/errorCode
replay moves tasks from dead_lettered to queued/retry_scheduled
replay marks dlq_items as replayed
```

Dependencies:

```text
C7 DLQ logic
B4 stats transitions
```

---

## 4.2 Person B — Data layer + fan-out + stats

### B1. Database migrations

Tasks:

```text
- Add pgcrypto extension.
- Create users/user_channels with region_id NOT NULL DEFAULT 'default'.
- Create channels/channel_regional_configs with queue_group.
- Create campaigns/campaign_region_runs/campaign_recipients.
- Create delivery_tasks with PK(region_id, campaign_region_run_id, id).
- Create delivery_attempts/results/dlq_items with composite keys.
- Create outbox_events with PK(region_id, id), UNIQUE(region_id, dedupe_key).
- Create idempotency_keys and campaign_stats_shards.
- Add indexes.
```

Acceptance:

```text
migrations run from empty DB
migrations can be rolled back or reset in dev
hot table keys include region_id and campaign_region_run_id
outbox region_id is NOT NULL
single-region mode works without mandatory runtime region directory operations
```

Dependencies:

```text
Architecture agreement
```

---

### B2. Repositories and transaction helpers

Tasks:

```text
- Implement DB repositories.
- Implement transaction wrapper.
- Implement ON CONFLICT helpers for tasks/outbox/results/DLQ.
- Implement deterministic task_id/idempotency_key generation.
```

Acceptance:

```text
fan-out can safely rerun without duplicate tasks
outbox insert is deduped by region_id + dedupe_key
```

Dependencies:

```text
B1 schema
```

---

### B3. User seed/import

Tasks:

```text
- Implement seed command for 50k users.
- Generate user_channels for email/sms/push/telegram.
- Add optional POST /users/bulk or CSV import for demo.
```

Acceptance:

```text
50k active users can be inserted quickly
user_channels are active and verified for selected channels
seed is repeatable/idempotent enough for local demos
```

Dependencies:

```text
B1 schema
```

---

### B4. Fan-out worker

Tasks:

```text
- Consume CampaignRegionRunRequested.
- Claim campaign_region_run with lock.
- Resolve recipients for all/user_ids/external_ids.
- Batch users 1000.
- Load active user_channels for selected channel codes.
- Snapshot recipient/channel/message.
- Insert delivery_tasks in batches.
- Insert DeliveryTaskCreated outbox events.
- Extend fanout_lock_until between batches.
- Mark fanout_completed.
```

Acceptance:

```text
campaign with 50k users and 2 channels creates around 100k tasks
killing fan-out worker mid-run and restarting does not create duplicates
fan-out writes stats total_tasks/queued only for newly inserted tasks
```

Dependencies:

```text
A3 campaign API
C3 outbox publisher
```

---

### B5. Stats shard service

Tasks:

```text
- Implement stats increment/decrement helper.
- Enforce updates only after successful state transition.
- Implement GET stats query.
- Add shard_id = hash(task_id) % 64.
```

Acceptance:

```text
stats match task counts after normal run
duplicate RabbitMQ signals do not double-count stats
stale worker finalization does not update stats
```

Dependencies:

```text
B1 schema
C4 delivery worker
```

---

### B6. Stats reconciliation job

Tasks:

```text
- Recompute stats for active/recent campaigns.
- Compare with campaign_stats_shards.
- Apply correction or log drift.
- Emit drift metric.
```

Acceptance:

```text
manual corruption of stats can be detected and corrected
job is bounded and does not scan all historical campaigns every run
```

Dependencies:

```text
B5 stats shard service
```

---

## 4.3 Person C — RabbitMQ + delivery/retry/DLQ/recovery

### C1. RabbitMQ topology

Tasks:

```text
- Define exchanges: notification.direct, notification.retry, notification.dlx.
- Define main queues for region `default` by queue_group, keeping naming template for future regions.
- Define retry buckets 30s/1m/5m/15m per queue_group.
- Define DLQ queues.
- Add durable queues, persistent messages, manual ack, prefetch.
- Add local RabbitMQ definitions file.
```

Acceptance:

```text
queues are created by docker-compose/bootstrap
messages route by notification.default.{queue_group}.{priority}
retry TTL routes back to main exchange
```

Dependencies:

```text
B1 channel queue_group schema
```

---

### C2. Outbox publisher

Tasks:

```text
- Poll outbox_events status=pending and transport_mode=rabbitmq_direct.
- Lock with SKIP LOCKED.
- Publish persistent message.
- Wait for publisher confirm.
- Mark published or retry with backoff.
- Implement publisher metrics.
```

Acceptance:

```text
outbox event reaches RabbitMQ
publisher restart does not lose event
published-to-RabbitMQ but DB-not-marked scenario results in duplicate signal, not data loss
```

Dependencies:

```text
B1 schema
C1 topology
```

---

### C3. Provider stub and adapter interface

Tasks:

```text
- Define ChannelAdapter interface.
- Implement GenericStubAdapter.
- Configurable success/transient/permanent rates.
- Random sleep between 2s and 300s.
- Respect context timeout.
- Return providerRequestId on success.
```

Acceptance:

```text
stub visibly waits random 2-300s
stub returns random success/error
provider_timeout=320s cancels overlong call
```

Dependencies:

```text
None
```

---

### C4. Delivery worker core

Tasks:

```text
- Consume DeliveryTaskCreated and TaskRetryScheduled.
- Load task by region/run/task.
- Atomic lease + delivery_attempt(started) transaction.
- Implement lease_token guard.
- Load live channel/regional config.
- Call adapter.
- Finalize attempt/task/result/stats/outbox in one transaction.
- Ack RabbitMQ after DB commit.
```

Acceptance:

```text
successful task creates delivery_attempt and delivery_result
duplicate RabbitMQ signal does not duplicate final result
stale lease finalization is ignored
worker crash before ack is recovered by idempotent processing
```

Dependencies:

```text
B1 schema
B5 stats helper
C1 topology
C3 stub
```

---

### C5. Lease heartbeat

Tasks:

```text
- Heartbeat every 30s during provider call.
- Extend lease_until by 120s.
- Stop heartbeat after provider returns or context cancels.
- If heartbeat affects zero rows, mark attempt stale when possible.
```

Acceptance:

```text
task with 300s stub call does not get picked by another worker while original worker is alive
killing worker stops heartbeat and lease recovery eventually reschedules task
```

Dependencies:

```text
C4 delivery worker
```

---

### C6. Retry logic

Tasks:

```text
- Implement retry decision by error type and attempt_count.
- Map attempts to 30s/1m/5m/15m buckets.
- Set delivery_tasks.available_at.
- Insert TaskRetryScheduled outbox event.
- Ensure worker checks available_at before send.
```

Acceptance:

```text
transient failures retry until max_attempts
retry message arriving early does not execute before available_at
retry task eventually succeeds or goes DLQ
```

Dependencies:

```text
C4 delivery worker
C2 outbox publisher
```

---

### C7. DLQ and replay execution

Tasks:

```text
- Move exhausted tasks to dead_lettered.
- Insert delivery_results final dead_lettered row.
- Insert dlq_items with reason/error/payload.
- Implement replay service called by API.
- Reset task state and insert outbox signal on replay.
```

Acceptance:

```text
exhausted task appears in GET /dlq
POST /dlq/replay can retry selected tasks
RabbitMQ DLQ is not required for product replay
```

Dependencies:

```text
A6 DLQ API
B5 stats helper
```

---

### C8. Recovery jobs

Tasks:

```text
- Outbox recovery: publishing lock expired -> pending.
- Lease recovery: sending lease expired -> retry_scheduled/dead_lettered.
- Retry scanner: retry_scheduled available_at <= now -> emit retry signal.
- Campaign finalizer: terminal campaign statuses.
- Optional fan-out recovery with Person B.
```

Acceptance:

```text
killed worker during send does not lose task
lost retry RabbitMQ signal is re-emitted by scanner
outbox publishing lock does not stay stuck forever
campaign eventually reaches completed/partially_failed/cancelled
```

Dependencies:

```text
B4 fan-out
C4 delivery worker
C6 retry
C7 DLQ
```

---

## 4.4 Person D — UI / QA / DevOps / Observability

### D1. Local environment

Tasks:

```text
- Docker Compose for PostgreSQL, RabbitMQ, API, workers.
- RabbitMQ management UI enabled.
- Makefile or task runner.
- .env.example.
- Seed script integration.
```

Acceptance:

```text
one command starts local stack
one command runs migrations
one command seeds channels and 50k users
```

Dependencies:

```text
B1 migrations
C1 RabbitMQ topology
```

---

### D2. Demo UI or CLI client

Tasks:

```text
- Create campaign form or CLI command.
- Select recipientSelector all/user_ids.
- Select channels.
- Show campaign stats.
- Show results/errors/DLQ pages or CLI outputs.
- Add channel enable/disable demo control if time allows.
```

Acceptance:

```text
non-backend reviewer can run demo scenario
results view shows recipient/channel/message/time
stats refresh without heavy DB count
```

Dependencies:

```text
A3/A4 APIs
A5 channel API
A6 DLQ API
```

---

### D3. Integration and E2E tests

Tasks:

```text
- Test POST /campaigns idempotency.
- Test selector all/user_ids/external_ids.
- Test fan-out creates expected tasks.
- Test delivery success path.
- Test retry then success path.
- Test exhausted retry -> DLQ.
- Test DLQ replay.
- Test worker kill/restart scenario.
- Test channel disable policy.
```

Acceptance:

```text
CI or local test command runs deterministic subset
E2E demo test can be run before presentation
```

Dependencies:

```text
A/B/C deliverables
```

---

### D4. Load and latency testing

Tasks:

```text
- Create load test for POST /campaigns.
- Seed 50k users.
- Run campaigns with 1, 2, 3 channels.
- Measure API p95.
- Measure fan-out throughput.
- Measure worker throughput under reduced stub latency mode.
```

Acceptance:

```text
p95 POST /campaigns < 1500 ms
fan-out does not block API
queue depth and outbox pending are visible
```

Dependencies:

```text
B3 seed
A3 campaign API
B4 fan-out
```

---

### D5. Observability and runbook

Tasks:

```text
- Add structured logs.
- Add basic Prometheus metrics or logs-as-metrics if time is limited.
- Dashboard/readme with key metrics.
- Runbook for stuck outbox, queue backlog, DLQ spike, stats drift.
```

Acceptance:

```text
demo can show outbox pending, RabbitMQ queue depth, successes/failures/retries/DLQ
README explains how to recover common failures
```

Dependencies:

```text
A/B/C metrics hooks
```

---

## 5. Cross-person integration contracts

### 5.1 Outbox event creation contract

Owned by B, consumed by C.

```text
region_id NOT NULL
for MVP, region_id is fixed to 'default' in runtime paths
dedupe_key unique per region
exchange/routing_key set by creator
payload includes messageType/version/eventId/regionId/campaignRegionRunId/dedupeKey
transport_mode = rabbitmq_direct for MVP
```

### 5.2 Task addressing contract

Used by B/C/A.

```text
region_id
campaign_region_run_id
task_id
```

Every worker message must include all three.

### 5.3 Stats update contract

Used by B/C/A.

```text
Stats can only change inside successful task transition transaction.
If task transition returns zero rows, stats must not change.
```

### 5.4 Result logging contract

Used by B/C/A/D.

```text
delivery_results must contain immutable recipient_address_snapshot, channel_code, message_snapshot, timestamps.
GET /campaigns/{id}/results must not reconstruct historical message/address from mutable tables.
```

### 5.5 Channel routing contract

Used by A/B/C.

```text
channel_code = product channel
queue_group = physical RabbitMQ routing group
routing_key = notification.{region}.{queue_group}.{priority}
payload contains both channelCode and queueGroup
MVP runtime routing uses notification.default.{queue_group}.{priority}
```

---

## 6. Suggested parallel work plan

### Day 1

| Person | Work |
|---|---|
| A | API skeleton, error envelope, campaign/channel contract stubs |
| B | DB migrations for core tables, seed channels with region_id='default' |
| C | RabbitMQ topology, outbox event struct, adapter interface skeleton |
| D | Docker Compose, Makefile, README skeleton |

Integration target:

```text
local stack starts; migrations run; health endpoint works
```

### Day 2

| Person | Work |
|---|---|
| A | Idempotency + POST /campaigns |
| B | users/user_channels seed 50k + repositories |
| C | outbox publisher with confirms |
| D | basic API client/demo script + CI/local test runner |

Integration target:

```text
POST /campaigns inserts campaign_region_run and outbox event; outbox publishes to RabbitMQ
```

### Day 3

| Person | Work |
|---|---|
| A | GET campaign/stats/tasks/results routes skeleton |
| B | fan-out worker with batch task/outbox insert |
| C | delivery worker core + stub provider simple path |
| D | E2E smoke test: campaign -> tasks -> success results with short stub latency mode |

Integration target:

```text
campaign creates tasks and at least some tasks reach succeeded
```

### Day 4

| Person | Work |
|---|---|
| A | channel config endpoints, DLQ API skeleton |
| B | stats shard service + fan-out idempotency hardening |
| C | atomic lease+attempt, heartbeat, retry buckets |
| D | load test for POST /campaigns, UI/CLI stats/results views |

Integration target:

```text
50k users x 2 channels fan-out works; retries occur; API p95 measured
```

### Day 5

| Person | Work |
|---|---|
| A | DLQ replay endpoint and validation polish |
| B | stats reconciliation + campaign finalizer |
| C | DLQ logic + recovery jobs |
| D | kill-worker test, demo script, observability/readme |

Integration target:

```text
acceptance scenario passes end-to-end
```

If timeline is shorter, reduce UI and use CLI/scripts, but do not cut source-of-truth DB, outbox, leases, snapshots, retry/DLQ, or idempotency.

---

## 7. Testing matrix

| Scenario | Owner | Priority |
|---|---|---|
| `POST /campaigns` returns `202` without fan-out | A/D | P0 |
| Same Idempotency-Key returns same response | A/D | P0 |
| Reused key with different body returns 409 | A/D | P0 |
| Selector `all` creates tasks for 50k users | B/D | P0 |
| Selector `user_ids` creates tasks only for selected users | A/B/D | P0 |
| New `whatsapp` channel with `queue_group=messenger` works | A/C/D | P0 |
| Worker success creates attempt/result snapshots | C/D | P0 |
| Stub sleeps random 2-300s in normal mode | C/D | P0 |
| Short-latency test mode works for CI | C/D | P0 |
| Worker killed during provider call recovers by lease recovery | C/D | P0 |
| Heartbeat prevents duplicate execution during long call | C/D | P0 |
| Transient error retries through bucket queue | C/D | P0 |
| Exhausted retry creates DB DLQ item | C/D | P0 |
| DLQ replay resets task and sends outbox signal | A/C/D | P0 |
| Stats match final task states after reconciliation | B/D | P0 |
| Channel disabled with `retry_later` reschedules | A/C/D | P1 |
| Channel disabled with `fail_fast` fails task | A/C/D | P1 |
| Campaign cancel stops not-started tasks | A/B/C/D | P1 |
| Outbox publisher restart causes no data loss | C/D | P0 |
| Fan-out worker restart causes no duplicate tasks | B/D | P0 |

---

## 8. Demo script

```text
1. Start local stack.
2. Run migrations.
3. Seed channels with region_id='default' defaults.
4. Seed 50k users with email/sms/telegram channels.
5. Add whatsapp via channel API with queue_group=messenger.
6. Create campaign recipientSelector=all channels=[email,sms].
7. Show POST /campaigns returns 202 quickly.
8. Show outbox events and RabbitMQ queue depth.
9. Show fan-out created ~100k tasks asynchronously.
10. Show stats changing: queued -> sending -> succeeded/retry/DLQ.
11. Kill one delivery worker during provider call.
12. Restart worker and run recovery.
13. Show no tasks lost; some retried or final.
14. Open results endpoint: recipient/channel/message/timestamps.
15. Create campaign with recipientSelector=user_ids for 3 users.
16. Disable telegram with retry_later and show worker behavior.
17. Force failures until DLQ.
18. Replay DLQ after changing stub success_rate.
19. Show final campaign status.
```

---

## 9. Definition of Done

### Product DoD

```text
Manager can send to all users.
Manager can send to selected users.
Manager can choose channels.
New channel can be added without schema/code changes for stub/generic adapter.
Manager can view stats/results/errors/DLQ.
```

### Reliability DoD

```text
Outbox prevents lost RabbitMQ publish after DB commit.
Workers use manual ack.
Delivery tasks use leases and lease_token.
Long provider calls use heartbeat.
Retries use DB available_at truth.
DLQ is persisted in DB.
Recovery jobs handle stuck outbox, expired leases and lost retry signals.
```

### Observability DoD

```text
API p95 is measured.
Fan-out throughput is measured.
Outbox pending age is visible.
RabbitMQ queue depth/unacked are visible.
Worker success/retry/DLQ counters are visible.
Stats drift is detected by reconciliation.
```

### Architecture DoD

```text
Hot tables have region_id and campaign_region_run_id where needed.
Outbox is partition-ready.
Message contracts include regionId and campaignRegionRunId.
queue_group is used for RabbitMQ routing.
Delivery results store immutable snapshots.
Semantics documented as at-least-once + idempotency.
Single-region acceptance is explicit: demo uses regionId=default and does not require multi-region rollout.
```

---

## 9.1 Migration note: what is postponed after MVP

```text
Already ready in MVP:
1. region_id/regionId is present in DB contracts, events and routing keys.
2. outbox/task dedupe keys are region-scoped and forward-compatible.
3. final wire contract already includes campaignRegionRunId and regionId.

Add later:
4. managed regions directory and multi-region config policy matrix.
5. operational multi-region rollout (DB split, RabbitMQ clusters, traffic isolation).
```

---

## 10. Risks and mitigations

| Risk | Impact | Mitigation | Owner |
|---|---|---|---|
| Stub 2-300s makes tests slow | Slow CI/demo | Add test profile with 20-200ms latency; keep normal profile for acceptance | C/D |
| Stats drift due to duplicate signals | Wrong dashboard | State transition guards + reconciliation job | B/C |
| Outbox grows too fast | DB pressure | Batch insert, publisher limits, cleanup job, monitor pending age | B/C |
| Queue explosion from dynamic channels | Ops complexity | `queue_group`, messenger shared queue, hot split later | A/C |
| Worker crash after provider send | Duplicate provider call | idempotency_key, provider idempotency where possible, at-least-once documented | C |
| Long provider call exceeds lease | Duplicate execution | heartbeat every 30s, lease extension 120s | C |
| Retry signal lost/delayed | Stuck retry task | retry recovery scanner based on DB available_at | C |
| Fan-out crash creates duplicates | Duplicate tasks/events | deterministic task_id and dedupe_key, ON CONFLICT DO NOTHING | B |
| p95 misses target | Failed requirement | Keep POST /campaigns small; load test; optimize validation and indexes | A/D |

---

## 11. Final handoff checklist

```text
[ ] README: how to start stack
[ ] README: how to run migrations
[ ] README: how to seed 50k users
[ ] README: how to create campaign
[ ] README: how to run workers/publisher/recovery
[ ] README: how to run demo script
[ ] OpenAPI or route examples committed
[ ] DB schema documented
[ ] RabbitMQ topology documented
[ ] Message contracts documented
[ ] Acceptance scenarios documented
[ ] Load test result for POST /campaigns p95 attached
[ ] Known limitations documented: at-least-once, local rate limiter, no Kafka in MVP
```
