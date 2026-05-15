# Notifications API MVP

Minimal Litestar service with PostgreSQL, native DI, and an outbox table.

## Run locally (without Docker)

1. Install dependencies:
   ```bash
   uv sync --extra dev
   ```
2. Export environment variables (or copy from `.env.example`).
3. Run migrations:
   ```bash
   uv run alembic upgrade head
   ```
4. Start API:
   ```bash
   uv run uvicorn notifications_api.app.litestar:app --host 0.0.0.0 --port 8000
   ```

## Run with Docker

```bash
docker compose up --build
```

Health endpoint:

```bash
curl http://localhost:8000/health
```

## API surface (MVP)

Implemented endpoints:

- `GET /health`, `GET /healthz`, `GET /readyz`
- `POST /campaigns`
- `POST /campaigns/{campaign_id}/cancel`
- `GET /campaigns`
- `GET /campaigns/{campaign_id}`
- `GET /campaigns/{campaign_id}/stats`
- `GET /campaigns/{campaign_id}/tasks`
- `GET /campaigns/{campaign_id}/results`
- `GET /campaigns/{campaign_id}/errors`
- `GET /channels`
- `POST /channels`
- `PATCH /channels/{channel_id}`
- `POST /channels/{channel_id}/enable`
- `POST /channels/{channel_id}/disable`
- `GET /channels/{channel_id}/regional-configs`
- `PUT /channels/{channel_id}/regional-configs/{region_id}`
- `GET /dlq`
- `POST /dlq/replay`
- `POST /users/bulk`

Mutating endpoints require:

- `Authorization: Bearer <token>`
- `Idempotency-Key: <client-key>`

Auth is a temporary stub: API extracts `manager_id` from Bearer token without signature verification.

## Quick examples

Create campaign:

```bash
curl -X POST http://localhost:8000/campaigns \
  -H 'Authorization: Bearer 6f8b5af4-3ae0-4c10-8f6a-5f7467b3c2d0' \
  -H 'Idempotency-Key: camp-create-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"May promo",
    "regionIds":["default"],
    "message":{"subject":"Promo","body":"Hello"},
    "recipientSelector":{"type":"all"},
    "channels":["email"],
    "priority":"normal"
  }'
```

Replay DLQ:

```bash
curl -X POST http://localhost:8000/dlq/replay \
  -H 'Authorization: Bearer 6f8b5af4-3ae0-4c10-8f6a-5f7467b3c2d0' \
  -H 'Idempotency-Key: dlq-replay-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "regionId":"default",
    "filter":{"channel":"email"},
    "limit":100,
    "additionalAttempts":1,
    "reason":"provider recovered"
  }'
```

Bulk users import:

```bash
curl -X POST http://localhost:8000/users/bulk \
  -H 'Authorization: Bearer 6f8b5af4-3ae0-4c10-8f6a-5f7467b3c2d0' \
  -H 'Idempotency-Key: users-bulk-001' \
  -H 'Content-Type: application/json' \
  -d '{
    "mode":"skip_duplicates",
    "items":[
      {
        "externalId":"crm_1",
        "status":"active",
        "channels":[{"channel":"email","address":"u1@example.com"}]
      }
    ]
  }'
```

## Known limitations

- Single-region runtime only: `regionId=default`.
- Contract-tail endpoints (`stats/results/errors/tasks`) return `consistency="eventual"` and may be empty/partial.
- Delivery/fan-out/provider execution is not performed by API; API writes campaign/run/outbox only.

## Smoke scripts

Added grouped smoke scripts under `scripts/smoke`:

- `01_health.sh`
- `02_channels.sh`
- `03_campaigns.sh`
- `04_dlq.sh`
- `05_users.sh`
- `run_all.sh`

Run all:

```bash
cd Notifications_service/api
API_URL=http://localhost:8000 MANAGER_ID=11111111-1111-1111-1111-111111111111 ./scripts/smoke/run_all.sh
```
