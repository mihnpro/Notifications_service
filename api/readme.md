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
- `POST /auth/login`
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

Protected endpoints require:

- `Authorization: Bearer <access_token>`
- `Idempotency-Key: <client-key>`

Auth is now login/password based:

- passwords are stored as PBKDF2 hashes in `managers.password_hash`
- `POST /auth/login` issues signed JWT access tokens
- all protected endpoints validate JWT signature and expiration

Bootstrap manager example:

```bash
HASH=$(uv run python -c "from notifications_api.app.http.auth import hash_password; print(hash_password('change-me-now'))")
psql \"$DATABASE_URL\" -c \"INSERT INTO managers (login, password_hash) VALUES ('admin', '$HASH');\"
```

## Quick examples

Login example:

```bash
curl -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"login":"admin","password":"change-me-now"}'
```

Create campaign:

```bash
curl -X POST http://localhost:8000/campaigns \
  -H 'Authorization: Bearer <access_token>' \
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
  -H 'Authorization: Bearer <access_token>' \
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
  -H 'Authorization: Bearer <access_token>' \
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
API_URL=http://localhost:8000 AUTH_TOKEN='<jwt-from-/auth/login>' ./scripts/smoke/run_all.sh
```
