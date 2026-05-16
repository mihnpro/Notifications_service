# Deploy

Production-oriented compose stack with frontend served by `nginx` and backend services in one Docker network.

## Topology

- `frontend` serves static UI on `http://<host>:${FRONTEND_PORT}`.
- `api` is published on `http://<host>:${API_PORT}` (default `8000`) for direct backend access and smoke scripts.
- `recovery` is published on `http://<host>:${RECOVERY_PORT}` (default `8081`) for recovery jobs API/metrics.
- UI calls API by absolute URL from `FRONTEND_VITE_API_BASE_URL` (for example `http://localhost:8000`).
- TLS is expected to terminate outside of this stack (LB/ingress/reverse proxy).

## Run

```bash
cd Notifications_service/deploy
cp .env.example .env
docker compose up --build -d
```

## Optional API gateway profile

If you need the legacy API-only nginx gateway (proxy to `api`), run:

```bash
docker compose --profile api-gateway up --build -d
```

It will expose `API_GATEWAY_PORT` (default `8080`).

## Smoke scripts

Smoke scripts from `api/scripts/smoke` must target the API service, not frontend nginx on port `80`.

For this deploy stack, run:

```bash
API_URL=http://localhost:8000 AUTH_TOKEN='<jwt>' ../api/scripts/smoke/run_all.sh
```
