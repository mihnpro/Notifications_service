# Deploy

Production-oriented compose stack with frontend served by `nginx` and backend services in one Docker network.

## Topology

- `frontend` serves static UI on `http://<host>:${FRONTEND_PORT}`.
- UI calls API by absolute URL from `FRONTEND_VITE_API_BASE_URL` (for example `https://api.example.com`).
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
