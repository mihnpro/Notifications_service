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
