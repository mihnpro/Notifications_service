#!/usr/bin/env sh
set -e

uv run alembic upgrade head
exec uv run uvicorn notifications_api.app.litestar:app --host "${APP_HOST:-0.0.0.0}" --port "${APP_PORT:-8000}"
