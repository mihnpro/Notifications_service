"""Endpoints + credentials. Defaults match deploy/docker-compose.yml; override via env."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Env:
    api_url: str = os.environ.get("LT_API_URL", "http://localhost:8000")
    prom_url: str = os.environ.get("LT_PROM_URL", "http://localhost:9090")
    pg_dsn: str = os.environ.get(
        "LT_PG_DSN", "postgres://notifications:notifications@localhost:5432/notifications"
    )
    manager_login: str = os.environ.get("LT_MANAGER_LOGIN", "admin")
    manager_password: str = os.environ.get("LT_MANAGER_PASSWORD", "change-me-now")
    region: str = os.environ.get("LT_REGION", "default")
    total_users: int = int(os.environ.get("LT_TOTAL_USERS", "50000"))
    completion_timeout_s: int = int(os.environ.get("LT_COMPLETION_TIMEOUT_S", "300"))
    poll_interval_s: float = float(os.environ.get("LT_POLL_INTERVAL_S", "1.0"))
    compose_dir: str = os.environ.get("LT_COMPOSE_DIR", os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "deploy")
    ))


ENV = Env()
