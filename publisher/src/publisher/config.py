from __future__ import annotations

import socket

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PublisherConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pg_dsn: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/notifications",
        description="SQLAlchemy async DSN (postgresql+asyncpg://...)",
    )
    pg_pool_min_size: int = 1
    pg_pool_max_size: int = 10

    rabbitmq_url: str = Field(
        default="amqp://notifications:notifications@localhost:5672/%2Fnotifications",
    )

    batch_size: int = 100
    poll_interval_sec: float = 1.0
    lock_ttl_sec: int = 60
    max_attempts: int = 10
    publish_timeout_sec: float = 10.0
    recover_every_n_ticks: int = 30

    worker_id: str = Field(default_factory=socket.gethostname)
    log_level: str = "INFO"
    shutdown_grace_sec: float = 15.0
    metrics_addr: str = ":9091"
