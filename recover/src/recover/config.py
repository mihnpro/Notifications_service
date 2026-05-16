from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServerConfig(BaseModel):
    bind: str = "0.0.0.0:8080"

    @property
    def host(self) -> str:
        return self.bind.rsplit(":", 1)[0]

    @property
    def port(self) -> int:
        return int(self.bind.rsplit(":", 1)[1])


class DbConfig(BaseModel):
    url: str = "postgresql+asyncpg://notifications:notifications@localhost:5432/notifications"
    max_connections: int = 10
    acquire_timeout_ms: int = 5000


class RmqConfig(BaseModel):
    url: str = "amqp://guest:guest@localhost:5672/%2f"
    enabled: bool = False


class RegionsConfig(BaseModel):
    ids: list[str] = Field(default_factory=lambda: ["default"])


class JobConfig(BaseModel):
    enabled: bool = True
    batch_size: int
    idle_interval_ms: int
    busy_interval_ms: int
    mid_interval_ms: int
    concurrency: int = 8


class JobsConfig(BaseModel):
    outbox_recovery: JobConfig = JobConfig(
        batch_size=200, idle_interval_ms=5000, busy_interval_ms=50, mid_interval_ms=500, concurrency=1
    )
    lease_recovery: JobConfig = JobConfig(
        batch_size=200, idle_interval_ms=3000, busy_interval_ms=50, mid_interval_ms=500, concurrency=16
    )
    retry_scanner: JobConfig = JobConfig(
        batch_size=500, idle_interval_ms=5000, busy_interval_ms=100, mid_interval_ms=500, concurrency=8
    )
    campaign_finalizer: JobConfig = JobConfig(
        batch_size=50, idle_interval_ms=10000, busy_interval_ms=200, mid_interval_ms=1000, concurrency=4
    )
    outbox_failed_scanner: JobConfig = JobConfig(
        batch_size=100, idle_interval_ms=15000, busy_interval_ms=500, mid_interval_ms=2000, concurrency=4
    )


class RetryConfig(BaseModel):
    buckets_seconds: list[int] = Field(default_factory=lambda: [30, 60, 300, 900])
    repush_grace_seconds: int = 30
    force_retry_max_attempts_ceiling: int = 20


class TelemetryConfig(BaseModel):
    log_format: str = "json"
    log_level: str = "INFO"


class RecoverConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="RECOVER__",
        env_nested_delimiter="__",
        extra="ignore",
    )

    instance_id: str = "recover-local"
    server: ServerConfig = ServerConfig()
    db: DbConfig = DbConfig()
    rmq: RmqConfig = RmqConfig()
    regions: RegionsConfig = RegionsConfig()
    jobs: JobsConfig = JobsConfig()
    retry: RetryConfig = RetryConfig()
    telemetry: TelemetryConfig = TelemetryConfig()
