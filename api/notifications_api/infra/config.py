from functools import lru_cache
from typing import ClassVar

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from notifications_api.infra.postgres import PostgresConfig


class AppConfig(BaseModel):
    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8000
    debug: bool = False


class GlobalConfig(BaseSettings):
    app_host: str = "0.0.0.0"  # noqa: S104
    app_port: int = 8000
    app_debug: bool = True

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"  # noqa: S105
    postgres_db: str = "notifications"
    idempotency_ttl_seconds: int = 3600
    pagination_default_limit: int = 50
    pagination_max_limit: int = 200
    dlq_replay_max_limit: int = 1000
    dlq_replay_max_additional_attempts: int = 10
    users_bulk_max_batch: int = 1000
    feature_operational_tail: bool = True
    feature_bc_eventual_mode: bool = True
    feature_dlq_replay_noop: bool = False

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def load(cls) -> "GlobalConfig":
        return get_settings()

    @property
    def app(self) -> AppConfig:
        return AppConfig(host=self.app_host, port=self.app_port, debug=self.app_debug)

    @property
    def postgres(self) -> PostgresConfig:
        return PostgresConfig(
            host=self.postgres_host,
            port=self.postgres_port,
            username=self.postgres_user,
            password=self.postgres_password,
            database=self.postgres_db,
        )

    @property
    def database_url(self) -> str:
        return self.postgres.url


@lru_cache
def get_settings() -> GlobalConfig:
    return GlobalConfig()
