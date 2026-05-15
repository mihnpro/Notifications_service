from functools import lru_cache
from typing import ClassVar

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from notifications_api.infra.app import AppConfig
from notifications_api.infra.auth import AuthConfig
from notifications_api.infra.database import DatabaseConfig
from notifications_api.infra.features import FeatureFlagsConfig
from notifications_api.infra.limits import LimitsConfig
from notifications_api.infra.postgres import PostgresConfig


class GlobalConfig(BaseSettings):
    app_host: str = "0.0.0.0"  # noqa: S104
    app_port: int = Field(default=8000, ge=1, le=65535)
    app_debug: bool = False

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"  # noqa: S105
    postgres_db: str = "notifications"

    idempotency_ttl_seconds: int = Field(default=3600, ge=1)
    pagination_default_limit: int = Field(default=50, ge=1)
    pagination_max_limit: int = Field(default=200, ge=1)
    dlq_replay_max_limit: int = Field(default=1000, ge=1)
    dlq_replay_max_additional_attempts: int = Field(default=10, ge=0)
    users_bulk_max_batch: int = Field(default=1000, ge=1)

    auth_jwt_secret: str = "dev-insecure-jwt-secret-change-me"  # noqa: S105
    auth_jwt_ttl_seconds: int = Field(default=3600, ge=1)

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
    def database(self) -> DatabaseConfig:
        return DatabaseConfig(
            host=self.postgres_host,
            port=self.postgres_port,
            user=self.postgres_user,
            password=self.postgres_password,
            db=self.postgres_db,
        )

    @property
    def postgres(self) -> PostgresConfig:
        return self.database.to_postgres()

    @property
    def limits(self) -> LimitsConfig:
        return LimitsConfig(
            idempotency_ttl_seconds=self.idempotency_ttl_seconds,
            pagination_default_limit=self.pagination_default_limit,
            pagination_max_limit=self.pagination_max_limit,
            dlq_replay_max_limit=self.dlq_replay_max_limit,
            dlq_replay_max_additional_attempts=self.dlq_replay_max_additional_attempts,
            users_bulk_max_batch=self.users_bulk_max_batch,
        )

    @property
    def auth(self) -> AuthConfig:
        return AuthConfig(
            jwt_secret=self.auth_jwt_secret,
            jwt_ttl_seconds=self.auth_jwt_ttl_seconds,
        )

    @property
    def features(self) -> FeatureFlagsConfig:
        return FeatureFlagsConfig(
            operational_tail=self.feature_operational_tail,
            bc_eventual_mode=self.feature_bc_eventual_mode,
            dlq_replay_noop=self.feature_dlq_replay_noop,
        )

    @property
    def database_url(self) -> str:
        return self.database.url


@lru_cache(maxsize=1)
def get_settings() -> GlobalConfig:
    return GlobalConfig()
