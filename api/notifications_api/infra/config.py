from functools import lru_cache
from typing import ClassVar

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppConfig(BaseModel):
    host: str = "0.0.0.0"  # noqa: S104
    port: int = 8000
    debug: bool = False


class PostgresConfig(BaseModel):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    username: str = Field(default="postgres")
    password: str = Field(default="postgres")
    database: str = Field(default="notifications")

    should_log_sql: bool | None = Field(default=False)
    pool_size: int = Field(default=10)
    pool_max_overflow: int | None = Field(default=20)

    @property
    def url(self) -> str:
        return f"postgresql+asyncpg://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class GlobalConfig(BaseSettings):
    app_host: str = "0.0.0.0"  # noqa: S104
    app_port: int = 8000
    app_debug: bool = False

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"  # noqa: S105
    postgres_db: str = "notifications"

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
