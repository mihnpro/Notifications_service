from pydantic import BaseModel, Field

from notifications_api.infra.postgres import PostgresConfig


class DatabaseConfig(BaseModel):
    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    user: str = "postgres"
    password: str = "postgres"  # noqa: S105
    db: str = "notifications"

    def to_postgres(self) -> PostgresConfig:
        return PostgresConfig(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.db,
        )

    @property
    def url(self) -> str:
        return self.to_postgres().url
