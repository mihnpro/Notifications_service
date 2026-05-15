from pydantic import BaseModel, Field


class AuthConfig(BaseModel):
    jwt_secret: str = "dev-insecure-jwt-secret-change-me"  # noqa: S105
    jwt_ttl_seconds: int = Field(default=3600, ge=1)
