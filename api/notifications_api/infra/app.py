from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    host: str = "0.0.0.0"  # noqa: S104
    port: int = Field(default=8000, ge=1, le=65535)
    debug: bool = False
