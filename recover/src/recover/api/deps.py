from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine

from recover.config import RecoverConfig
from recover.state import Registry


@dataclass
class AppState:
    cfg: RecoverConfig
    engine: AsyncEngine
    registry: Registry
    rmq_ready: bool = False
