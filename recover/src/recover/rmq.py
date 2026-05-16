from __future__ import annotations

import aio_pika
from aio_pika.abc import AbstractRobustConnection

from recover.config import RmqConfig


async def connect(cfg: RmqConfig) -> AbstractRobustConnection:
    return await aio_pika.connect_robust(cfg.url)
