"""Compose-driven chaos: stop/start services mid-run to measure loss + recovery."""
from __future__ import annotations

import asyncio
import subprocess
import time
from dataclasses import dataclass

from loadtests_lib.env import ENV


@dataclass
class ChaosEvent:
    service: str
    down_at: float
    up_at: float

    @property
    def downtime_s(self) -> float:
        return self.up_at - self.down_at


def _compose(*args: str) -> list[str]:
    return ["docker", "compose", "-f", f"{ENV.compose_dir}/docker-compose.yml", *args]


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


async def stop_service(service: str) -> None:
    await asyncio.to_thread(_run, _compose("stop", service))


async def start_service(service: str) -> None:
    await asyncio.to_thread(_run, _compose("start", service))


async def downtime_window(service: str, downtime_s: float) -> ChaosEvent:
    """Stop a service, wait, start it. Returns the event so callers can correlate."""
    down_at = time.time()
    await stop_service(service)
    await asyncio.sleep(downtime_s)
    await start_service(service)
    return ChaosEvent(service=service, down_at=down_at, up_at=time.time())


async def schedule_chaos(service: str, after_s: float, downtime_s: float) -> ChaosEvent:
    """Wait `after_s` seconds after a test starts, then kill the service for `downtime_s`."""
    await asyncio.sleep(after_s)
    return await downtime_window(service, downtime_s)
