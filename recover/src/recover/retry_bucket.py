from __future__ import annotations

from dataclasses import dataclass

EXCHANGE_DIRECT = "notification.direct"
_LABELS: tuple[str, ...] = ("30s", "1m", "5m", "15m")
_DEFAULT_SECONDS: tuple[int, ...] = (30, 60, 300, 900)


@dataclass(frozen=True)
class Bucket:
    delay_seconds: int
    label: str


def for_attempt(attempt_count: int, configured_seconds: list[int]) -> Bucket:
    idx = max(attempt_count, 1) - 1
    idx = min(idx, len(_LABELS) - 1)
    secs = configured_seconds[idx] if idx < len(configured_seconds) else _DEFAULT_SECONDS[idx]
    return Bucket(delay_seconds=int(secs), label=_LABELS[idx])


def main_routing_key(region: str, queue_group: str, priority: str) -> str:
    return f"notification.{region}.{queue_group}.{priority}"
