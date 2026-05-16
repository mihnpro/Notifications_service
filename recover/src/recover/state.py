from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock


@dataclass
class JobRun:
    last_ran_at_monotonic: float
    last_rows: int
    last_outcome: str
    leader: bool


@dataclass
class JobSnapshot:
    name: str
    last_run_ms_ago: int
    last_rows: int
    last_outcome: str
    leader: bool

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "last_run_ms_ago": self.last_run_ms_ago,
            "last_rows": self.last_rows,
            "last_outcome": self.last_outcome,
            "leader": self.leader,
        }


class Registry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._jobs: dict[str, JobRun] = {}

    def record(self, name: str, rows: int, outcome: str, leader: bool) -> None:
        with self._lock:
            self._jobs[name] = JobRun(
                last_ran_at_monotonic=time.monotonic(),
                last_rows=rows,
                last_outcome=outcome,
                leader=leader,
            )

    def snapshot(self) -> list[JobSnapshot]:
        now = time.monotonic()
        with self._lock:
            return [
                JobSnapshot(
                    name=name,
                    last_run_ms_ago=int((now - run.last_ran_at_monotonic) * 1000),
                    last_rows=run.last_rows,
                    last_outcome=run.last_outcome,
                    leader=run.leader,
                )
                for name, run in self._jobs.items()
            ]
