"""Smoke tests: проверяют, что сервис recover собирается и базовая логика
работает без поднятия Postgres/RabbitMQ. Запуск: `pytest tests/test_smoke.py`.
"""
from __future__ import annotations

import importlib

import pytest


def test_package_importable() -> None:
    mod = importlib.import_module("recover")
    assert mod is not None


@pytest.mark.parametrize(
    "module",
    [
        "recover.config",
        "recover.repo",
        "recover.retry_bucket",
        "recover.jobs.lease",
        "recover.jobs.retry_scanner",
        "recover.jobs.outbox",
        "recover.jobs.outbox_failed",
        "recover.jobs.finalizer",
        "recover.jobs.backlog",
        "recover.main",
        "recover.api",
        "recover.metrics",
    ],
)
def test_submodules_importable(module: str) -> None:
    assert importlib.import_module(module) is not None


def test_job_config_constructs() -> None:
    from recover.config import JobConfig

    cfg = JobConfig(
        batch_size=10,
        idle_interval_ms=1,
        busy_interval_ms=1,
        mid_interval_ms=1,
        concurrency=4,
    )
    assert cfg.enabled is True
    assert cfg.batch_size == 10
    assert cfg.concurrency == 4


def test_main_routing_key_format() -> None:
    from recover.retry_bucket import main_routing_key

    assert main_routing_key("default", "marketing", "high") == "notification.default.marketing.high"
    assert main_routing_key("eu", "tx", "low") == "notification.eu.tx.low"


@pytest.mark.parametrize(
    "attempt,buckets,exp_secs,exp_label",
    [
        (1, [30, 60, 300, 900], 30, "30s"),
        (2, [30, 60, 300, 900], 60, "1m"),
        (3, [30, 60, 300, 900], 300, "5m"),
        (4, [30, 60, 300, 900], 900, "15m"),
        (10, [30, 60, 300, 900], 900, "15m"),
        (0, [30, 60, 300, 900], 30, "30s"),
    ],
)
def test_retry_bucket_for_attempt(
    attempt: int, buckets: list[int], exp_secs: int, exp_label: str
) -> None:
    from recover.retry_bucket import for_attempt

    b = for_attempt(attempt, buckets)
    assert b.delay_seconds == exp_secs
    assert b.label == exp_label


def test_retry_bucket_falls_back_to_defaults_when_config_short() -> None:
    from recover.retry_bucket import for_attempt

    b = for_attempt(4, [30])
    assert b.delay_seconds == 900
    assert b.label == "15m"


def test_jobs_config_defaults_loadable() -> None:
    from recover.config import JobsConfig

    cfg = JobsConfig()
    assert cfg.lease_recovery.batch_size > 0
    assert cfg.outbox_recovery.batch_size > 0
    assert cfg.retry_scanner.batch_size > 0


def test_exchange_constant() -> None:
    from recover.retry_bucket import EXCHANGE_DIRECT

    assert EXCHANGE_DIRECT == "notification.direct"
