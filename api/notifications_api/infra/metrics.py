from __future__ import annotations

from prometheus_client import Counter, Histogram, REGISTRY, generate_latest, CONTENT_TYPE_LATEST

REQUEST_COUNT = Counter(
    "api_http_requests_total",
    "Total HTTP requests.",
    ["method", "path", "status_code"],
)

REQUEST_DURATION = Histogram(
    "api_http_request_duration_seconds",
    "HTTP request latency.",
    ["method", "path"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

CAMPAIGNS_CREATED = Counter(
    "api_campaigns_created_total",
    "Campaigns successfully created.",
)

CAMPAIGNS_CANCELLED = Counter(
    "api_campaigns_cancelled_total",
    "Campaigns cancelled.",
)


def metrics_response() -> tuple[bytes, str]:
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
