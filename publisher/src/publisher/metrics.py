from __future__ import annotations

import threading
from http.server import HTTPServer
from wsgiref.simple_server import WSGIServer, make_server

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    REGISTRY,
    generate_latest,
    make_wsgi_app,
)

RELAY_BATCHES = Counter(
    "publisher_relay_batches_total",
    "Relay poll iterations by outcome.",
    ["outcome"],  # ok, error
)

RELAY_PUBLISHED = Counter(
    "publisher_relay_published_total",
    "Outbox messages successfully published to RabbitMQ.",
)

RELAY_FAILED = Counter(
    "publisher_relay_failed_total",
    "Outbox messages permanently failed (max attempts exceeded).",
)

RELAY_RETRIED = Counter(
    "publisher_relay_retried_total",
    "Outbox messages scheduled for retry.",
)

RELAY_BATCH_SIZE = Histogram(
    "publisher_relay_batch_size",
    "Number of outbox rows claimed per relay batch.",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

RELAY_RECOVERED = Counter(
    "publisher_relay_recovered_stale_total",
    "Stale outbox locks recovered.",
)


def start_metrics_server(addr: str = ":9091") -> None:
    host, _, port_str = addr.rpartition(":")
    wsgi_app = make_wsgi_app(REGISTRY)
    server = make_server(host or "0.0.0.0", int(port_str), wsgi_app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
