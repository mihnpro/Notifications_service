import asyncio
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, Response
from prometheus_client import Counter, Histogram, make_asgi_app
from pythonjsonlogger import jsonlogger
from pydantic import BaseModel

# ── Structured JSON logging ───────────────────────────────────────────────────

def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    ))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)

_configure_logging()
log = logging.getLogger("provider_mock")

# ── Prometheus metrics ────────────────────────────────────────────────────────

REQUESTS_TOTAL = Counter(
    "provider_mock_requests_total",
    "Total provider send requests by channel and outcome.",
    ["channel", "outcome"],  # outcome: success, transient, permanent
)

REQUEST_DURATION = Histogram(
    "provider_mock_request_duration_seconds",
    "Provider send handler latency.",
    ["channel"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# ── Config ────────────────────────────────────────────────────────────────────

SUCCESS_RATE = float(os.getenv("SUCCESS_RATE", "0.7"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.05"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.5"))

# ── App ──��────────────────────────────────────────────────────────────────────

app = FastAPI()

# Mount /metrics as a sub-application so it bypasses FastAPI middleware overhead.
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


class SendRequest(BaseModel):
    task_id: str
    recipient: str
    channel: str
    message: Any
    idempotency_key: str | None = None


class SendResponse(BaseModel):
    provider_request_id: str | None = None
    error_type: str | None = None
    error_code: str | None = None
    message: str | None = None
    request_received_at: str
    response_sent_at: str


@app.post("/send", response_model=SendResponse)
async def send_notification(req: SendRequest, response: Response):
    request_received_at = datetime.now(timezone.utc)

    log.info("request_received", extra={
        "task_id": req.task_id,
        "recipient": req.recipient,
        "channel": req.channel,
        "received_at": request_received_at.isoformat(),
    })

    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    start = asyncio.get_event_loop().time()
    await asyncio.sleep(delay)

    response_sent_at = datetime.now(timezone.utc)
    elapsed = asyncio.get_event_loop().time() - start
    REQUEST_DURATION.labels(channel=req.channel).observe(elapsed)

    if random.random() < SUCCESS_RATE:
        REQUESTS_TOTAL.labels(channel=req.channel, outcome="success").inc()
        log.info("response_sent", extra={"task_id": req.task_id, "status": "ok"})
        return SendResponse(
            provider_request_id=str(uuid.uuid4()),
            request_received_at=request_received_at.isoformat(),
            response_sent_at=response_sent_at.isoformat(),
        )

    is_transient = random.random() < 0.5
    error_type = "transient" if is_transient else "permanent"
    error_code = "TRANSIENT_FAILURE" if is_transient else "PERMANENT_FAILURE"
    response.status_code = 422 if is_transient else 400

    REQUESTS_TOTAL.labels(channel=req.channel, outcome=error_type).inc()
    log.warning("response_sent", extra={
        "task_id": req.task_id,
        "error_type": error_type,
        "error_code": error_code,
    })

    return SendResponse(
        error_type=error_type,
        error_code=error_code,
        message=f"simulated {error_type} failure",
        request_received_at=request_received_at.isoformat(),
        response_sent_at=response_sent_at.isoformat(),
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        access_log=False,  # structured logging replaces uvicorn access log
    )
