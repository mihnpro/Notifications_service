import asyncio
import os
import random
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

SUCCESS_RATE = float(os.getenv("SUCCESS_RATE", "0.7"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "2"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "300"))


class SendRequest(BaseModel):
    task_id: str
    recipient: str
    channel: str
    message: str


class SendResponse(BaseModel):
    status: str
    provider_request_id: str | None = None
    error_code: str | None = None

    request_received_at: str
    response_sent_at: str


@app.post("/send", response_model=SendResponse)
async def send_notification(req: SendRequest):
    request_received_at = datetime.now(timezone.utc)

    print(
        f"[REQUEST RECEIVED] "
        f"task_id={req.task_id}, "
        f"recipient={req.recipient}, "
        f"channel={req.channel}, "
        f"received_at={request_received_at.isoformat()}"
    )

    # случайная задержка
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)

    response_sent_at = datetime.now(timezone.utc)

    # случайный исход
    if random.random() < SUCCESS_RATE:
        response = SendResponse(
            status="ok",
            provider_request_id=str(uuid.uuid4()),
            request_received_at=request_received_at.isoformat(),
            response_sent_at=response_sent_at.isoformat(),
        )
    else:
        error_code = (
            "TRANSIENT_FAILURE" if random.random() < 0.5 else "PERMANENT_FAILURE"
        )

        response = SendResponse(
            status="error",
            error_code=error_code,
            request_received_at=request_received_at.isoformat(),
            response_sent_at=response_sent_at.isoformat(),
        )

    print(
        f"[RESPONSE SENT] "
        f"task_id={req.task_id}, "
        f"status={response.status}, "
        f"error_code={response.error_code}, "
        f"received_at={response.request_received_at}, "
        f"sent_at={response.response_sent_at}"
    )

    return response


@app.get("/health")
async def health():
    return {"status": "ok"}