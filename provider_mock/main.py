import asyncio
import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, Response
from pydantic import BaseModel

app = FastAPI()

SUCCESS_RATE = float(os.getenv("SUCCESS_RATE", "0.7"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.05"))
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.5"))


class SendRequest(BaseModel):
    task_id: str
    recipient: str
    channel: str
    message: Any  # JSON object sent by the delivery worker
    idempotency_key: str | None = None


class SendResponse(BaseModel):
    provider_request_id: str | None = None
    error_type: str | None = None   # "transient" | "permanent" — read by HTTPAdapter
    error_code: str | None = None
    message: str | None = None
    request_received_at: str
    response_sent_at: str


@app.post("/send", response_model=SendResponse)
async def send_notification(req: SendRequest, response: Response):
    request_received_at = datetime.now(timezone.utc)

    print(
        f"[REQUEST RECEIVED] "
        f"task_id={req.task_id}, "
        f"recipient={req.recipient}, "
        f"channel={req.channel}, "
        f"received_at={request_received_at.isoformat()}"
    )

    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)

    response_sent_at = datetime.now(timezone.utc)

    if random.random() < SUCCESS_RATE:
        print(f"[RESPONSE SENT] task_id={req.task_id} status=ok")
        return SendResponse(
            provider_request_id=str(uuid.uuid4()),
            request_received_at=request_received_at.isoformat(),
            response_sent_at=response_sent_at.isoformat(),
        )

    is_transient = random.random() < 0.5
    error_type = "transient" if is_transient else "permanent"
    error_code = "TRANSIENT_FAILURE" if is_transient else "PERMANENT_FAILURE"
    # 422 → transient, 400 → permanent — matches HTTPAdapter error routing.
    response.status_code = 422 if is_transient else 400

    print(
        f"[RESPONSE SENT] task_id={req.task_id} "
        f"error_type={error_type} error_code={error_code}"
    )
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
    )
