import asyncio
import random
import uuid
import os
from datetime import datetime
from fastapi import FastAPI, Request
from pydantic import BaseModel

app = FastAPI()

SUCCESS_RATE = float(os.getenv("SUCCESS_RATE", "0.7"))
MIN_DELAY = 2
MAX_DELAY = 300

class SendRequest(BaseModel):
    task_id: str
    recipient: str
    channel: str
    message: str

class SendResponse(BaseModel):
    status: str
    provider_request_id: str | None = None
    error_code: str | None = None

@app.post("/send", response_model=SendResponse)
async def send_notification(req: SendRequest):
    # случайная задержка
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    await asyncio.sleep(delay)

    # случайный исход
    if random.random() < SUCCESS_RATE:
        return SendResponse(
            status="ok",
            provider_request_id=str(uuid.uuid4())
        )
    else:
        error_code = "TRANSIENT_FAILURE" if random.random() < 0.5 else "PERMANENT_FAILURE"
        return SendResponse(
            status="error",
            error_code=error_code
        )

@app.get("/health")
async def health():
    return {"status": "ok"}