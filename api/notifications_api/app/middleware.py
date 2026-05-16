from __future__ import annotations

import time
from typing import TYPE_CHECKING

from litestar.middleware.base import AbstractMiddleware
from litestar.types import ASGIApp, Receive, Scope, Send

from notifications_api.infra.metrics import REQUEST_COUNT, REQUEST_DURATION

if TYPE_CHECKING:
    pass


class PrometheusMiddleware(AbstractMiddleware):
    exclude = ["/metrics", "/healthz", "/readyz", "/health"]

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        method: str = scope.get("method", "")

        status_code = 500
        start = time.perf_counter()

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            REQUEST_COUNT.labels(method=method, path=path, status_code=str(status_code)).inc()
            REQUEST_DURATION.labels(method=method, path=path).observe(duration)
