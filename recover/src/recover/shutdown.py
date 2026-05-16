from __future__ import annotations

import asyncio
import signal


class Shutdown:
    """Single event flipped by SIGINT/SIGTERM; jobs poll `.is_set()` / await `.wait()`."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def install(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request)
            except NotImplementedError:
                pass

    def request(self) -> None:
        self._event.set()

    @property
    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    async def sleep(self, seconds: float) -> bool:
        """Sleep until `seconds` elapse OR shutdown fires. Returns True if shutdown fired."""
        if self._event.is_set():
            return True
        try:
            await asyncio.wait_for(self._event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False
