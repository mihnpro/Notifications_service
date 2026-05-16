from __future__ import annotations

import logging

import structlog


def configure_logging(level: str, fmt: str = "json") -> None:
    lvl = getattr(logging, level.upper().split(",")[0], logging.INFO)
    logging.basicConfig(format="%(message)s", level=lvl)
    renderer = structlog.processors.JSONRenderer() if fmt == "json" else structlog.dev.ConsoleRenderer()
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(lvl),
    )
