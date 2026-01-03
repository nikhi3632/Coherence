"""
Structured logging helper adapted from router_skeleton_fastapi/app/logging.py
Provides get_logger and TraceMiddleware compatible with FastAPI.
"""
import os
import logging
import structlog
from typing import Dict, Any

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)


def get_logger(name: str = "router"):
    return structlog.get_logger(name)


class TraceMiddleware:
    def __init__(self, app):
        self.app = app
        self.logger = get_logger("middleware")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        trace_id = None
        for key, value in scope.get("headers", []):
            if key.decode("latin1").lower() == "x-trace-id":
                trace_id = value.decode("latin1")
                break
        if not trace_id:
            from Implementation.router.app import generate_trace_id
            trace_id = generate_trace_id()
        ctx = structlog.contextvars.bind_contextvars(trace_id=trace_id)
        path = scope.get("path", "")
        method = scope.get("method", "")
        self.logger.info("request_start", path=path, method=method, trace_id=trace_id)
        start_time = __import__('time').time()
        try:
            await self.app(scope, receive, send)
        except Exception as e:
            self.logger.error("request_error", path=path, method=method, trace_id=trace_id, error=str(e), duration=__import__('time').time() - start_time)
            raise
        finally:
            self.logger.info("request_end", path=path, method=method, trace_id=trace_id, duration=__import__('time').time() - start_time)
            structlog.contextvars.clear_contextvars()
