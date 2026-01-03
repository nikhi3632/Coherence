"""
Helpers adapted from router_skeleton_fastapi/app/utils.py
Includes RateLimiter, CircuitBreaker, retry decorator wrapper (uses tenacity), parallel execution helper, and generate_trace_id.
"""
import time
import asyncio
from typing import Dict, Callable, Any, Optional, List
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import uuid
import logging

logger = logging.getLogger("implementation.helpers")

class RateLimiter:
    def __init__(self, limit_per_second: int = 100, window_size: int = 60):
        self.limit_per_second = limit_per_second
        self.window_size = window_size
        self.windows: Dict[str, Dict[int, int]] = {}

    async def check_rate_limit(self, sender_id: str) -> bool:
        current_time = int(time.time())
        window_start = current_time - self.window_size
        if sender_id not in self.windows:
            self.windows[sender_id] = {}
        self.windows[sender_id] = {ts: count for ts, count in self.windows[sender_id].items() if ts >= window_start}
        total_requests = sum(self.windows[sender_id].values())
        if total_requests >= self.limit_per_second * self.window_size:
            logger.warning("Rate limit exceeded", extra={"sender_id": sender_id, "requests": total_requests})
            return False
        if current_time in self.windows[sender_id]:
            self.windows[sender_id][current_time] += 1
        else:
            self.windows[sender_id][current_time] = 1
        return True

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_time: int = 30):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failure_counts: Dict[str, int] = {}
        self.circuit_open_until: Dict[str, datetime] = {}

    def is_circuit_open(self, agent: str) -> bool:
        if agent in self.circuit_open_until:
            if datetime.now() < self.circuit_open_until[agent]:
                return True
            else:
                del self.circuit_open_until[agent]
                self.failure_counts[agent] = 0
        return False

    def record_success(self, agent: str) -> None:
        self.failure_counts[agent] = 0

    def record_failure(self, agent: str) -> bool:
        if agent not in self.failure_counts:
            self.failure_counts[agent] = 0
        self.failure_counts[agent] += 1
        if self.failure_counts[agent] >= self.failure_threshold:
            self.circuit_open_until[agent] = datetime.now() + timedelta(seconds=self.recovery_time)
            logger.warning("Circuit breaker tripped", extra={"agent": agent, "until": self.circuit_open_until[agent].isoformat()})
            return True
        return False

circuit_breaker = CircuitBreaker()
rate_limiter = RateLimiter()

def with_retry(max_attempts: int = 3, min_wait_ms: int = 100, max_wait_ms: int = 1000):
    def decorator(func):
        @retry(stop=stop_after_attempt(max_attempts), wait=wait_exponential(multiplier=min_wait_ms/1000, max=max_wait_ms/1000), retry=retry_if_exception_type((Exception,)), reraise=True)
        async def wrapper(*args, **kwargs):
            agent = kwargs.get('agent', 'unknown')
            try:
                logger.debug("Retry attempt starting", extra={"agent": agent})
                result = await func(*args, **kwargs)
                logger.debug("Retry attempt succeeded", extra={"agent": agent})
                return result
            except Exception as e:
                logger.warning("Retry attempt failed", extra={"agent": agent, "error": str(e)})
                raise
        return wrapper
    return decorator

async def execute_parallel(func: Callable, items: List[Any], max_concurrency: int = 10, timeout: float = 5.0) -> List[Any]:
    semaphore = asyncio.Semaphore(max_concurrency)
    async def _wrapped_func(item):
        async with semaphore:
            try:
                return await asyncio.wait_for(func(item), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Function execution timed out", extra={"item": str(item)})
                return None
            except Exception as e:
                logger.error("Error in parallel execution", extra={"error": str(e)})
                return None
    return await asyncio.gather(*(_wrapped_func(item) for item in items), return_exceptions=False)


def generate_trace_id() -> str:
    return uuid.uuid4().hex
