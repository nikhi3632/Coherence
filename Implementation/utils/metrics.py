"""
Prometheus metrics adapted from router_skeleton_fastapi/app/metrics.py
Exports counters and a timer helper used by the router.
"""
from prometheus_client import Counter, Histogram, Gauge

ROUTER_INGRESS_TOTAL = Counter(
    "router_ingress_total",
    "Total number of requests received by router",
    ["type"]
)

ROUTER_LATENCY_SECONDS = Histogram(
    "router_latency_seconds",
    "Time taken for routing operations",
    ["operation", "kind"],
    buckets=[0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
)

ROUTER_DOWNSTREAM_SUCCESS_TOTAL = Counter(
    "router_downstream_success_total",
    "Total number of successful downstream calls",
    ["service"]
)

ROUTER_DOWNSTREAM_FAIL_TOTAL = Counter(
    "router_downstream_fail_total",
    "Total number of failed downstream calls",
    ["service", "reason"]
)

ROUTER_DLQ_TOTAL = Counter(
    "router_dlq_total",
    "Total number of items sent to DLQ",
    ["reason"]
)

DLQ_BACKLOG = Gauge(
    "dlq_backlog",
    "Current number of items in DLQ"
)

class TimerContextManager:
    def __init__(self, metric, labels=None):
        self.metric = metric
        self.labels = labels or {}
        self.start = None

    def __enter__(self):
        import time
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        if self.start:
            duration = time.perf_counter() - self.start
            self.metric.labels(**self.labels).observe(duration)


def timer(operation, kind="unknown"):
    return TimerContextManager(ROUTER_LATENCY_SECONDS, {"operation": operation, "kind": kind})
