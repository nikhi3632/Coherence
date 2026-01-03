"""
Redis sliding window helper adapted from signal_service_trial/integrity_service main.py
Provides a small interface for zset-based sliding windows used for anomaly detection and rate counting.
"""
from typing import Any


class InMemorySlidingWindow:
    def __init__(self):
        # map key -> list of timestamps
        self.windows = {}

    def add(self, key: str, ts: float):
        if key not in self.windows:
            self.windows[key] = []
        self.windows[key].append(ts)

    def expire_older_than(self, key: str, cutoff: float):
        if key not in self.windows:
            return
        self.windows[key] = [t for t in self.windows[key] if t >= cutoff]

    def count(self, key: str) -> int:
        return len(self.windows.get(key, []))

    def earliest(self, key: str):
        lst = self.windows.get(key, [])
        return min(lst) if lst else None
