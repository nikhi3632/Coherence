"""
History storage adapted from Signal-Router-Service/history_storage.py
Provides a small in-memory store with a pluggable backend to persist per-user history.
"""
from typing import Dict, Any, List, Optional
import os
import json


class InMemoryHistory:
    def __init__(self):
        self.logs: List[Dict[str, Any]] = []

    def write_log(self, log_entry: Dict[str, Any]):
        self.logs.append(log_entry)

    def get_all_logs(self) -> List[Dict[str, Any]]:
        return list(self.logs)

    def get_last_log(self) -> Optional[Dict[str, Any]]:
        return self.logs[-1] if self.logs else None


class HistoryStorage:
    def __init__(self, backend: str = "memory"):
        self.backend = backend
        if backend == "memory":
            self.store = InMemoryHistory()
        else:
            # Placeholder for DB backend (to be implemented)
            self.store = InMemoryHistory()

    def write_log(self, log_entry: Dict[str, Any]):
        # Could add encryption here if required
        self.store.write_log(log_entry)

    def get_all_logs(self) -> List[Dict[str, Any]]:
        return self.store.get_all_logs()

    def get_last_log(self) -> Optional[Dict[str, Any]]:
        return self.store.get_last_log()

    def export_to_file(self, path: str):
        with open(path, "w") as f:
            json.dump(self.get_all_logs(), f, indent=2)
