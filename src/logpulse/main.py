import json
import os
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class LogPulse:
    """High-precision, persistent performance logger for cross-execution tracking."""

    def __init__(self, storage_path: str = "logs/perf_metrics.csv", session_tag: str = "default"):
        self.storage_path = Path(storage_path)
        self.state_path = self.storage_path.parent / ".logpulse_state.json"
        self.session_tag = session_tag
        self.records: List[Dict] = []

        # Initialize run_id from disk or start fresh
        self.run_id = self._get_next_run_id()
        self.start_timestamp = datetime.now().isoformat()

    def _get_next_run_id(self) -> int:
        os.makedirs(self.storage_path.parent, exist_ok=True)
        last_id = 0
        if self.state_path.exists():
            try:
                with open(self.state_path, "r") as f:
                    last_id = json.load(f).get("last_run_id", 0)
            except (json.JSONDecodeError, IOError):
                pass

        new_id = last_id + 1
        with open(self.state_path, "w") as f:
            json.dump({"last_run_id": new_id}, f)
        return new_id

    def clear_history(self, delete_logs: bool = False):
        """Resets run counter and optionally deletes log files."""
        if self.state_path.exists():
            os.remove(self.state_path)
        if delete_logs and self.storage_path.exists():
            os.remove(self.storage_path)
        print("🧹 LogPulse state reset complete.")

    def measure(self, label: str):
        return self._MeasureContext(self, label)

    def timeit(self, label: Optional[str] = None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                with self.measure(label or func.__name__):
                    return func(*args, **kwargs)

            return wrapper

        return decorator

    def get_summary(self) -> pd.DataFrame:
        """Restored: Returns statistics for current records in memory."""
        if not self.records:
            return pd.DataFrame()
        df = pd.DataFrame(self.records)
        # Statistics engine
        return df.groupby("label")["duration_sec"].agg(["mean", "min", "max", "count"])

    def save(self):
        """Appends memory records to the persistent CSV file."""
        if not self.records:
            return
        df = pd.DataFrame(self.records)
        header = not self.storage_path.exists()
        df.to_csv(self.storage_path, mode="a", index=False, header=header, encoding="utf-8-sig")
        self.records = []

    class _MeasureContext:
        def __init__(self, parent, label):
            self.parent, self.label = parent, label
            self.start_ns = None

        def __enter__(self):
            self.start_ns = time.perf_counter_ns()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            duration_s = (time.perf_counter_ns() - self.start_ns) / 1_000_000_000
            self.parent.records.append(
                {
                    "run_id": self.parent.run_id,
                    "session_tag": self.parent.session_tag,
                    "timestamp": self.parent.start_timestamp,
                    "label": self.label,
                    "duration_sec": round(duration_s, 9),
                    "status": "SUCCESS" if not exc_type else f"ERROR: {exc_type.__name__}",
                }
            )
            return False
