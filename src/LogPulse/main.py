import pandas as pd
import time
import os
from datetime import datetime
from functools import wraps
from typing import Optional, List, Dict

class LogPulse:
    """High-precision performance logger using nanosecond monotonic counters."""
    
    def __init__(self, storage_path: str = "logs/perf_metrics.csv"):
        self.storage_path = storage_path
        self.records: List[Dict] = []
        self._run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def measure(self, label: str):
        """Context manager for block timing."""
        return self._MeasureContext(self, label)

    def timeit(self, label: Optional[str] = None):
        """Decorator for function timing."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                name = label or func.__name__
                with self.measure(name):
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    def get_summary(self) -> pd.DataFrame:
        """Returns statistics (mean, min, max) for all measured labels."""
        if not self.records:
            return pd.DataFrame()
        df = pd.DataFrame(self.records)
        # Using label as grouping key
        return df.groupby("label")["duration_sec"].agg(["mean", "min", "max", "count"])

    def save(self, format: str = "csv"):
        """Save and flush the record buffer."""
        if not self.records: return
        
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        df = pd.DataFrame(self.records)
        
        if format.lower() == "csv":
            header = not os.path.exists(self.storage_path)
            df.to_csv(self.storage_path, mode="a", index=False, header=header, encoding="utf-8-sig")
        
        self.records = []

    class _MeasureContext:
        def __init__(self, parent, label):
            self.parent, self.label = parent, label
            self.start_ns = None

        def __enter__(self):
            self.start_ns = time.perf_counter_ns() # Nanosecond resolution
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            end_ns = time.perf_counter_ns()
            duration_s = (end_ns - self.start_ns) / 1_000_000_000 
            
            self.parent.records.append({
                "run_id": self.parent._run_id,
                "timestamp": datetime.now().isoformat(),
                "label": self.label,
                "duration_sec": round(duration_s, 9), 
                "status": "SUCCESS" if not exc_type else f"ERROR: {exc_type.__name__}"
            })
            return False