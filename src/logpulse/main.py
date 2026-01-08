import json
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class LogPulse:
    """High-precision, persistent performance logger for cross-execution tracking."""

    def __init__(self, session_tag: str = "default", split_files: bool = False):
        self.log_dir = Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.session_tag = session_tag
        self.state_path = self.log_dir / ".logpulse_state.json"

        if split_files:
            self.storage_path = self.log_dir / f"{session_tag}.csv"
        else:
            self.storage_path = self.log_dir / "perf_metrics.csv"

        self.records: List[Dict] = []

        # Load state and check for storage fragmentation
        state = self._load_state()
        if state["session_counters"].get(self.session_tag, 0) > 0 and not self.storage_path.exists():
            print(
                f"ℹ️ LogPulse: Session '{self.session_tag}' has existing history, "
                f"but current config saves to a new file: {self.storage_path.name}"
            )

        self.global_run_id, self.session_run_id = self._get_next_run_ids()

    def _load_state(self) -> Dict:
        """Loads the global state. Migrates legacy v0.1.0 keys if found."""
        default_state = {"global_counter": 0, "session_counters": {}}
        if not self.state_path.exists():
            return default_state
        try:
            with open(self.state_path, "r") as f:
                state = json.load(f)
                if "global_counter" not in state:  # Legacy 0.1 migration
                    state = {"global_counter": state.get("last_run_id", 0), "session_counters": {}}
                return state
        except (json.JSONDecodeError, IOError):
            return default_state

    def _save_state(self, state: Dict) -> None:
        with open(self.state_path, "w") as f:
            json.dump(state, f, indent=4)

    def _get_next_run_ids(self) -> tuple[int, int]:
        state = self._load_state()
        state["global_counter"] = int(state.get("global_counter", 0)) + 1

        session_counters = state.get("session_counters", {})
        session_counters[self.session_tag] = int(session_counters.get(self.session_tag, 0)) + 1
        state["session_counters"] = session_counters

        self._save_state(state)
        return state["global_counter"], session_counters[self.session_tag]

    def clear_history(self, session_only: bool = True, delete_logs: bool = False):
        """
        Surgically clears history based on the state tracker inventory.
        """
        state = self._load_state()
        inventory = state.get("session_counters", {})

        if session_only:
            if self.session_tag in inventory:
                del inventory[self.session_tag]

            if delete_logs and self.storage_path.exists():
                self.storage_path.unlink()

            msg = f"✅ LogPulse: Cleared session '{self.session_tag}'."

        else:
            if delete_logs:
                main_file = self.log_dir / "perf_metrics.csv"
                if main_file.exists():
                    main_file.unlink()

                for tag in inventory.keys():
                    tag_file = self.log_dir / f"{tag}.csv"
                    if tag_file.exists():
                        tag_file.unlink()

                for bak in self.log_dir.glob("*.v1.bak"):
                    bak.unlink()

            state = {"global_counter": 0, "session_counters": {}}
            msg = "☢️ LogPulse: Inventory-based global reset complete. (User files preserved)."

        self._save_state(state)
        print(msg)

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

    def get_summary(self, auto_print: bool = True) -> pd.DataFrame:
        """
        Calculates descriptive statistics for the current in-memory records.
        Useful for a quick 'Post-Run' check before committing to disk.
        """
        if not self.records:
            if auto_print:
                print("📋 LogPulse: No records in memory to summarize.")
            return pd.DataFrame()

        df = pd.DataFrame(self.records)
        summary = (
            df.groupby("label")["duration_sec"]
            .agg(["mean", "min", "max", "count"])
            .rename(columns={"mean": "Avg (s)", "min": "Min (s)", "max": "Max (s)", "count": "Runs"})
        )

        if auto_print:  # later, maybe add formatting options and/or tr-100 machine report format
            print(f"\n📊 Summary for {self.session_tag}:")
            print(summary)
        return summary

    def save(self):
        """Saves memory records to CSV with a migration check for v0.1 compatibility."""
        if not self.records:
            return
        df = pd.DataFrame(self.records)

        if self.storage_path.exists():
            try:
                existing_cols = pd.read_csv(self.storage_path, nrows=0).columns.tolist()
                if "run_id" in existing_cols and "global_run_id" not in existing_cols:
                    backup = self.storage_path.with_suffix(".v1.bak")
                    self.storage_path.rename(backup)
                    print(f"⚠️ LogPulse: Legacy CSV detected. Archived legacy CSV to {backup}")
            except Exception:
                pass

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
                    "global_run_id": self.parent.global_run_id,
                    "session_run_id": self.parent.session_run_id,
                    "session_tag": self.parent.session_tag,
                    "timestamp": datetime.now().isoformat(),
                    "label": self.label,
                    "duration_sec": round(duration_s, 9),
                    "status": "SUCCESS" if not exc_type else f"ERROR: {exc_type.__name__}",
                }
            )
            return False  # Don't suppress exceptions
