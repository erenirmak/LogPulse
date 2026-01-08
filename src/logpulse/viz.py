from pathlib import Path
from typing import List, Optional

import pandas as pd


class PulseVisualizer:
    """Professional visualization engine for LogPulse performance data."""

    def __init__(self, storage_path: str = "logs/perf_metrics.csv"):
        self.storage_path = Path(storage_path)
        if not self.storage_path.exists():
            raise FileNotFoundError(f"?? No logs found at {storage_path}")

    def _load_and_filter(self, tags: Optional[List[str]] = None) -> pd.DataFrame:
        df = pd.read_csv(self.storage_path)

        # MIGRATION LOGIC: Backwards compatibility for v0.1 users
        if "run_id" in df.columns and "session_run_id" not in df.columns:
            # If it's an old file, treat the global run_id as both for now
            df["global_run_id"] = df["run_id"]
            df["session_run_id"] = df.groupby("session_tag").cumcount() + 1

        if tags:
            df = df[df["session_tag"].isin(tags)]

        return df.sort_values("global_run_id")

    def _ensure_run_indices(self, df: pd.DataFrame) -> pd.DataFrame:
        if "session_run_id" not in df.columns:
            df = df.copy()
            df["session_run_id"] = df.groupby("session_tag").cumcount() + 1
        if "global_run_id" not in df.columns:
            df = df.copy()
            df["global_run_id"] = range(1, len(df) + 1)
        return df

    def _ensure_run_indices(self, df: pd.DataFrame) -> pd.DataFrame:
        if "session_run_id" not in df.columns:
            df = df.copy()
            df["session_run_id"] = df.groupby("session_tag").cumcount() + 1
        if "global_run_id" not in df.columns:
            df = df.copy()
            df["global_run_id"] = range(1, len(df) + 1)
        return df

    def plot_session(self, tag: str, start_idx: int = 0, end_idx: Optional[int] = None):
        """Visualizes a specific range of a session (Zoom-in)."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("? Run: pip install 'logpulse[viz]'")

        # 1. Load and Slice the data
        df = self._load_and_filter([tag])
        df = self._ensure_run_indices(df)
        df = df.sort_values("session_run_id").reset_index(drop=True)

        start_id = 1 if start_idx <= 0 else start_idx
        if end_idx:
            df_sliced = df[(df["session_run_id"] >= start_id) & (df["session_run_id"] <= end_idx)]
        else:
            df_sliced = df[df["session_run_id"] >= start_id]

        # 2. Plot the focused section
        plt.figure(figsize=(12, 5))
        sns.lineplot(data=df_sliced, x="session_run_id", y="duration_sec", color="#e67e22")

        plt.title(f"Zoomed View: {tag} (Runs {start_id} to {end_idx or 'End'})")
        plt.xlabel("Session Run ID")
        plt.ylabel("Latency (s)")

        # 3. Dynamic Y-Axis Scaling
        # Matplotlib auto-scales, but manual set_ylim can focus on specific ranges
        plt.grid(True, alpha=0.3)
        plt.show()

    def compare_sessions(self, tags: Optional[List[str]] = None):
        """Compares multiple sessions side-by-side using session-scoped IDs."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("❌ Run: pip install 'logpulse[viz]'")

        df = self._load_and_filter(tags)

        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # 1. Timeline Comparison (Normalized by Session-ID)
        # We use the NEW column you added to the CSV!
        sns.lineplot(ax=axes[0], data=df, x="session_run_id", y="duration_sec", hue="session_tag")
        axes[0].set_title("Performance Trends (Scoped by Session)")
        axes[0].set_xlabel("Run Number within Session")

        # 2. Distribution Comparison (remains effective)
        sns.boxplot(ax=axes[1], data=df, x="session_tag", y="duration_sec")
        axes[1].set_title("Latency Distribution & Outliers")

        plt.tight_layout()
        plt.show()

    def plot_distribution(self, tags: Optional[List[str]] = None):
        """Detailed histogram to see 'clusters' of performance."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("? Run: pip install 'logpulse[viz]'")

        df = self._load_and_filter(tags)
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x="duration_sec", hue="session_tag", kde=True, element="step")
        plt.title("Latency Density (Distribution Shape)")
        plt.show()

    def plot_system_drift(self):
        """Visualizes latency trends across the entire history of the project."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("? Run: pip install 'logpulse[viz]'")

        df = self._load_and_filter()
        plt.figure(figsize=(12, 5))

        # Plotting against the GLOBAL id shows the chronological history
        sns.lineplot(data=df, x="global_run_id", y="duration_sec", color="gray", alpha=0.3)
        # Add a moving average to see the "Pulse" of the system

        window_size = min(len(df), 50)  # Use 50, or the whole dataset if smaller
        df["rolling_mean"] = df["duration_sec"].rolling(window=window_size, min_periods=1).mean()

        sns.lineplot(data=df, x="global_run_id", y="rolling_mean", color="red")

        plt.title("Global Performance Drift (All Sessions)")
        plt.xlabel("Total Cumulative Runs")
        plt.show()
