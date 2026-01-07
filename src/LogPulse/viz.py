import pandas as pd
from pathlib import Path
from typing import List, Optional

class PulseVisualizer:
    """Professional visualization engine for LogPulse performance data."""
    
    def __init__(self, storage_path: str = "logs/perf_metrics.csv"):
        self.storage_path = Path(storage_path)
        if not self.storage_path.exists():
            raise FileNotFoundError(f"🔍 No logs found at {storage_path}")

    def _load_and_filter(self, tags: Optional[List[str]] = None) -> pd.DataFrame:
        df = pd.read_csv(self.storage_path)
        if tags:
            df = df[df['session_tag'].isin(tags)]
        return df

    def plot_session(self, tag: str, start_idx: int = 0, end_idx: Optional[int] = None):
        """Visualizes a specific range of a session (Zoom-in)."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("❌ Run: pip install 'logpulse[viz]'")

        # 1. Load and Slice the data
        df = self._load_and_filter([tag])
        df = df.copy().reset_index(drop=True)
        
        # Apply the slice
        df_sliced = df.iloc[start_idx : end_idx] if end_idx else df.iloc[start_idx:]

        # 2. Plot the focused section
        plt.figure(figsize=(12, 5))
        sns.lineplot(data=df_sliced, x=df_sliced.index, y="duration_sec", color="#e67e22")
        
        plt.title(f"Zoomed View: {tag} (Runs {start_idx} to {end_idx or 'End'})")
        plt.xlabel("Local Run Index")
        plt.ylabel("Latency (s)")
        
        # 3. Dynamic Y-Axis Scaling
        # Matplotlib auto-scales, but manual set_ylim can focus on specific ranges
        plt.grid(True, alpha=0.3)
        plt.show()

    def compare_sessions(self, tags: Optional[List[str]] = None):
        """Compares multiple sessions with normalized X-axis for direct comparison."""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("❌ Run: pip install 'logpulse[viz]'")

        df = self._load_and_filter(tags)
        
        # KEY FIX: Normalize the X-Axis for each session
        # We create a new column 'run_index' that starts at 1 for every group
        df = df.copy()
        df['run_index'] = df.groupby('session_tag').cumcount() + 1

        fig, axes = plt.subplots(1, 2, figsize=(15, 6))

        # 1. Normalized Timeline Comparison
        sns.lineplot(ax=axes[0], data=df, x="run_index", y="duration_sec", hue="session_tag")
        axes[0].set_title("Performance Trends (Normalized X-Axis)")
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
            raise ImportError("❌ Run: pip install 'logpulse[viz]'")

        df = self._load_and_filter(tags)
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x="duration_sec", hue="session_tag", kde=True, element="step")
        plt.title("Latency Density (Distribution Shape)")
        plt.show()

    