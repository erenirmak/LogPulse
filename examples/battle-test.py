import os

import numpy as np
import pandas as pd

from logpulse.viz import PulseVisualizer


def generate_battle_data(path="logs/battle_test.csv"):
    os.makedirs("logs", exist_ok=True)
    data = []

    # Session 1: The "Stable Veteran" (GPT-3.5)
    # 5,000 runs, very consistent, low latency.
    for i in range(1, 5001):
        data.append(
            {
                "run_id": i,
                "session_tag": "gpt-3.5-stable",
                "duration_sec": np.random.normal(0.4, 0.05),  # Mean 0.4s, low jitter
                "status": "SUCCESS",
            }
        )

    # Session 2: The "Spiky Powerhouse" (GPT-4o)
    # 2,000 runs, faster mean but massive outliers (cold starts).
    for i in range(5001, 7001):
        latency = np.random.normal(0.3, 0.02)
        if i % 50 == 0:
            latency += 2.5  # Simulate a heavy outlier every 50 runs
        data.append(
            {"run_id": i, "session_tag": "gpt-4o-spiky", "duration_sec": max(0.1, latency), "status": "SUCCESS"}
        )

    # Session 3: The "Degrading Agent" (Local LLM)
    # 1,500 runs, starts fast but gets slower over time (memory leak simulation).
    for i in range(7001, 8501):
        leak_factor = (i - 7000) * 0.0005
        data.append(
            {
                "run_id": i,
                "session_tag": "local-llm-leak",
                "duration_sec": np.random.normal(0.5 + leak_factor, 0.1),
                "status": "SUCCESS",
            }
        )

    pd.DataFrame(data).to_csv(path, index=False)
    print(f"🔥 Battle-test data generated: {len(data)} rows.")


# --- The Battle Test ---
if __name__ == "__main__":
    generate_battle_data()

    viz = PulseVisualizer(storage_path="logs/battle_test.csv")

    print("📈 Testing Multi-Session Comparison...")
    viz.compare_sessions()  # This should show the boxplot vs line chart

    print("📊 Testing Distribution Shape...")
    viz.plot_distribution()  # This should show the "Leaking" vs "Stable" shapes
