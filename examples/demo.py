import random
import time
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from logpulse import LogPulse

LOG_PATH = "logs/demo_metrics.csv"


def _cpu_work(tag: str, n: int) -> int:
    tracker = LogPulse(storage_path=LOG_PATH, session_tag=tag)

    @tracker.timeit("cpu-heavy_computation")
    def process_data(size: int):
        print(f"?? [{tag}] heavy computation with n={size}")
        total = sum(i * i for i in range(size))
        time.sleep(0.15)
        return total

    result = process_data(n)

    tracker.get_summary()
    tracker.save()
    return result


def _io_mix(tag: str) -> None:
    tracker = LogPulse(storage_path=LOG_PATH, session_tag=tag)

    with tracker.measure("db-select"):
        time.sleep(random.uniform(0.05, 0.2))
        print(f"?? [{tag}] db select complete")

    with tracker.measure("db-write"):
        time.sleep(random.uniform(0.08, 0.25))
        print(f"?? [{tag}] db write complete")

    with tracker.measure("cache-hit"):
        time.sleep(random.uniform(0.01, 0.05))

    with tracker.measure("cache-miss"):
        time.sleep(random.uniform(0.04, 0.12))

    with tracker.measure("api-request"):
        if random.random() > 0.85:
            print(f"? [{tag}] API failed")
            raise ConnectionError("Server unreachable")
        time.sleep(random.uniform(0.08, 0.18))
        print(f"?? [{tag}] API response received")

    tracker.get_summary()
    tracker.save()


def _serialization(tag: str, payload_kb: int) -> None:
    tracker = LogPulse(storage_path=LOG_PATH, session_tag=tag)
    with tracker.measure("serialize-json"):
        time.sleep(random.uniform(0.02, 0.05))
    with tracker.measure("serialize-protobuf"):
        time.sleep(random.uniform(0.01, 0.03))
    with tracker.measure("deserialize-json"):
        time.sleep(random.uniform(0.02, 0.05))
    with tracker.measure("deserialize-protobuf"):
        time.sleep(random.uniform(0.01, 0.03))

    tracker.get_summary()
    tracker.save()


def _batch_run(tag: str, n_values: Iterable[int]) -> None:
    for n in n_values:
        _cpu_work(tag, n)
        try:
            _io_mix(tag)
        except ConnectionError:
            pass


def _run_scenarios(plan: List[Tuple[str, Dict]]) -> None:
    for tag, options in plan:
        print(f"\n== Running session: {tag} ==")
        _batch_run(tag, options["n_values"])
        for _ in range(options["serialization_runs"]):
            _serialization(tag, options["payload_kb"])


if __name__ == "__main__":
    random.seed(7)

    scenarios = [
        ("gpt-4o", {"n_values": [150_000, 250_000], "payload_kb": 32, "serialization_runs": 2}),
        ("claude-3.5", {"n_values": [100_000, 180_000, 220_000], "payload_kb": 24, "serialization_runs": 3}),
        ("local-llama", {"n_values": [80_000, 120_000], "payload_kb": 16, "serialization_runs": 4}),
        ("edge-device", {"n_values": [40_000, 60_000], "payload_kb": 8, "serialization_runs": 2}),
    ]

    _run_scenarios(scenarios)

    print("\n--- Performance Summary (last session only) ---")

    last_tag = scenarios[-1][0]
    df = pd.read_csv(LOG_PATH)
    summary = (
        df[df["session_tag"] == last_tag].groupby("label")["duration_sec"].agg(["mean", "min", "max", "count"])
    )
    print(summary)

    print(f"\n? Metrics saved to: {LOG_PATH}")
