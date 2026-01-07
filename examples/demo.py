import random
import time

from logpulse import LogPulse

# 1. Initialize the logger
# This will create a 'logs' directory and a CSV file
tracker = LogPulse(storage_path="logs/demo_metrics.csv")


# 2. Use as a Decorator
@tracker.timeit("heavy_computation")
def process_data(n: int):
    """Simulates a CPU-intensive task."""
    print(f"🚀 Starting task with n={n}...")
    total = sum(i * i for i in range(n))
    time.sleep(0.5)  # Simulate IO delay
    return total


# 3. Use as a Context Manager
def run_simulation():
    with tracker.measure("database_query"):
        # Simulating a random latency query
        time.sleep(random.uniform(0.1, 0.4))
        print("📥 Query complete.")

    with tracker.measure("api_request"):
        # Simulating an API call that might fail
        if random.random() > 0.8:
            print("❌ API failed!")
            raise ConnectionError("Server unreachable")
        time.sleep(0.2)
        print("📡 API response received.")


# --- Execution ---
if __name__ == "__main__":
    # Run the decorated function
    process_data(10**6)

    # Run the simulation multiple times to get statistics
    for i in range(3):
        try:
            run_simulation()
        except ConnectionError:
            pass

    # 4. Show the Summary in the terminal
    print("\n--- Performance Summary ---")
    summary = tracker.get_summary()
    print(summary)

    # 5. Persist to disk
    tracker.save()
    print(f"\n✅ Metrics saved to: {tracker.storage_path}")
