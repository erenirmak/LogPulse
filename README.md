# 📉 LogPulse

**High-resolution performance logging for Python with persistent session tracking.**

LogPulse is designed for developers who need to monitor execution latency across multiple, independent script runs—perfect for benchmarking RAG systems, AI Agents, and ephemeral cloud functions.

## ✨ Features

* **⏱️ Nanosecond Precision**: Uses `time.perf_counter_ns()` for the highest possible accuracy.
* **💾 Persistent Sessions**: Automatically tracks run IDs across script restarts using a local state.
* **🏷️ Session Tagging**: Group your runs (e.g., `gpt-4o-test`, `v1-prompt`) for easy A/B comparison.
* **📊 Built-in Visualization**: One-line plotting for latency trends, distributions, and boxplots.
* **⚡ Lightweight**: Zero-dependency core (Pandas only used for export/stats).

---

## 🚀 Installation

Install the core library:
```bash
pip install logpulse
```

Or include the visualization suite:
```Bash
pip install "logpulse[viz]"
```
## 📖 Quick Start
#### 1. Basic Logging
```Python

from logpulse import LogPulse

# Initialize (creates logs/perf_metrics.csv by default)
tracker = LogPulse(session_tag="experiment-alpha")

# Use as a decorator
@tracker.timeit("heavy_task")
def my_function():
    # ... your code ...
    pass

# Or as a context manager
with tracker.measure("database_query"):
    # ... code to measure ...
    pass

# Save results to disk
tracker.save()
```
#### 2. Visualize Your Progress

Because LogPulse remembers your previous runs, you can visualize trends across time:
```Python

from logpulse.viz import PulseVisualizer

viz = PulseVisualizer()

# Compare different session tags side-by-side
viz.compare_sessions(tags=["gpt-4o-test", "gpt-3.5-test"])

# View the latency distribution (Density Plot)
viz.plot_distribution()
```
## 🛠️ Advanced Usage
#### Persistent Run IDs

Unlike other loggers, LogPulse stores a global state in .logpulse_state.json. If you run your script 100 times in a row, the run_id will correctly increment from 1 to 100 in your CSV, allowing for true time-series analysis of ephemeral scripts.
#### Clearing History

To reset the global counter and start fresh:
```Python
tracker = LogPulse()
tracker.clear_history(delete_logs=True)
```
## 📊 Comparison Strategy

LogPulse is built for RAG Evaluation. Use different _session_tag_ values to compare:

- Different LLM Models (GPT-4 vs Claude 3.5)

- Different Chunk Sizes

- Different Embedding Models

## 📜 License

Distributed under the MIT License. See LICENSE for more information.
