import time
from pathlib import Path

import pandas as pd
import pytest

from logpulse import LogPulse


@pytest.fixture(autouse=True)
def cleanup_logs():
    """Implicitly test the 'Nuclear' clear_history to reset the tool."""
    # We initialize a temporary logger just to perform the global reset
    logger = LogPulse()
    # Nuclear option: reset global counter + delete inventory-tracked CSVs
    logger.clear_history(session_only=False, delete_logs=True)
    yield
    # Ensure no artifacts remain for the next test run
    logger.clear_history(session_only=False, delete_logs=True)


def test_ns_precision():
    """Verify high-resolution timing is captured."""
    logger = LogPulse()
    with logger.measure("fast_op"):
        pass

    assert logger.records[0]["duration_sec"] > 0
    assert isinstance(logger.records[0]["duration_sec"], float)


def test_exception_handling():
    """Ensure errors are caught and logged without stopping execution."""
    logger = LogPulse()
    with pytest.raises(ValueError):
        with logger.measure("fail_test"):
            raise ValueError("Boom!")

    assert logger.records[0]["status"] == "ERROR: ValueError"


def test_summary_generation():
    """Verify that get_summary uses machine-readable column names."""
    iterations = 3
    logger = LogPulse()
    for _ in range(iterations):
        with logger.measure("repeat"):
            time.sleep(0.01)

    summary = logger.get_summary(auto_print=False)
    # Testing for 'count' (raw pandas) vs 'Runs' (display name)
    assert summary.loc["repeat", "Runs"] == iterations


def test_dual_id_persistence():
    """Test the core v0.2.0 feature: Global vs Session ID tracking."""
    # 1. First session (Starts at 1/1)
    s1 = LogPulse(session_tag="alpha")
    with s1.measure("task"):
        pass
    s1.save()
    assert s1.global_run_id == 1
    assert s1.session_run_id == 1

    # 2. Second session (Global 2, Session 1)
    s2 = LogPulse(session_tag="beta")
    with s2.measure("task"):
        pass
    s2.save()
    assert s2.global_run_id == 2
    assert s2.session_run_id == 1

    # 3. Resume first session (Global 3, Session 2)
    s3 = LogPulse(session_tag="alpha")
    assert s3.global_run_id == 3
    assert s3.session_run_id == 2


def test_split_files_logic():
    """Verify that split_files=True creates a dedicated CSV."""
    tag = "isolated_test"
    logger = LogPulse(session_tag=tag, split_files=True)
    with logger.measure("test"):
        pass
    logger.save()

    expected_file = Path("logs") / f"{tag}.csv"
    assert expected_file.exists()


def test_history_clearing_surgical():
    """Ensure clear_history only deletes the specific session state."""
    # Setup two sessions
    LogPulse(session_tag="keep_me")._get_next_run_ids()
    l2 = LogPulse(session_tag="delete_me")
    l2._get_next_run_ids()

    # Delete one
    l2.clear_history(delete_logs=True)

    # Check JSON state
    state = l2._load_state()
    assert "delete_me" not in state["session_counters"]
    assert "keep_me" in state["session_counters"]


def test_legacy_migration_logic(tmp_path):
    """Simulate a v0.1.0 CSV and ensure LogPulse archives it."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    csv_file = log_dir / "perf_metrics.csv"

    # Create a fake v0.1.0 CSV with the old 'run_id' column
    old_df = pd.DataFrame([{"run_id": 1, "session_tag": "default", "duration_sec": 0.1}])
    old_df.to_csv(csv_file, index=False)

    # Initialize v0.2.0 logger
    logger = LogPulse(session_tag="default", split_files=False)
    with logger.measure("new_run"):
        pass
    logger.save()

    # Verify backup exists and new file is clean
    assert (log_dir / "perf_metrics.v1.bak").exists()
    new_df = pd.read_csv(csv_file)
    assert "global_run_id" in new_df.columns
