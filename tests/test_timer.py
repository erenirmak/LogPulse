import time
import pytest
from LogPulse import LogPulse 

def test_ns_precision():
    logger = LogPulse()
    with logger.measure("fast_op"):
        pass # Very fast operation
    
    assert logger.records[0]["duration_sec"] > 0
    assert isinstance(logger.records[0]["duration_sec"], float)

def test_exception_handling():
    timer = LogPulse()
    
    with pytest.raises(ValueError):
        with timer.measure("fail_test"):
            raise ValueError("Boom!")
            
    assert timer.records[0]["status"] == "ERROR: ValueError"

@pytest.mark.parametrize("iterations", [3])
def test_summary_generation(iterations):
    timer = LogPulse()
    for _ in range(iterations):
        with timer.measure("repeat"):
            time.sleep(0.01)
            
    summary = timer.get_summary()
    assert summary.loc["repeat", "count"] == iterations