import pytest
import time
import threading
from unittest.mock import patch, MagicMock
from settlement_matcher import SettlementMatcher

def test_rate_limiter_thread_safety():
    """
    Test that the rate limiter in SettlementMatcher is thread-safe
    and enforces the rate limit across multiple threads.
    """
    matcher = SettlementMatcher()
    matcher.last_request_time = 0  # Reset for test
    
    # We want to test that 3 concurrent calls take at least 2 seconds 
    # (0s, 1s, 2s)
    
    start_time = time.time()
    
    def call_rate_limit():
        matcher._rate_limit()
        
    threads = []
    for _ in range(3):
        t = threading.Thread(target=call_rate_limit)
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Duration for 3 calls: {duration:.2f}s")
    
    # It should take at least 2.0 seconds
    # (Allowing a small margin for system noise, e.g. 1.95s)
    assert duration >= 1.9
    # It shouldn't take much more than 2 seconds (e.g. 2.2s)
    assert duration < 2.5

def test_rate_limiter_sequential():
    """
    Test that sequential calls are also rate limited correctly.
    """
    matcher = SettlementMatcher()
    matcher.last_request_time = 0
    
    start_time = time.time()
    matcher._rate_limit() # T=0
    matcher._rate_limit() # T=1
    end_time = time.time()
    
    duration = end_time - start_time
    assert duration >= 0.9
    assert duration < 1.5
