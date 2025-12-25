import pytest
import time
import threading
from unittest.mock import patch, MagicMock
from settlement_matcher import SettlementMatcher

def test_parallel_nominatim_rate_limiting():
    """
    Test that multiple threads calling search_settlement are rate-limited
    to approximately 1 request per second globally.
    """
    matcher = SettlementMatcher()
    # Ensure cache is empty to force API calls
    matcher.cache.cache = {}
    
    def mock_get(*args, **kwargs):
        mock_res = MagicMock()
        mock_res.status_code = 200
        mock_res.json.return_value = [] # No results is fine for timing
        return mock_res

    # We'll use 5 threads. The first one runs immediately,
    # the next 4 should each wait at least 1 second.
    # Total time should be at least 4 seconds.
    
    num_threads = 5
    threads = []
    start_time = time.time()
    
    with patch('requests.get', side_effect=mock_get):
        for i in range(num_threads):
            # Using names like "City0" (no spaces) prevents re.split from creating variants
            t = threading.Thread(target=matcher.search_settlement, args=(f"City{i}",))
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Duration for {num_threads} parallel requests: {duration:.2f}s")
    
    # It should take at least (num_threads - 1) * NOMINATIM_RATE_LIMIT
    assert duration >= (num_threads - 1) * 1.0
    # And it shouldn't take MUCH longer (e.g. not 10 seconds)
    assert duration < num_threads * 1.5
