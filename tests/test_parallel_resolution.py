import pytest
import time
import concurrent.futures
from unittest.mock import patch, MagicMock
from batch_process_settlements import BatchProcessor

def test_pipeline_overlap_logic():
    """
    Verify that pipelines start as soon as a settlement is resolved.
    City A: 1s resolution, 1s pipeline
    City B: 1s resolution, 1s pipeline
    
    If sequential resolution (Step 1 then Step 2):
    T=0: Start Res A
    T=1: Finish Res A, Start Res B
    T=2: Finish Res B, Start Pipelines
    T=3: Pipelines finish (since they are parallel)
    Total: ~3s
    
    If overlapped (Submit pipeline immediately after resolution):
    T=0: Start Res A, Start Res B (in parallel pool)
    T=1: Res A finished -> Start Pipeline A
    T=1: Res B finished -> Start Pipeline B
    T=2: Pipelines A & B finished
    Total: ~2s
    """
    processor = BatchProcessor(workers=4)
    settlements = ["City A", "City B"]
    
    # Track when events happen
    events = []
    
    def mock_resolve(name):
        time.sleep(1.0)
        events.append(f"Res {name} finished at {time.time():.1f}")
        return {
            'settlement': name,
            'status': 'ready_for_pipeline',
            'match': {'display_name': f'Matched {name}'}
        }

    def mock_worker(name, *args, **kwargs):
        time.sleep(1.0)
        events.append(f"Pipe {name} finished at {time.time():.1f}")
        return {'status': 'success', 'duration_seconds': 1.0}

    with patch.object(BatchProcessor, 'resolve_settlement', side_effect=mock_resolve), \
         patch('batch_process_settlements.worker_wrapper', side_effect=mock_worker):
        
        start_time = time.time()
        processor.run_batch(settlements)
        end_time = time.time()
        
        duration = end_time - start_time
        print(f"Total duration: {duration:.2f}s")
        for e in sorted(events):
            print(e)
            
        # If overlapped, it should take ~2s (1s res + 1s pipe)
        # If not overlapped, it should take ~3s (2s all-res + 1s pipe)
        assert duration < 2.5