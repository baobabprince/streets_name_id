import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pipeline import run_pipeline

def test_pipeline_calls_batch_ai_resolution():
    """
    Test that the pipeline correctly batches AI requests.
    """
    # Create mock data with two streets that need AI
    osm_gdf = pd.DataFrame({
        'osm_id': ['1', '2'],
        'osm_name': ['הרצל', 'זבוטינסקי'],
        'normalized_name': ['הרצל', 'זבוטינסקי'],
        'city': ['אלעד', 'אלעד'],
        'geometry': [None, None]
    })
    
    # Needs_AI usually happens when score is between 75 and 80 or multiple candidates
    # We'll mock find_fuzzy_candidates to return NEEDS_AI
    candidates_df = pd.DataFrame({
        'osm_id': ['1', '2'],
        'status': ['NEEDS_AI', 'NEEDS_AI'],
        'best_score': [78.0, 78.0],
        'best_LAMAS_id': ['101', '201'],
        'best_LAMAS_name': ['הרצל', 'זבוטינסקי'],
        'all_candidates': ["ID: 101, Name: 'הרצל'", "ID: 201, Name: 'זבוטינסקי'"],
        'diagnostics': ['{}', '{}']
    })
    
    with patch('pipeline.load_or_fetch_osm', return_value=osm_gdf), \
         patch('pipeline.load_or_fetch_LAMAS', return_value=pd.DataFrame(columns=['LAMAS_id', 'LAMAS_name', 'city'])), \
         patch('pipeline.SettlementMatcher.search_settlement', return_value=MagicMock(display_name='אלעד', settlement_name='אלעד')), \
         patch('pipeline.build_adjacency_map', return_value={}), \
         patch('pipeline.find_fuzzy_candidates', return_value=candidates_df), \
         patch('pipeline.get_ai_resolution_batch') as mock_gemini_batch, \
         patch('pipeline.API_KEY', 'fake_key'):
        
        # Setup mock return value for gemini batch
        mock_gemini_batch.return_value = {'הרצל': '101', 'זבוטינסקי': '201'}
        
        # Run pipeline
        run_pipeline(place='אלעד', use_ai=True, use_local_ai=False, skip_html=True)
        
        # Verify batch was called once with 2 streets
        assert mock_gemini_batch.called
        args, kwargs = mock_gemini_batch.call_args
        assert len(args[1]) == 2 # 2 streets in data
        assert args[1][0]['street_name'] in ['הרצל', 'זבוטינסקי']