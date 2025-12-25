import os

code = """import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pipeline import run_pipeline

def test_pipeline_matches_arabic_name():
    # Create mock data
    osm_gdf = pd.DataFrame({
        'osm_id': ['1'],
        'osm_name': ['شارع السلام'],
        'city': ['רהט'],
        'geometry': [None]
    })
    
    lamas_df = pd.DataFrame({
        'LAMAS_id': ['101'],
        'LAMAS_name': ['השלום'],
        'city': ['רהט']
    })
    
    with patch('pipeline.load_or_fetch_osm', return_value=osm_gdf), \
         patch('pipeline.load_or_fetch_LAMAS', return_value=lamas_df), \
         patch('pipeline.SettlementMatcher.search_settlement', return_value=MagicMock(display_name='רהט', settlement_name='רהט')), \
         patch('pipeline.build_adjacency_map', return_value={}), \
         patch('normalization.requests.post') as mock_ai_post, \
         patch.dict('os.environ', {{'GEMINI_API_KEY': 'fake_key'}})):
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "השלום"}]}}]
        }
        mock_ai_post.return_value = mock_response
        
        status = run_pipeline(place='רהט', use_ai=False, skip_html=True)
        assert status.value == 'SUCCESS'
"""

with open('tests/test_arabic_integration.py', 'w', encoding='utf-8') as f:
    f.write(code)
print("Successfully written tests/test_arabic_integration.py")
