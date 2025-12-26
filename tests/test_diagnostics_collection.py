import pytest
import pandas as pd
import json
from normalization import find_fuzzy_candidates

def test_find_fuzzy_candidates_captures_scoring_breakdown():
    """
    Test that find_fuzzy_candidates captures the scoring breakdown in a diagnostics column.
    """
    osm_data = pd.DataFrame({
        'osm_id': ['1'],
        'normalized_name': ['הרצל'],
        'city': ['תל אביב']
    })
    
    lamas_data = pd.DataFrame({
        'LAMAS_id': ['101'],
        'LAMAS_name': ['בנימין זאב הרצל'],
        'normalized_name': ['בנימין זאב הרצל'],
        'city': ['תל אביב']
    })
    
    # We expect a new column 'diagnostics' containing detailed score info
    results = find_fuzzy_candidates(osm_data, lamas_data)
    
    assert 'diagnostics' in results.columns
    
    # Parse the diagnostics JSON for the first row
    diag_raw = results.iloc[0]['diagnostics']
    assert diag_raw is not None
    
    diag = json.loads(diag_raw)
    assert 'scoring_breakdown' in diag
    
    # Check for specific components
    best_candidate_diag = diag['scoring_breakdown'][0]
    assert 'fuzz_ratio' in best_candidate_diag
    assert 'token_sort_ratio' in best_candidate_diag
    assert 'token_set_ratio' in best_candidate_diag
    assert 'final_score' in best_candidate_diag
    assert best_candidate_diag['lamas_id'] == '101'

def test_pipeline_captures_ai_metadata(tmp_path):
    """
    Test that the pipeline captures AI prompts and responses in diagnostics.
    This requires mocking the AI call.
    """
    from pipeline import run_pipeline
    from unittest.mock import patch, MagicMock
    import os
    
    # Create mock data
    osm_gdf = pd.DataFrame({
        'osm_id': ['1'],
        'osm_name': ['הרצל'],
        'normalized_name': ['הרצל'],
        'city': ['אלעד'],
        'geometry': [None]
    })
    
    lamas_df = pd.DataFrame({
        'LAMAS_id': ['101', '102'],
        'LAMAS_name': ['הרצל א', 'הרצל ב'],
        'normalized_name': ['הרצל א', 'הרצל ב'],
        'city': ['אלעד', 'אלעד']
    })
    
    # Mocking the load/fetch functions to avoid network calls
    with patch('pipeline.load_or_fetch_osm', return_value=osm_gdf), \
         patch('pipeline.load_or_fetch_LAMAS', return_value=lamas_df), \
         patch('pipeline.SettlementMatcher.search_settlement', return_value=MagicMock(display_name='אלעד', settlement_name='אלעד')), \
         patch('pipeline.get_ai_resolution_batch', return_value={'הרצל': '101'}), \
         patch('pipeline.build_adjacency_map', return_value={}), \
         patch('pipeline.API_KEY', 'fake_key'):
        
        # We need to ensure the find_fuzzy_candidates returns a status that triggers AI
        # For 'הרצל' vs 'הרצל א', it will likely be NEEDS_AI
        
        # Run pipeline with AI enabled
        status = run_pipeline(place='אלעד', use_ai=True, skip_html=True)
        
        # Check diagnostic report
        # The pipeline saves it to data/diagnostic_report_אלעד.csv
        report_path = os.path.join('data', 'diagnostic_report_אלעד.csv')
        assert os.path.exists(report_path)
        
        report = pd.read_csv(report_path)
        assert 'diagnostics' in report.columns
        
        diag = json.loads(report.iloc[0]['diagnostics'])
        assert 'ai_resolution' in diag
        assert diag['ai_resolution']['prompt'] is not None
        assert diag['ai_resolution']['response'] == '101'

def test_pipeline_generates_json_diagnostic_report(tmp_path):
    """
    Test that the pipeline saves a detailed JSON diagnostic report in batch_reports/.
    """
    from pipeline import run_pipeline
    from unittest.mock import patch, MagicMock
    import os
    import glob
    
    # Create mock data
    osm_gdf = pd.DataFrame({
        'osm_id': ['1'],
        'osm_name': ['הרצל'],
        'normalized_name': ['הרצל'],
        'city': ['אלעד'],
        'geometry': [None]
    })
    
    lamas_df = pd.DataFrame({
        'LAMAS_id': ['101'],
        'LAMAS_name': ['הרצל א'],
        'normalized_name': ['הרצל א'],
        'city': ['אלעד']
    })
    
    with patch('pipeline.load_or_fetch_osm', return_value=osm_gdf), \
         patch('pipeline.load_or_fetch_LAMAS', return_value=lamas_df), \
         patch('pipeline.SettlementMatcher.search_settlement', return_value=MagicMock(display_name='אלעד', settlement_name='אלעד')), \
         patch('pipeline.build_adjacency_map', return_value={}):
        
        # Run pipeline
        run_pipeline(place='אלעד', use_ai=False, skip_html=True)
        
        # Check if a JSON file was created in batch_reports/
        json_reports = glob.glob(os.path.join('batch_reports', 'diagnostics_*.json'))
        assert len(json_reports) > 0
        
        # Check content of the latest report
        latest_report = max(json_reports, key=os.path.getmtime)
        with open(latest_report, 'r', encoding='utf-8') as f:
            data = json.load(f)
            assert 'אלעד' in data
            assert '1' in data['אלעד']
            assert 'normalization' in data['אלעד']['1']

def test_html_generation_includes_diagnostics():
    """
    Test that generate_html.py embeds diagnostics in the path elements.
    """
    import os
    import re
    from generate_html import create_html_from_gdf
    from shapely.geometry import LineString
    import geopandas as gpd
    
    # Create mock GDF with diagnostics
    diag_info = {'scoring_breakdown': [{'final_score': 95}]}
    gdf = gpd.GeoDataFrame({
        'osm_id': ['123'],
        'osm_name': ['הרצל'],
        'geometry': [LineString([(0,0), (1,1)])],
        'diagnostics': [json.dumps(diag_info)],
        'final_LAMAS_id': ['101']
    }, crs="EPSG:4326")
    
    # Generate HTML
    place = "TestCity"
    create_html_from_gdf(gdf, place)
    
    html_path = os.path.join('HTML', f'{place}_roads.html')
    assert os.path.exists(html_path)
    
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
        # Check if the diagnostics JSON is embedded in the data-diagnostics attribute
        assert 'data-diagnostics=\'{"scoring_breakdown": [{"final_score": 95}]}\'' in content
