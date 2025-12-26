import pytest
from unittest.mock import patch, MagicMock
from settlement_matcher import SettlementMatcher

def test_selective_fallback_with_parentheses():
    """
    Test that search_settlement uses the full name first, then only the parenthetical content.
    It should NOT append ", Israel" or split by dashes.
    """
    matcher = SettlementMatcher()
    matcher.cache.cache = {} # Clear cache
    
    def mock_get(url, params=None, **kwargs):
        q = params.get('q', '')
        mock_res = MagicMock()
        mock_res.status_code = 200
        # Return result ONLY for the parenthetical part to test fallback
        if q == "זרעית":
            mock_res.json.return_value = [{'display_name': 'זרעית, ישראל', 'lat': '33.0', 'lon': '35.0', 'osm_id': '123', 'type': 'village', 'address': {'country': 'Israel', 'country_code': 'il'}}]
        else:
            mock_res.json.return_value = []
        return mock_res

    with patch('requests.get', side_effect=mock_get) as mock_requests:
        # "Kfar Rosenwald (Zarit)"
        result = matcher.search_settlement("כפר רוזנוולד (זרעית)")
        
        # Check calls
        calls = [call.args[1].get('q') if len(call.args) > 1 else call.kwargs.get('params', {}).get('q') 
                 for call in mock_requests.call_args_list]
        
        # We expect:
        # 1. "כפר רוזנוולד (זרעית)" (Full)
        # 2. "זרעית" (Parenthetical fallback)
        # Should NOT have "כפר רוזנוולד (זרעית), Israel" or "כפר רוזנוולד"
        
        assert "כפר רוזנוולד (זרעית)" in calls
        assert "זרעית" in calls
        assert "כפר רוזנוולד (זרעית), Israel" not in calls
        assert "כפר רוזנוולד" not in calls # Should NOT split by dash/space if no parentheses match? 
        # Actually, the spec says "Remove splitting by dashes/hyphens".
        
        assert result is not None
        assert result.settlement_name == "כפר רוזנוולד (זרעית)"

def test_no_israel_suffix_added():
    """
    Test that ", Israel" is no longer appended to queries.
    """
    matcher = SettlementMatcher()
    matcher.cache.cache = {}
    
    with patch('requests.get') as mock_requests:
        mock_res = MagicMock()
        mock_res.status_code = 200
        mock_res.json.return_value = []
        mock_requests.return_value = mock_res
        
        matcher.search_settlement("תל אביב")
        
        calls = [call.kwargs.get('params', {}).get('q') for call in mock_requests.call_args_list]
        for q in calls:
            assert ", Israel" not in q
