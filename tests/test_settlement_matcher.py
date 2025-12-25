import pytest
from unittest.mock import patch, MagicMock
from settlement_matcher import SettlementMatcher

@pytest.fixture
def matcher():
    return SettlementMatcher()

def test_kfar_rosenwald_zarit_match(matcher):
    """
    Test that 'כפר רוזנואלד (זרעית)' matches 'זרעית' correctly.
    """
    def side_effect(url, params, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        q = params.get('q', '')
        
        if q == 'זרעית':
            mock_response.json.return_value = [
                {
                    'display_name': 'זרעית, ישראל',
                    'type': 'village',
                    'lat': '33.08',
                    'lon': '35.24',
                    'importance': 0.35,
                    'osm_id': '689955306',
                    'boundingbox': ['33.07', '33.09', '35.23', '35.25'],
                    'address': {'village': 'זרעית', 'country': 'ישראל', 'country_code': 'il'}
                }
            ]
        else:
            mock_response.json.return_value = []
        return mock_response

    with patch('requests.get', side_effect=side_effect):
        matcher.cache.cache = {}
        match = matcher.search_settlement("כפר רוזנואלד (זרעית)")
        
        assert match is not None
        assert "זרעית" in match.display_name
        assert match.osm_id == '689955306'

def test_kfar_rut_hijacking_prevention_deterministic(matcher):
    """
    Test that 'כפר רות' does NOT match 'כפר סבא' even if 'כפר' is a variant.
    """
    def side_effect(url, params, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        q = params.get('q', '')
        if q == 'כפר':
            mock_response.json.return_value = [
                {
                    'display_name': 'כפר סבא, ישראל',
                    'type': 'city',
                    'lat': '32.175',
                    'lon': '34.906',
                    'importance': 0.6,
                    'osm_id': '1383631',
                    'boundingbox': ['32.1', '32.2', '34.8', '35.0'],
                    'address': {'city': 'כפר סבא', 'country': 'ישראל', 'country_code': 'il'}
                }
            ]
        else:
            mock_response.json.return_value = []
        return mock_response

    with patch('requests.get', side_effect=side_effect):
        matcher.cache.cache = {}
        match = matcher.search_settlement("כפר רות")
        assert match is None

def test_ai_resolution_ambiguity(matcher):
    """Test AI resolution when multiple valid candidates are found."""
    def nominatim_side_effect(url, params, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        q = params.get('q', '')
        if q == 'כפר רות':
            mock_response.json.return_value = [
                {
                    'display_name': 'כפר רות, ישראל',
                    'type': 'village',
                    'lat': '31.91',
                    'lon': '35.01',
                    'importance': 0.3,
                    'osm_id': '111',
                    'boundingbox': ['31.90', '31.92', '35.00', '35.02'],
                    'address': {'village': 'כפר רות', 'country': 'ישראל', 'country_code': 'il'}
                },
                {
                    'display_name': 'יער כפר רות, ישראל',
                    'type': 'wood', 
                    'lat': '31.92',
                    'lon': '35.02',
                    'importance': 0.2,
                    'osm_id': '222',
                    'boundingbox': ['31.91', '31.93', '35.01', '35.03'],
                    'address': {'forest': 'יער כפר רות', 'country': 'ישראל', 'country_code': 'il'}
                }
            ]
        else:
            mock_response.json.return_value = []
        return mock_response

    def ai_side_effect(url, json, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"candidates": [{"content": {"parts": [{"text": "111"}]}}]}
        return mock_response

    with patch('requests.get', side_effect=nominatim_side_effect), \
         patch('requests.post', side_effect=ai_side_effect), \
         patch.dict('os.environ', {'GEMINI_API_KEY': 'fake_key'}):
        matcher.cache.cache = {}
        match = matcher.search_settlement("כפר רות")
        assert match is not None
        assert match.osm_id == '111'

def test_rejected_types_validation(matcher):
    """Confirm that rejected types are filtered out."""
    def side_effect(url, params, **kwargs):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'display_name': 'תחנת כפר רות, ישראל',
                'type': 'bus_stop',
                'lat': '31.91',
                'lon': '35.01',
                'importance': 0.01,
                'osm_id': '999',
                'boundingbox': ['31.90', '31.92', '35.00', '35.02'],
                'address': {'bus_stop': 'תחנה', 'country': 'ישראל', 'country_code': 'il'}
            }
        ]
        return mock_response

    with patch('requests.get', side_effect=side_effect):
        matcher.cache.cache = {}
        match = matcher.search_settlement("כפר רות")
        assert match is None
