import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import normalize_street_name, AI_TRANSLITERATION_CACHE
from unittest.mock import patch

def test_arabic_prefix_removal():
    """
    Tests that common Arabic street prefixes are removed.
    شارع (Sharia - Street)
    طريق (Tariq - Way)
    """
    # Mock AI polish to return the algorithmic result for predictability
    with patch('normalization.polish_transliteration_with_ai', side_effect=lambda a, h: h):
        # شارع (Arabic) -> סלאם
        assert normalize_street_name('شارع السلام') == 'סלאם'
        # طريق הנביאים (Tariq HaNeviim) -> הנביאים
        assert normalize_street_name('طريق הנביאים') == 'הנביאים'

def test_hebrew_names_not_hijacked():
    """
    Ensure Hebrew names starting with 'אל' are not stripped if not intended.
    Currently our regex \bאל(?=[א-ת]) is very aggressive.
    """
    # אלעזר should remain אלעזר
    assert normalize_street_name('אלעזר') == 'אלעזר'
    assert normalize_street_name('אלחנן') == 'אלחנן'

def test_al_prefix_standardization():
    """
    Tests that the Arabic 'Al-' prefix (in various forms) is handled.
    """
    with patch('normalization.polish_transliteration_with_ai', side_effect=lambda a, h: h):
        # אל-סלאם -> סלאם
        assert normalize_street_name('אל-סלאם') == 'סלאם'
        
        # In Arabic script: السلام (Al-Salam) -> סלאם
        assert normalize_street_name('السلام') == 'סלאם'

def test_hebrew_transliterated_arabic_prefixes():
    """
    Tests handling of common Hebrew transliterations of Arabic prefixes.
    """
    # א-סלאם -> סלאם
    assert normalize_street_name('א-סלאם') == 'סלאם'
    # אל-סלאם -> סלאם
    assert normalize_street_name('אל-סלאם') == 'סלאם'
    # אלסלאם remains אלסלאם (to avoid false positives with Hebrew names like אלעזר)
    assert normalize_street_name('אלסלאם') == 'אלסלאם'
