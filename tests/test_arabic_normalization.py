import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import normalize_street_name

def test_arabic_prefix_removal():
    """
    Tests that common Arabic street prefixes are removed.
    شارع (Sharia - Street)
    طريق (Tariq - Way)
    """
    # شارע السلام (Sharia Al-Salam) -> سلام
    assert normalize_street_name('شارع السلام') == 'سلام'
    # طريق הנביאים (Tariq HaNeviim) -> הנביאים
    assert normalize_street_name('طريق הנביאים') == 'הנביאים'

def test_hebrew_names_not_hijacked():
    """
    Ensure Hebrew names starting with 'אל' are not stripped if not intended.
    Currently our regex \bאל(?=[א-ת]) is very aggressive.
    """
    # אלעזר should remain אלעזר
    # If this fails, we need to refine the regex
    assert normalize_street_name('אלעזר') == 'אלעזר'
    assert normalize_street_name('אלחנן') == 'אלחנן'

def test_al_prefix_standardization():
    """
    Tests that the Arabic 'Al-' prefix (in various forms) is handled.
    Note: We might want to strip it or standardize it to 'ה' in Hebrew.
    For now, let's assume we want to strip it to improve fuzzy matching
    against Hebrew names that might or might not have 'ה'.
    """
    # אל-סלאם -> סלאם
    assert normalize_street_name('אל-סלאם') == 'סלאם'
    # Al-Salam -> Salam (if we handle English transliteration prefixes)
    # However, the focus is on Arabic/Hebrew matching.
    
    # In Arabic script: السلام (Al-Salam) -> سلام
    assert normalize_street_name('السلام') == 'سلام'

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
