import pytest
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import transliterate_arabic_to_hebrew

def test_basic_transliteration():
    """
    Tests basic phonetic mapping from Arabic to Hebrew.
    """
    # سلام (Salam) -> סלאם
    assert transliterate_arabic_to_hebrew('سلام') == 'סלאם'
    # قدس (Al-Quds) -> קדס
    assert transliterate_arabic_to_hebrew('قدس') == 'קדס'

def test_arabic_hebrew_phonetic_rules():
    """
    Tests specific Arabic characters and their Hebrew mappings.
    """
    # ب -> ב
    assert transliterate_arabic_to_hebrew('باب') == 'באב'
    # תמר (Tamar) -> תמר
    assert transliterate_arabic_to_hebrew('תמר') == 'תמר'
    # ث -> ת (phonetic)
    assert transliterate_arabic_to_hebrew('ث') == 'ת'
    # ج -> ג
    assert transliterate_arabic_to_hebrew('جبل') == 'גבל'
    # ح -> ח
    assert transliterate_arabic_to_hebrew('حارة') == 'חארה'
    # خ -> ח or כ
    assert transliterate_arabic_to_hebrew('خ') == 'ח'
    # د -> ד
    assert transliterate_arabic_to_hebrew('د') == 'ד'
    # ذ -> ד
    assert transliterate_arabic_to_hebrew('ذ') == 'ד'
    # ر -> ר
    assert transliterate_arabic_to_hebrew('ر') == 'ר'
    # ז -> ז
    assert transliterate_arabic_to_hebrew('ز') == 'ז'
    # س -> ס
    assert transliterate_arabic_to_hebrew('س') == 'ס'
    # ש -> ש
    assert transliterate_arabic_to_hebrew('ש') == 'ש'
    # ص -> ס or צ
    assert transliterate_arabic_to_hebrew('ص') == 'ס'
    # ض -> ד
    assert transliterate_arabic_to_hebrew('ض') == 'ד'
    # ط -> ט
    assert transliterate_arabic_to_hebrew('ط') == 'ט'
    # ظ -> ז
    assert transliterate_arabic_to_hebrew('ظ') == 'ז'
    # ע -> ע
    assert transliterate_arabic_to_hebrew('ע') == 'ע'
    # غ -> ג
    assert transliterate_arabic_to_hebrew('غ') == 'ג'
    # פ -> פ (or ף in final position)
    assert transliterate_arabic_to_hebrew('ف') == 'ף'
    # ק -> ק
    assert transliterate_arabic_to_hebrew('ق') == 'ק'
    # כ -> כ (or ך in final position)
    assert transliterate_arabic_to_hebrew('ك') == 'ך'
    # ל -> ל
    assert transliterate_arabic_to_hebrew('ل') == 'ל'
    # מ -> מ (or ם in final position)
    assert transliterate_arabic_to_hebrew('م') == 'ם'
    # נ -> נ (or ן in final position)
    assert transliterate_arabic_to_hebrew('ن') == 'ן'
    # ה -> ה
    assert transliterate_arabic_to_hebrew('ه') == 'ה'
    # ו -> ו
    assert transliterate_arabic_to_hebrew('و') == 'ו'
    # ي -> י
    assert transliterate_arabic_to_hebrew('ي') == 'י'

def test_polish_transliteration_with_ai():
    """
    Tests the AI polish function using a mock.
    """
    from normalization import polish_transliteration_with_ai
    from unittest.mock import patch, MagicMock
    
    # We mock the requests.post call inside polish_transliteration_with_ai
    with patch('requests.post') as mock_post:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "candidates": [{"content": {"parts": [{"text": "סלאם"}]}}]
        }
        mock_post.return_value = mock_response
        
        # We need a fake API key in the environment for the function to run
        with patch.dict('os.environ', {'GEMINI_API_KEY': 'fake_key'}):
            result = polish_transliteration_with_ai('سلام', 'סלאם')
            assert result == 'סלאם'
            
            # Test another one where it corrects it
            mock_response.json.return_value = {
                "candidates": [{"content": {"parts": [{"text": "אל-קודס"}]}}]
            }
            result = polish_transliteration_with_ai('אל-קודס', 'קדס')
            assert result == 'אל-קודס'