import unittest
import re
import pandas as pd

from normalization import normalize_street_name

class TestNormalizeStreetName(unittest.TestCase):

    def test_abbreviations_and_expansion(self):
        # 1. קיצורים והרחבות
        self.assertEqual(normalize_street_name("רח' הנביאים"), "הנביאים")
        self.assertEqual(normalize_street_name("שד. רוטשילד"), "רוטשילד")
        self.assertEqual(normalize_street_name("שד' העצמאות"), "העצמאות")
        self.assertEqual(normalize_street_name("שד בן גוריון"), "בן גוריון")
        self.assertEqual(normalize_street_name("כי. המדינה"), "המדינה")

    def test_punctuation_and_spacing(self):
        # 2. סימני פיסוק ורווחים
        self.assertEqual(normalize_street_name("מנחם בגין - דרך"), "מנחם בגין דרך")
        self.assertEqual(normalize_street_name("הרצל. "), "הרצל")
        self.assertEqual(normalize_street_name("  הנשיא   הראשון  "), "הנשיא הראשון")
        self.assertEqual(normalize_street_name("  בן-יהודה  "), "בן יהודה")
        
    def test_mixed_case_and_special_chars(self):
        # 3. שמות שונים ומקרי קצה
        self.assertEqual(normalize_street_name("אבא הלל"), "אבא הלל")
        self.assertEqual(normalize_street_name("הרב סילבר"), "הרב סילבר")
        self.assertEqual(normalize_street_name("אבא הלל סילבר"), "אבא הלל סילבר")
        self.assertIsNone(normalize_street_name(None))
        self.assertIsNone(normalize_street_name(float('nan')))

    def test_no_unnecessary_expansion(self):
        """
        Tests that the function does not expand words that are already in their full form.
        """
        self.assertEqual(normalize_street_name("שדרות רוטשילד"), "רוטשילד")
        self.assertEqual(normalize_street_name("רחובות העיר"), "רחובות העיר") # "רחוב" is part of "רחובות"

    def test_prefix_suffix_removal(self):
        """
        Tests the removal of common prefixes and suffixes.
        """
        self.assertEqual(normalize_street_name("רחוב הרצל"), "הרצל")
        self.assertEqual(normalize_street_name("שדרות רגר"), "רגר")
        self.assertEqual(normalize_street_name("סמטת האלמונים"), "האלמונים")
        self.assertEqual(normalize_street_name("האלמונים סמטה"), "האלמונים")
        self.assertEqual(normalize_street_name("כיכר אתרים"), "אתרים")
        self.assertEqual(normalize_street_name("מבוא הגפן"), "הגפן")
        # Test that it doesn't remove from single-word names
        self.assertEqual(normalize_street_name("שדרות"), "שדרות")
        self.assertEqual(normalize_street_name("מבוא"), "מבוא")
        # Test with abbreviations
        self.assertEqual(normalize_street_name("רח' הרצל"), "הרצל")
        self.assertEqual(normalize_street_name("שד. רגר"), "רגר")


# # הרצת הבדיקות
unittest.main(argv=['first-arg-is-ignored'], exit=False) 
# נמנעים מהרצה אוטומטית בתוך הקוד, אך זהו אופן השימוש הנכון.