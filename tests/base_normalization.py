import unittest
import re
import pandas as pd

from normalization import normalize_street_name

class TestNormalizeStreetName(unittest.TestCase):

    def test_abbreviations_and_expansion(self):
        # 1. קיצורים והרחבות
        self.assertEqual(normalize_street_name("רח' הנביאים"), "רחוב הנביאים")
        self.assertEqual(normalize_street_name("שד. רוטשילד"), "שדרות רוטשילד")
        self.assertEqual(normalize_street_name("שד' העצמאות"), "שדרות העצמאות")
        self.assertEqual(normalize_street_name("שד בן גוריון"), "שדרות בן גוריון")
        self.assertEqual(normalize_street_name("כי. המדינה"), "כיכר המדינה")

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
        self.assertEqual(normalize_street_name("שדרות רוטשילד"), "שדרות רוטשילד")
        self.assertEqual(normalize_street_name("רחובות העיר"), "רחובות העיר") # "רחוב" is part of "רחובות"


# # הרצת הבדיקות
unittest.main(argv=['first-arg-is-ignored'], exit=False) 
# נמנעים מהרצה אוטומטית בתוך הקוד, אך זהו אופן השימוש הנכון.