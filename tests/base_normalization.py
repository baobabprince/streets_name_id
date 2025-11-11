import unittest
import re
import pandas as pd

# נגדיר מחדש את הפונקציה כדי שיהיה ניתן לבדוק אותה
def normalize_street_name(name):
    """
    מנרמלת שם רחוב לצורך השוואה עקבית.
    """
    if pd.isna(name) or name is None:
        return None
    
    name = str(name).strip()
    
    # 1. המרת קיצורים והחלפות נפוצות (רגיש לאותיות גדולות/קטנות)
    replacements = {
        r'\bשד[\'|\.]': 'שדרות',
        r'\bרח[\'|\.]': 'רחוב',
        r'\bכי[\'|\.]': 'כיכר',
    }
    for old, new in replacements.items():
        # flags=re.I להתעלם מגודל אותיות, re.sub מחליף מופעים
        name = re.sub(old, new, name, flags=re.I) 

    # 2. הסרת סימני פיסוק מיותרים והחלפתם ברווח
    # מטפל ב: ., -
    name = re.sub(r'[.,-]', ' ', name)
    
    # 3. ניקוי רווחים כפולים
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

class TestNormalizeStreetName(unittest.TestCase):

    def test_abbreviations_and_expansion(self):
        # 1. קיצורים והרחבות
        self.assertEqual(normalize_street_name("רח' הנביאים"), "רחוב הנביאים")
        self.assertEqual(normalize_street_name("שד. רוטשילד"), "שדרות רוטשילד")
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

# # הרצת הבדיקות
unittest.main(argv=['first-arg-is-ignored'], exit=False) 
# נמנעים מהרצה אוטומטית בתוך הקוד, אך זהו אופן השימוש הנכון.