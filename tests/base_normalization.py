import unittest
import re
import pandas as pd

from normalization import find_fuzzy_candidates, normalize_street_name

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

    def test_fuzzy_candidates_keep_alias_matches_and_statuses(self):
        osm_gdf = pd.DataFrame({
            "osm_id": [10, 20],
            "normalized_name": ["שדרות רוטשילד", "שדרות העצמאות"],
        })
        lamas_df = pd.DataFrame({
            "LAMAS_id": [101, 101, 202],
            "LAMAS_name": ["שדרות רוטשילד", "שדרות רוטשילד 1", "שדרות העצמאות"],
            "normalized_name": ["שדרות רוטשילד", "שדרות רוטשילד", "שדרות העצמאות"],
            "city": ["תל אביב-יפו", "תל אביב-יפו", "תל אביב-יפו"],
        })

        candidates = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=70, needs_ai_threshold=50)

        exact_match = candidates[candidates["osm_id"] == 10].iloc[0]
        self.assertEqual(exact_match["status"], "CONFIDENT")
        self.assertEqual(exact_match["best_LAMAS_id"], "101")
        self.assertIn("שדרות רוטשילד", exact_match["all_candidates"])

    def test_fuzzy_candidates_keep_unnamed_osm_rows_as_missing(self):
        osm_gdf = pd.DataFrame({"osm_id": [10], "normalized_name": [None]})
        lamas_df = pd.DataFrame({
            "LAMAS_id": ["101"],
            "LAMAS_name": ["שדרות רוטשילד"],
            "normalized_name": ["שדרות רוטשילד"],
        })

        candidates = find_fuzzy_candidates(osm_gdf, lamas_df)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates.iloc[0]["status"], "MISSING")


# # הרצת הבדיקות
unittest.main(argv=['first-arg-is-ignored'], exit=False) 
# נמנעים מהרצה אוטומטית בתוך הקוד, אך זהו אופן השימוש הנכון.