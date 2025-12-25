
import unittest
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import normalize_street_name, find_fuzzy_candidates

class TestMatchingEdgeCases(unittest.TestCase):

    def test_partial_vs_full_name(self):
        """
        Tests if a partial name (e.g., 'שבזי') is flagged as NEEDS_AI
        when using a high confidence threshold, reflecting stricter logic
        to avoid false positives (like Herzl vs Herzl Rosenblum).
        """
        osm_data = {'osm_id': [1], 'normalized_name': ['שלום שבזי']}
        lamas_data = {'LAMAS_id': [101], 'LAMAS_name': ['שבזי'], 'normalized_name': ['שבזי']}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        # We expect this to be NEEDS_AI because the score (~82) is below the strict 95 threshold
        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=95)
        self.assertEqual(results_df.iloc[0]['status'], 'NEEDS_AI')

    def test_spelling_variation(self):
        """
        Tests if minor spelling variations (e.g., הנרייטה vs. הנריטה) can be
        confidently matched.
        """
        osm_data = {'osm_id': [2], 'normalized_name': ['הנרייטה סולד']}
        lamas_data = {'LAMAS_id': [102], 'LAMAS_name': ['הנריטה סולד'], 'normalized_name': ['הנריטה סולד']}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=95)
        self.assertEqual(results_df.iloc[0]['status'], 'CONFIDENT')

    def test_word_order_difference(self):
        """
        Tests if different word order (e.g., 'מרדכי וחווה פרימן' vs.
        'פרימן מרדכי וחווה') can be confidently matched.
        """
        osm_data = {'osm_id': [3], 'normalized_name': ['מרדכי וחווה פרימן']}
        lamas_data = {'LAMAS_id': [103], 'LAMAS_name': ['פרימן מרדכי וחווה'], 'normalized_name': ['פרימן מרדכי וחווה']}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=95)
        self.assertEqual(results_df.iloc[0]['status'], 'CONFIDENT')

    def test_unhandled_abbreviation(self):
        """
        Tests that a new, unhandled abbreviation (e.g., 'פרופ') fails to
        normalize correctly. This test should fail until the normalization
        rules are updated.
        """
        self.assertEqual(normalize_street_name('פרופ כהן'), 'פרופסור כהן')

    def test_aggressive_normalization_distinction(self):
        """
        Tests that street types (e.g., 'רחוב' vs. 'סמטה') are not aggressively
        normalized away, leading to incorrect matches.
        'רחוב הגפן' and 'סמטת הגפן' should NOT be a confident match.
        """
        osm_data = {'osm_id': [4], 'normalized_name': ['רחוב הגפן']}
        lamas_data = {'LAMAS_id': [104], 'LAMAS_name': ['סמטת הגפן'], 'normalized_name': ['סמטת הגפן']}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=95)
        self.assertNotEqual(results_df.iloc[0]['status'], 'CONFIDENT')

    def test_empty_string_normalization(self):
        """
        Tests that an empty or whitespace-only string is normalized to None.
        """
        self.assertIsNone(normalize_street_name(""))
        self.assertIsNone(normalize_street_name("   "))

    def test_nan_lamas_name(self):
        """
        Tests that a NaN value in lamas_df is handled correctly.
        """
        osm_data = {'osm_id': [5], 'normalized_name': ['רחוב כלשהו']}
        lamas_data = {'LAMAS_id': [105], 'LAMAS_name': [None], 'normalized_name': [None]}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df)
        self.assertEqual(results_df.iloc[0]['status'], 'MISSING')

    def test_ambiguous_match(self):
        """
        Tests that multiple high-scoring matches result in a 'NEEDS_AI' status.
        """
        osm_data = {'osm_id': [6], 'normalized_name': ['הרצל']}
        lamas_data = {
            'LAMAS_id': [106, 107],
            'LAMAS_name': ['רחוב הרצל', 'שדרות הרצל'],
            'normalized_name': ['רחוב הרצל', 'שדרות הרצל']
        }
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=80, needs_ai_threshold=70)
        self.assertEqual(results_df.iloc[0]['status'], 'NEEDS_AI')

    def test_synonyms_same_id_not_ambiguous(self):
        """
        Tests that multiple high-scoring matches with the SAME LAMAS ID
        do not trigger 'NEEDS_AI'.
        """
        osm_data = {'osm_id': [8], 'normalized_name': ['זבוטינסקי']}
        # In reality, multiple LAMAS entries might normalize to the same name
        lamas_data = {
            'LAMAS_id': [139, 139],
            'LAMAS_name': ["ז'בוטינסקי", 'זבוטינסקי'],
            'normalized_name': ['זבוטינסקי', 'זבוטינסקי']
        }
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=95)
        # Should be CONFIDENT because both top matches point to the same ID
        self.assertEqual(results_df.iloc[0]['status'], 'CONFIDENT')
        self.assertEqual(results_df.iloc[0]['best_LAMAS_id'], 139)

    def test_unnamed_osm_street(self):
        """
        Tests that an unnamed (NaN) OSM street is correctly marked as 'MISSING'.
        """
        osm_data = {'osm_id': [7], 'normalized_name': [None]}
        lamas_data = {'LAMAS_id': [108], 'LAMAS_name': ['רחוב כלשהו'], 'normalized_name': ['רחוב כלשהו']}
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)
        results_df = find_fuzzy_candidates(osm_gdf, lamas_df)
        self.assertEqual(results_df.iloc[0]['status'], 'MISSING')

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
