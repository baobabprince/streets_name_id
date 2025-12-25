import unittest
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from normalization import find_fuzzy_candidates

class TestFuzzyMatchingAdjustments(unittest.TestCase):

    def test_spelling_variation_score_boost(self):
        """
        Tests that minor spelling variations (e.g., 'הנרייטה סולד' vs 'הנריטה סולד')
        receive a CONFIDENT score (>70).
        With the current 90% weight on token_set_ratio, this might fail if the 
        token mismatch penalizes the score too heavily.
        """
        osm_data = {'osm_id': [1], 'normalized_name': ['הנרייטה סולד']}
        lamas_data = {
            'LAMAS_id': [101], 
            'LAMAS_name': ['הנריטה סולד'], 
            'normalized_name': ['הנריטה סולד']
        }
        osm_gdf = pd.DataFrame(osm_data)
        lamas_df = pd.DataFrame(lamas_data)

        # We expect a CONFIDENT match (default threshold 70)
        results_df = find_fuzzy_candidates(osm_gdf, lamas_df, confident_threshold=70)
        
        # Check if status is CONFIDENT
        self.assertEqual(results_df.iloc[0]['status'], 'CONFIDENT', 
                         f"Expected CONFIDENT match, got {results_df.iloc[0]['status']} with score {results_df.iloc[0]['best_score']}")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
