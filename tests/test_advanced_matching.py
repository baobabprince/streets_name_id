import unittest
import pandas as pd
from normalization import find_fuzzy_candidates, normalize_street_name

class TestAdvancedFuzzyMatching(unittest.TestCase):

    def setUp(self):
        """Set up mock data for testing."""
        # Mock OSM data with a primary non-Hebrew name and an alternative Hebrew name
        osm_data = {
            'osm_id': ['123'],
            'osm_name': ['Main Street'],
            'all_osm_names': [['Main Street', 'רחוב ראשי']],
            'city': ['Test City']
        }
        self.osm_df = pd.DataFrame(osm_data)

        # Mock LAMAS data with the Hebrew name
        lamas_data = {
            'LAMAS_id': ['1001'],
            'LAMAS_name': ['רחוב ראשי'],
            'city': ['Test City']
        }
        self.lamas_df = pd.DataFrame(lamas_data)

        # Pre-normalize the LAMAS names as the main pipeline would
        self.lamas_df['normalized_name'] = self.lamas_df['LAMAS_name'].apply(normalize_street_name)

    def test_match_on_alternative_name(self):
        """
        Verify that a match is found based on an alternative name ('רחוב ראשי')
        when the primary name ('Main Street') does not match.
        """
        result_df = find_fuzzy_candidates(self.osm_df, self.lamas_df)

        self.assertFalse(result_df.empty, "Result DataFrame should not be empty.")

        match_result = result_df.iloc[0]

        # Check that the status is CONFIDENT and the correct ID was found
        self.assertEqual(match_result['status'], 'CONFIDENT', "Match status should be CONFIDENT.")
        self.assertEqual(match_result['best_LAMAS_id'], '1001', "Should have matched LAMAS ID 1001.")

        # Check that the score is high (should be 100 or close)
        self.assertGreater(match_result['best_score'], 95, "Match score should be very high.")

        # Verify that the correct OSM name is identified as the source of the match
        self.assertEqual(match_result['matched_osm_name'], 'רחוב ראשי', "The matched OSM name should be the Hebrew one.")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
