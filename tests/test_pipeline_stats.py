
import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import sys
import os

# Add the root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline import calculate_diagnostics

class TestPipelineStats(unittest.TestCase):
    def test_ai_proposals_are_not_counted_as_approved_matches(self):
        lamas_data = pd.DataFrame(
            {
                "LAMAS_name": ["רחוב מאושר", "רחוב מוצע"],
                "LAMAS_id": ["101", "202"],
                "city": ["יישוב בדיקה", "יישוב בדיקה"],
            }
        )
        diagnostic_data = pd.DataFrame(
            {
                "normalized_name": ["מאושר", "מוצע"],
                "status": ["CONFIDENT", "NEEDS_AI"],
                "mapping_status": ["APPROVED_FUZZY", "AI_PROPOSED"],
                "final_LAMAS_id": ["101", None],
                "ai_suggested_LAMAS_id": [None, "202"],
            }
        )
        osm_gdf = gpd.GeoDataFrame(
            {
                "normalized_name": ["מאושר", "מוצע"],
                "geometry": [Point(34.8, 32.0), Point(34.81, 32.0)],
            },
            crs="EPSG:4326",
        )

        diagnostics = calculate_diagnostics(lamas_data, diagnostic_data, osm_gdf)

        self.assertEqual(diagnostics["total_matched"], 1)
        self.assertEqual(diagnostics["confident_matches"], 1)
        self.assertEqual(diagnostics["ai_proposed_matches"], 1)
        self.assertEqual(diagnostics["unmatched_osm_streets"], 1)

    def test_unmatched_lamas_street_with_multiple_names(self):
        """
        Tests that if a LAMAS street has multiple name variations (e.g., 'רחוב הניצנים' and
        'סמ הניצנים') and one of them is matched, the other name variation does not
        incorrectly appear in the 'unmatched' list.
        """
        # --- Mock LAMAS Data ---
        # A single unique LAMAS ID (123) has two different names.
        # Legacy cache without status metadata: every unique id counts.
        lamas_data = {
            'LAMAS_name': ['רחוב הניצנים', 'סמ הניצנים', 'רחוב אחר'],
            'LAMAS_id': [123, 123, 567],
            'city': ['אבן יהודה', 'אבן יהודה', 'אבן יהודה']
        }
        lamas_in_city_df = pd.DataFrame(lamas_data)

        # --- Mock OSM and Diagnostic Data ---
        # OSM street 'הניצנים' is successfully matched to LAMAS ID 123.
        diagnostic_data = {
            'osm_name': ['הניצנים', 'רחוב לא תואם'],
            'normalized_name': ['הניצנים', 'לא תואם'],
            'final_LAMAS_id': ['123', None],
            'status': ['CONFIDENT', 'NO_MATCH']
        }
        diagnostic_df_full = pd.DataFrame(diagnostic_data)

        # A minimal GeoDataFrame is needed for the function call.
        osm_gdf = gpd.GeoDataFrame({
            'osm_name': ['הניצנים', 'רחוב לא תואם', 'רחוב נוסף'],
            'normalized_name': ['הניצנים', 'לא תואם', 'נוסף'],
            'geometry': [Point(1, 1), Point(2, 2), Point(3, 3)]
        })

        # --- Run the function to be tested ---
        diagnostics = calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf)

        # --- Assertions ---
        # The 'unmatched_lamas_street_names' list should only contain 'רחוב אחר'.
        # 'סמ הניצנים' should NOT be in this list because its LAMAS ID (123) was matched.
        self.assertNotIn('סמ הניצנים', diagnostics['unmatched_lamas_street_names'])
        self.assertIn('רחוב אחר', diagnostics['unmatched_lamas_street_names'])
        self.assertEqual(diagnostics['unmatched_lamas_count'], 1)

    def test_status_metadata_counts_four_digit_official_ids_and_ignores_synonyms(self):
        lamas_in_city_df = pd.DataFrame({
            "LAMAS_id": ["1457", "1457", "9000"],
            "LAMAS_key": ["3000:1457", "3000:1457", "3000:9000"],
            "LAMAS_name": ["רחוב רשמי", "שם נרדף", "רחוב אחר"],
            "city": ["עיר בדיקה"] * 3,
            "street_name_status": [
                "official", "synonym of 1457", "official",
            ],
            "is_official": [True, False, True],
        })
        diagnostic_df_full = pd.DataFrame({
            "normalized_name": ["רשמי", "לא תואם"],
            "final_LAMAS_id": ["1457", None],
            "final_LAMAS_key": ["3000:1457", None],
            "ai_suggested_LAMAS_id": [None, None],
        })
        osm_gdf = gpd.GeoDataFrame({
            "normalized_name": ["רשמי", "לא תואם"],
            "geometry": [Point(1, 1), Point(2, 2)],
        })

        diagnostics = calculate_diagnostics(
            lamas_in_city_df, diagnostic_df_full, osm_gdf
        )

        self.assertEqual(diagnostics["total_lamas_streets"], 2)
        self.assertEqual(diagnostics["unmatched_lamas_count"], 1)
        self.assertNotIn("שם נרדף", diagnostics["unmatched_lamas_street_names"])
        self.assertIn("רחוב אחר", diagnostics["unmatched_lamas_street_names"])

if __name__ == '__main__':
    unittest.main()
