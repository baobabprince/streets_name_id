
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

    def test_unmatched_lamas_street_with_multiple_names(self):
        """
        Tests that if a LAMAS street has multiple name variations (e.g., 'רחוב הניצנים' and
        'סמ הניצנים') and one of them is matched, the other name variation does not
        incorrectly appear in the 'unmatched' list.
        """
        # --- Mock LAMAS Data ---
        # A single unique LAMAS ID (123) has two different names.
        # Using 3-digit codes since 4-digit codes are filtered out (non-streets)
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

if __name__ == '__main__':
    unittest.main()
