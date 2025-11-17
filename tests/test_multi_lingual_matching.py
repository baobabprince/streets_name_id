import unittest
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

from normalization import find_fuzzy_candidates, normalize_street_name

class TestMultiLingualMatching(unittest.TestCase):

    def test_abu_ghosh_scenario(self):
        """
        Tests the matching logic for a scenario like Abu Ghosh, where OSM streets
        have primary Arabic names and secondary Hebrew names.
        """
        # --- Mock OSM Data ---
        osm_data = {
            'osm_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
            'osm_name': [
                'شارع السلام', 'شارع عمر بن الخطاب', 'شارع المدارس',
                'Street Without Hebrew Name', 'شارع الكنيسة', 'شارع عين رافا',
                'شارع صلاح الدين', 'Another Nameless Street', 'شارع عبد الرحمن', 'شارع الشروق'
            ],
            'name:he': [
                'רחוב השלום', 'עומר בן אלחטאב', 'אל מדארס',
                None, 'רחוב הכנסיה', 'עין ראפה',
                'צלאח א דין', None, 'עבד אל רחמן', 'אל שרוק'
            ],
            'city': ['אבו גוש'] * 10,
            'geometry': [LineString([(0, i), (1, i)]) for i in range(10)]
        }
        osm_gdf = gpd.GeoDataFrame(osm_data, crs="EPSG:4326")

        # --- Mock LAMAS Data ---
        lamas_data = {
            'LAMAS_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'LAMAS_name': [
                'רחוב השלום', 'עומר אבן אל חטאב', 'אלמדארס',
                'רחוב התאנה', 'רחוב הכנסייה', 'עין ראפא',
                'צלאח אל דין', 'רחוב הזית', 'עבד אל רחמאן', 'א-שרוק'
            ],
            'city': ['אבו גוש'] * 10
        }
        lamas_df = pd.DataFrame(lamas_data)

        # --- Normalization (simulating the pipeline) ---
        lamas_df['normalized_name'] = lamas_df['LAMAS_name'].apply(normalize_street_name)

        osm_name_columns = [col for col in osm_gdf.columns if col.startswith('name') or col == 'osm_name']
        for col in osm_name_columns:
            normalized_col_name = f"normalized_{col}"
            osm_gdf[normalized_col_name] = osm_gdf[col].apply(normalize_street_name)

        osm_gdf.dropna(subset=osm_name_columns, how='all', inplace=True)

        # --- Run Matching ---
        candidates_df = find_fuzzy_candidates(osm_gdf, lamas_df)

        # --- Assertions ---
        self.assertFalse(candidates_df.empty, "Candidates DataFrame should not be empty")

        confident_matches = candidates_df[candidates_df['status'] == 'CONFIDENT']

        # We expect at least 5 of the 8 streets with Hebrew names to match
        # (allowing for some fuzzy matching failures)
        self.assertGreaterEqual(len(confident_matches), 5, "Matching rate should be at least 50%")
