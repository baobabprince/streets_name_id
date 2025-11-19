import unittest
import geopandas as gpd
import sys
import os

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_management.OSM_streets import fetch_osm_street_data

class TestHebrewNameFetching(unittest.TestCase):

    def test_fetch_hebrew_names_for_abu_ghosh(self):
        """
        Tests that fetching data for a place with known Hebrew names
        (like Abu Ghosh) returns a GeoDataFrame containing the 'name:he' column.
        """
        # Using a smaller, specific place for a quicker test
        place_name = "Abu Ghosh, Israel"

        # Act
        osm_gdf = fetch_osm_street_data(place_name)

        # Assert
        self.assertIsNotNone(osm_gdf, "The returned GeoDataFrame should not be None.")
        self.assertIn('name:he', osm_gdf.columns, "The 'name:he' column should be present in the output.")

        # Optional: Check if at least some streets have a Hebrew name
        self.assertTrue(osm_gdf['name:he'].notna().any(), "At least one street should have a non-null 'name:he' value.")

if __name__ == '__main__':
    unittest.main()
