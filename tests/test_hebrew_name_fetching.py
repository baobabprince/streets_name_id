import os
import shutil
import unittest

from OSM_streets import fetch_osm_street_data

# Integration test: needs a real OSM extract (or live Overpass access).
HAS_OSMIUM = bool(shutil.which(os.environ.get("OSMIUM_BIN", "osmium")))


class TestHebrewNameFetching(unittest.TestCase):

    @unittest.skipUnless(HAS_OSMIUM, "requires osmium-tool and a local OSM PBF extract")
    def test_fetch_hebrew_names_for_abu_ghosh(self):
        """Fetching Abu Ghosh must expose the 'name:he' column with values."""
        osm_gdf = fetch_osm_street_data("Abu Ghosh, Israel")

        self.assertIsNotNone(osm_gdf, "The returned GeoDataFrame should not be None.")
        self.assertIn('name:he', osm_gdf.columns, "The 'name:he' column should be present in the output.")
        self.assertTrue(osm_gdf['name:he'].notna().any(), "At least one street should have a non-null 'name:he' value.")


if __name__ == '__main__':
    unittest.main()
