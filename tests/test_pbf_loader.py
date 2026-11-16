"""
Tests for OSM PBF loader and offline extractor.
"""

import unittest
import geopandas as gpd
from shapely.geometry import box

from osm_pbf_loader import (
    clean_place_name,
    fetch_pbf_street_data,
    find_settlement_boundary,
    is_hebrew,
    load_national_streets,
)


class TestPBFLoader(unittest.TestCase):

    def test_is_hebrew(self):
        self.assertTrue(is_hebrew("הרצל"))
        self.assertTrue(is_hebrew("Dizengoff דיזנגוף"))
        self.assertFalse(is_hebrew("Main Street"))
        self.assertFalse(is_hebrew(None))
        self.assertFalse(is_hebrew(""))

    def test_clean_place_name(self):
        self.assertEqual(clean_place_name("Tel Aviv-Yafo, Israel"), "Tel Aviv-Yafo")
        self.assertEqual(clean_place_name("חיפה, ישראל"), "חיפה")
        self.assertEqual(clean_place_name("עיריית ירושלים, ישראל"), "ירושלים")

    def test_load_national_streets(self):
        gdf = load_national_streets()
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        self.assertGreater(len(gdf), 10000)
        for col in ("osm_id", "osm_name", "geometry", "highway"):
            self.assertIn(col, gdf.columns)

    def test_fetch_pbf_street_data_by_bbox(self):
        # Tel Aviv bounding box
        place_dict = {
            "display_name": "Tel Aviv",
            "boundingbox": [32.05, 32.10, 34.75, 34.82],
        }
        gdf = fetch_pbf_street_data(place_dict)
        self.assertIsNotNone(gdf)
        self.assertGreater(len(gdf), 500)
        self.assertIn("osm_name", gdf.columns)
        self.assertIn("geometry", gdf.columns)

    def test_fetch_pbf_street_data_by_place_name(self):
        gdf = fetch_pbf_street_data("אבו גוש")
        self.assertIsNotNone(gdf)
        self.assertGreater(len(gdf), 0)
        self.assertTrue(any("גורדון" in str(name) or "השלום" in str(name) for name in gdf["osm_name"]))


if __name__ == "__main__":
    unittest.main()
