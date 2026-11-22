from types import SimpleNamespace
import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import LineString

import osm_pbf_loader
import settlement_matcher


class TestPBFPlaceFallback(unittest.TestCase):
    def test_string_place_uses_geocoded_area_when_no_boundary_exists(self):
        streets = gpd.GeoDataFrame(
            {
                "osm_id": ["nearby", "distant"],
                "osm_name": ["רחוב קרוב", "רחוב רחוק"],
                "highway": ["residential", "residential"],
                "geometry": [
                    LineString([(34.79, 31.99), (34.80, 31.99)]),
                    LineString([(34.90, 32.00), (34.91, 32.00)]),
                ],
            },
            crs="EPSG:4326",
        )

        with (
            patch.object(osm_pbf_loader, "load_national_streets", return_value=streets),
            patch.object(osm_pbf_loader, "find_settlement_boundary", return_value=None),
            patch.object(osm_pbf_loader, "find_settlement_place_area", return_value=None),
            patch.object(settlement_matcher, "SettlementMatcher") as matcher_class,
        ):
            matcher_class.return_value.search_settlement.return_value = SimpleNamespace(
                lat=32.0,
                lon=34.8,
            )
            result = osm_pbf_loader.fetch_pbf_street_data("נחלים, Israel")

        self.assertIsNotNone(result)
        self.assertEqual(result["osm_id"].tolist(), ["nearby"])
        self.assertEqual(result["city"].tolist(), ["נחלים"])
        matcher_class.return_value.search_settlement.assert_called_once_with("נחלים")


if __name__ == "__main__":
    unittest.main()
