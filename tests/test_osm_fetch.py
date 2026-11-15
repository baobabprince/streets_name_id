import os
import tempfile
import unittest
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Polygon

import data_loader
import OSM_streets


class TestOSMFetch(unittest.TestCase):
    def test_fetch_simplifies_graph_and_preserves_hebrew_names(self):
        edges = gpd.GeoDataFrame(
            {
                "name": [["Main Road"], "רחוב בעברית"],
                "name:he": [["רחוב ראשי"], None],
                "highway": ["residential", "primary"],
                "geometry": [
                    LineString([(0, 0), (1, 0)]),
                    LineString([(1, 0), (2, 0)]),
                ],
            },
            index=pd.MultiIndex.from_tuples(
                [(1, 2, 0), (2, 3, 0)], names=["u", "v", "key"]
            ),
            crs="EPSG:4326",
        )

        with (
            patch.object(OSM_streets.ox, "graph_from_place", return_value=object()) as graph_from_place,
            patch.object(OSM_streets.ox, "graph_to_gdfs", return_value=edges),
        ):
            result = OSM_streets.fetch_osm_street_data("Test town")

        graph_from_place.assert_called_once_with(
            "Test town", network_type="all", simplify=True
        )
        self.assertEqual(result["osm_id"].tolist(), ["1-2-0", "2-3-0"])
        self.assertEqual(result["osm_name"].tolist(), ["רחוב ראשי", "רחוב בעברית"])
        self.assertEqual(result["osm_name_original"].tolist(), ["Main Road", "רחוב בעברית"])

    def test_failed_refresh_uses_existing_osm_cache(self):
        cached_data = gpd.GeoDataFrame(
            {
                "osm_id": ["1-2-0"],
                "osm_name": ["רחוב בדיקה"],
                "geometry": [LineString([(0, 0), (1, 0)])],
            },
            crs="EPSG:4326",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = os.path.join(temp_dir, "osm_data_Test.pkl")
            cached_data.to_pickle(cache_path)
            with (
                patch.object(data_loader, "OSM_CACHE_TEMPLATE", os.path.join(temp_dir, "osm_data_{place}.pkl")),
                patch.object(data_loader, "fetch_osm_street_data", return_value=None),
            ):
                result = data_loader.load_or_fetch_osm("Test", force_refresh=True)

        pd.testing.assert_frame_equal(result, cached_data)

    def test_failed_fetch_without_cache_raises_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch.object(data_loader, "OSM_CACHE_TEMPLATE", os.path.join(temp_dir, "osm_data_{place}.pkl")),
                patch.object(data_loader, "fetch_osm_street_data", return_value=None),
            ):
                with self.assertRaisesRegex(ValueError, "no usable street data"):
                    data_loader.load_or_fetch_osm("Test", force_refresh=True)

    def test_matched_osm_relation_uses_its_boundary_without_regeocoding_name(self):
        boundary = gpd.GeoDataFrame(
            geometry=[
                Polygon([(34, 31), (35, 31), (35, 32), (34, 32), (34, 31)])
            ],
            crs="EPSG:4326",
        )
        edges = gpd.GeoDataFrame(
            {
                "name": ["Main Road"],
                "highway": ["residential"],
                "geometry": [LineString([(34.1, 31.1), (34.2, 31.2)])],
            },
            index=pd.MultiIndex.from_tuples(
                [(1, 2, 0)], names=["u", "v", "key"]
            ),
            crs="EPSG:4326",
        )
        place = {
            "display_name": "Right Settlement, Israel",
            "osm_id": "123",
            "osm_type": "relation",
            "lat": 31.5,
            "lon": 34.5,
        }

        with (
            patch.object(OSM_streets.ox, "geocode_to_gdf", return_value=boundary) as lookup,
            patch.object(OSM_streets.ox, "graph_from_polygon", return_value=object()) as graph,
            patch.object(OSM_streets.ox, "graph_to_gdfs", return_value=edges),
            patch.object(OSM_streets.ox, "graph_from_place") as graph_from_place,
        ):
            result = OSM_streets.fetch_osm_street_data(place)

        lookup.assert_called_once_with("R123", by_osmid=True)
        graph.assert_called_once_with(
            boundary.geometry.iloc[0],
            network_type="all",
            simplify=True,
        )
        graph_from_place.assert_not_called()
        self.assertEqual(result["osm_name"].tolist(), ["Main Road"])

    def test_missing_name_column_is_created_for_valid_osm_graph(self):
        edges = gpd.GeoDataFrame(
            {
                "highway": ["residential"],
                "geometry": [LineString([(0, 0), (1, 1)])],
            },
            index=pd.MultiIndex.from_tuples(
                [(1, 2, 0)], names=["u", "v", "key"]
            ),
            crs="EPSG:4326",
        )
        with (
            patch.object(OSM_streets.ox, "graph_from_place", return_value=object()),
            patch.object(OSM_streets.ox, "graph_to_gdfs", return_value=edges),
        ):
            result = OSM_streets.fetch_osm_street_data("Test town")

        self.assertIn("osm_name", result.columns)
        self.assertTrue(result["osm_name"].isna().all())


if __name__ == "__main__":
    unittest.main()
