import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import LineString

import OSM_streets as osm_streets
from settlement_names import (
    MAX_SETTLEMENT_BBOX_SPAN_DEG,
    MIN_SETTLEMENT_HALF_HEIGHT_LAT,
    MIN_SETTLEMENT_HALF_WIDTH_LON,
)


class _Graph:
    def __init__(self, edge_count=1):
        self.edges = list(range(edge_count))


class TestOSMStreetsFallback(unittest.TestCase):
    def test_tiny_bbox_is_expanded_and_large_bbox_is_capped(self):
        place = {
            "boundingbox": (32.0, 32.0, 34.8, 34.8),
            "lat": 32.0,
            "lon": 34.8,
        }
        bounds = osm_streets._bbox_bounds(place)
        self.assertIsNotNone(bounds)
        min_lon, min_lat, max_lon, max_lat = bounds
        self.assertAlmostEqual((max_lat - min_lat) / 2, MIN_SETTLEMENT_HALF_HEIGHT_LAT)
        self.assertAlmostEqual((max_lon - min_lon) / 2, MIN_SETTLEMENT_HALF_WIDTH_LON)

        huge = {"boundingbox": (29.0, 34.0, 33.5, 36.0)}
        huge_bounds = osm_streets._bbox_bounds(huge)
        self.assertAlmostEqual(
            huge_bounds[3] - huge_bounds[1], MAX_SETTLEMENT_BBOX_SPAN_DEG
        )
        self.assertAlmostEqual(
            huge_bounds[2] - huge_bounds[0], MAX_SETTLEMENT_BBOX_SPAN_DEG
        )

    def test_place_dict_fallback_uses_bbox_not_graph_from_place(self):
        place = {
            "settlement_name": "אבו עמאר )שבט(",
            "display_name": "אבו עמאר, Israel",
            "lat": 31.1373783,
            "lon": 34.9532604,
            "boundingbox": (31.1223783, 31.1523783, 34.9352604, 34.9712604),
            "osm_id": "4944470650",
            "osm_type": "",
        }
        with (
            patch.object(osm_streets.ox, "graph_from_polygon", return_value=_Graph()) as polygon,
            patch.object(osm_streets.ox, "graph_from_place") as by_name,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
        ):
            graph = osm_streets._graph_from_place_dict(place)
        self.assertIsInstance(graph, _Graph)
        polygon.assert_called_once()
        by_name.assert_not_called()
        by_point.assert_not_called()
        call_kwargs = polygon.call_args.kwargs
        self.assertEqual(call_kwargs["network_type"], "all")
        self.assertTrue(call_kwargs["simplify"])
        called_polygon = polygon.call_args.args[0]
        self.assertGreater(called_polygon.area, 0)

    def test_empty_graph_tries_expanded_bbox_before_geocoding(self):
        place = {
            "display_name": "יישוב קטן, Israel",
            "lat": 31.2,
            "lon": 34.9,
            "boundingbox": (31.199, 31.201, 34.899, 34.901),
        }
        with (
            patch.object(
                osm_streets.ox,
                "graph_from_polygon",
                side_effect=[_Graph(0), _Graph(1)],
            ) as polygon,
            patch.object(osm_streets.ox, "graph_from_place") as by_name,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
        ):
            graph = osm_streets._graph_from_place_dict(place)
        self.assertEqual(len(graph.edges), 1)
        self.assertEqual(polygon.call_count, 2)
        first, second = [call.args[0] for call in polygon.call_args_list]
        self.assertGreater(second.area, first.area)
        by_name.assert_not_called()
        by_point.assert_not_called()

    def test_fetch_uses_dict_fallback_when_pbf_is_disabled(self):
        edges = gpd.GeoDataFrame({
            "u": [1], "v": [2], "key": [0],
            "name": ["הגפן"], "highway": ["residential"],
            "geometry": [LineString([(34.9, 31.2), (34.91, 31.21)])],
        }, crs="EPSG:4326")
        place = {
            "display_name": "יישוב קטן, Israel",
            "lat": 31.2,
            "lon": 34.9,
            "boundingbox": (31.19, 31.21, 34.89, 34.91),
        }
        with (
            patch.object(osm_streets, "_graph_from_place_dict", return_value=_Graph()) as fallback,
            patch.object(osm_streets.ox, "graph_from_place") as by_name,
            patch.object(osm_streets.ox, "graph_to_gdfs", return_value=edges) as to_gdfs,
        ):
            result = osm_streets.fetch_osm_street_data(place, use_pbf=False)
        fallback.assert_called_once_with(place)
        by_name.assert_not_called()
        to_gdfs.assert_called_once()
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["osm_name"], "הגפן")


if __name__ == "__main__":
    unittest.main()
