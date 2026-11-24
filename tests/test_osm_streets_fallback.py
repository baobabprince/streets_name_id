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

    def test_empty_graph_is_not_retried_with_other_strategies(self):
        """A PBF miss is a data issue: one Overpass query, no escalation."""
        place = {
            "display_name": "יישוב קטן, Israel",
            "lat": 31.2,
            "lon": 34.9,
            "boundingbox": (31.199, 31.201, 34.899, 34.901),
            "osm_id": "123",
            "osm_type": "node",
        }
        with (
            patch.object(osm_streets.ox, "graph_from_polygon", return_value=_Graph(0)) as polygon,
            patch.object(osm_streets.ox, "graph_from_place") as by_name,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
            patch.object(osm_streets.ox, "geocode_to_gdf") as by_id,
        ):
            graph = osm_streets._graph_from_place_dict(place)
        self.assertIsNone(graph)
        polygon.assert_called_once()
        by_name.assert_not_called()
        by_point.assert_not_called()
        by_id.assert_not_called()

    def test_no_data_error_is_treated_as_empty_without_retry(self):
        place = {"lat": 31.2, "lon": 34.9, "boundingbox": (31.19, 31.21, 34.89, 34.91)}
        with (
            patch.object(
                osm_streets.ox,
                "graph_from_polygon",
                side_effect=ValueError("No data elements in server response."),
            ) as polygon,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
        ):
            self.assertIsNone(osm_streets._graph_from_place_dict(place))
        polygon.assert_called_once()
        by_point.assert_not_called()

    def test_point_used_only_when_no_bbox_and_nothing_without_coordinates(self):
        with patch.object(osm_streets.ox, "graph_from_point", return_value=_Graph()) as by_point:
            self.assertIsNotNone(osm_streets._graph_from_place_dict({"lat": 31.2, "lon": 34.9}))
        by_point.assert_called_once()
        with (
            patch.object(osm_streets.ox, "graph_from_place") as by_name,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
        ):
            self.assertIsNone(osm_streets._graph_from_place_dict({"display_name": "X, Israel"}))
        by_name.assert_not_called()
        by_point.assert_not_called()

    def test_fetch_after_pbf_miss_makes_one_short_overpass_attempt(self):
        place = {"lat": 31.2, "lon": 34.9, "boundingbox": (31.19, 31.21, 34.89, 34.91)}
        seen = {}

        def fake_polygon(*args, **kwargs):
            seen["timeout"] = osm_streets.ox.settings.requests_timeout
            seen["rate_limit"] = osm_streets.ox.settings.overpass_rate_limit
            raise ConnectionError("read timed out")

        before = osm_streets.ox.settings.requests_timeout
        with (
            patch.dict(osm_streets.os.environ, {"OSM_OVERPASS_TIMEOUT": "12"}),
            patch("osm_pbf_loader.fetch_pbf_street_data", return_value=None),
            patch.object(osm_streets.ox, "graph_from_polygon", side_effect=fake_polygon) as polygon,
            patch.object(osm_streets.ox, "graph_from_point") as by_point,
        ):
            result = osm_streets.fetch_osm_street_data(place)
        self.assertIsNone(result)
        polygon.assert_called_once()
        by_point.assert_not_called()
        self.assertEqual(seen, {"timeout": 12, "rate_limit": False})
        self.assertEqual(osm_streets.ox.settings.requests_timeout, before)

    def test_osmnx_429_retry_loop_is_cut_short(self):
        from osmnx import _overpass

        calls = []

        def fake_request(data, pause=None, error_pause=60):
            calls.append(error_pause)
            # Simulate OSMnx's recursive retry on HTTP 429 with the same payload.
            return _overpass._overpass_request(data, pause, error_pause)

        with patch.object(_overpass, "_overpass_request", fake_request):
            with osm_streets._limited_overpass():
                with self.assertRaises(osm_streets.OverpassNoRetryError):
                    _overpass._overpass_request(data={"data": "q"})
                # A *different* payload (e.g. subdivided polygon) is still allowed.
                with self.assertRaises(osm_streets.OverpassNoRetryError):
                    _overpass._overpass_request(data={"data": "q2"})
            self.assertIs(_overpass._overpass_request, fake_request)
        self.assertEqual(calls, [0, 0])

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
