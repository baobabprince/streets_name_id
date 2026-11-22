import json
import os
import tempfile
import unittest
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, box

import osm_pbf_loader
from settlement_index import build_osm_places


class TestOSMSettlementIndex(unittest.TestCase):
    def test_find_settlement_boundary_uses_canonical_names_and_centres(self):
        frame = gpd.GeoDataFrame(
            {
                "osm_id": ["r1", "r2"],
                "name:he": ["תל אביב־יפו", "תל אביב־יפו"],
                "admin_level": ["8", "8"],
                "geometry": [
                    box(34.77, 32.07, 34.79, 32.09),
                    box(35.40, 31.00, 35.42, 31.02),
                ],
            },
            crs="EPSG:4326",
        )
        geometry = osm_pbf_loader.find_settlement_boundary(
            "תל אביב - יפו", boundaries_gdf=frame,
            expected_center=(34.78, 32.08),
        )
        self.assertIsNotNone(geometry)
        self.assertAlmostEqual(geometry.centroid.x, 34.78, places=2)

    def test_find_settlement_boundary_rejects_far_fuzzy_match(self):
        frame = gpd.GeoDataFrame(
            {
                "osm_id": ["r1"],
                "name:he": ["עפולה"],
                "admin_level": ["8"],
                "geometry": [box(35.40, 31.00, 35.42, 31.02)],
            },
            crs="EPSG:4326",
        )
        self.assertIsNone(
            osm_pbf_loader.find_settlement_boundary(
                "עפולה", boundaries_gdf=frame, expected_center=(35.28, 32.61)
            )
        )

    def test_boundary_lookup_ignores_neighbourhood_level_features(self):
        frame = gpd.GeoDataFrame([{
            "osm_id": "r9", "name:he": "שכונת בדיקה", "admin_level": "9",
            "geometry": box(34.98, 32.78, 35.00, 32.81),
        }], crs="EPSG:4326")
        self.assertIsNone(
            osm_pbf_loader.find_settlement_boundary(
                "שכונת בדיקה", boundaries_gdf=frame
            )
        )

    def test_boundary_lookup_ignores_district_scale_polygons(self):
        frame = gpd.GeoDataFrame([{
            "osm_id": "r6", "name:he": "מחוז בדיקה", "admin_level": "6",
            "geometry": box(34.0, 31.0, 35.5, 33.0),
        }], crs="EPSG:4326")
        self.assertIsNone(
            osm_pbf_loader.find_settlement_boundary(
                "מחוז בדיקה", boundaries_gdf=frame
            )
        )

    def test_place_area_skips_suburbs_and_non_place_features_unless_pinned(self):
        places = gpd.GeoDataFrame([{
            "osm_id": "n12", "name:he": "אבו כביר", "place": "suburb",
            "geometry": Point(34.80, 32.00),
        }, {
            "osm_id": "n13", "name:he": "בית בדיקה", "place": "house",
            "geometry": Point(34.81, 32.01),
        }], crs="EPSG:4326")
        self.assertIsNone(
            osm_pbf_loader.find_settlement_place_area("אבו כביר", places_gdf=places)
        )
        self.assertIsNone(
            osm_pbf_loader.find_settlement_place_area("בית בדיקה", places_gdf=places)
        )
        self.assertIsNotNone(
            osm_pbf_loader.find_settlement_place_area(
                "אבו כביר", places_gdf=places, allow_non_settlement=True
            )
        )

    def test_place_node_gets_non_degenerate_bbox(self):
        places = gpd.GeoDataFrame(
            {
                "osm_id": ["n42"], "name:he": ["נחלים"], "place": ["village"],
                "geometry": [Point(34.80, 32.00)],
            },
            crs="EPSG:4326",
        )
        record = build_osm_places(places=places)[0]
        min_lat, max_lat, min_lon, max_lon = record.bbox
        self.assertGreater(max_lat - min_lat, 0.01)
        self.assertGreater(max_lon - min_lon, 0.01)

    def test_degenerate_bbox_is_expanded_before_street_extraction(self):
        streets = gpd.GeoDataFrame(
            {
                "osm_id": ["s1"],
                "osm_name": ["רחוב ראשי"],
                "highway": ["residential"],
                "geometry": [LineString([(34.799, 32.0), (34.801, 32.0)])],
            },
            crs="EPSG:4326",
        )
        place = {
            "settlement_name": "נחלים", "display_name": "נחלים, Israel",
            "osm_id": "42", "osm_type": "node",
            "lat": 32.0, "lon": 34.8,
            "boundingbox": (32.0, 32.0, 34.8, 34.8),
        }
        with (
            patch.object(osm_pbf_loader, "load_national_streets", return_value=streets),
            patch.object(osm_pbf_loader, "load_national_boundaries",
                         side_effect=RuntimeError("no boundary cache")),
            patch.object(osm_pbf_loader, "load_national_places",
                         side_effect=RuntimeError("no places cache")),
        ):
            result = osm_pbf_loader.fetch_pbf_street_data(place)
        self.assertIsNotNone(result)
        self.assertEqual(result["osm_id"].tolist(), ["s1"])

    def test_tiny_bbox_is_expanded_before_street_extraction(self):
        streets = gpd.GeoDataFrame(
            {
                "osm_id": ["s2"],
                "osm_name": ["דרך עוקפת"],
                "highway": ["residential"],
                "geometry": [LineString([(34.805, 32.0), (34.806, 32.0)])],
            },
            crs="EPSG:4326",
        )
        place = {
            "settlement_name": "יישוב קטן", "display_name": "יישוב קטן, Israel",
            "osm_id": "43", "osm_type": "node",
            "lat": 32.0, "lon": 34.8,
            "boundingbox": (32.0, 32.0002, 34.8, 34.8002),
        }
        with (
            patch.object(osm_pbf_loader, "load_national_streets", return_value=streets),
            patch.object(osm_pbf_loader, "load_national_boundaries",
                         side_effect=RuntimeError("no boundary cache")),
            patch.object(osm_pbf_loader, "load_national_places",
                         side_effect=RuntimeError("no places cache")),
        ):
            result = osm_pbf_loader.fetch_pbf_street_data(place)
        self.assertIsNotNone(result)
        self.assertEqual(result["osm_id"].tolist(), ["s2"])

    def test_place_polygon_is_used_verbatim_without_inflating_to_square(self):
        village_poly = box(34.800, 32.000, 34.803, 32.003)
        places = gpd.GeoDataFrame(
            {
                "osm_id": ["w94713250"],
                "osm_type": ["w"],
                "name:he": ["גליל ים"],
                "place": ["village"],
                "geometry": [village_poly],
            },
            crs="EPSG:4326",
        )
        streets = gpd.GeoDataFrame(
            {
                "osm_id": ["inside", "neighbor_city"],
                "osm_name": ["רחוב הקיבוץ", "רחוב הרצליה"],
                "highway": ["residential", "residential"],
                "geometry": [
                    LineString([(34.801, 32.001), (34.802, 32.002)]),
                    LineString([(34.808, 32.001), (34.809, 32.002)]),
                ],
            },
            crs="EPSG:4326",
        )
        place = {
            "settlement_name": "גליל ים",
            "display_name": "גליל ים, Israel",
            "osm_id": "94713250",
            "osm_type": "way",
            "lat": 32.0015,
            "lon": 34.8015,
            "boundingbox": (32.000, 32.003, 34.800, 34.803),
        }
        with (
            patch.object(osm_pbf_loader, "load_national_streets", return_value=streets),
            patch.object(
                osm_pbf_loader,
                "load_national_boundaries",
                return_value=gpd.GeoDataFrame(columns=["osm_id", "geometry"], crs="EPSG:4326"),
            ),
            patch.object(osm_pbf_loader, "load_national_places", return_value=places),
        ):
            result = osm_pbf_loader.fetch_pbf_street_data(place)
        self.assertIsNotNone(result)
        self.assertEqual(result["osm_id"].tolist(), ["inside"])

    def test_settlement_index_prefers_polygon_over_closer_node_and_rejects_regional_councils(self):
        from settlement_index import SettlementNameIndex

        boundaries = gpd.GeoDataFrame(
            {
                "osm_id": ["r1", "r2"],
                "admin_level": ["8", "5"],
                "name:he": ["מועצה אזורית שומרון", "נפת כנרת"],
                "geometry": [
                    box(35.0, 32.1, 35.3, 32.4),
                    box(35.4, 32.6, 35.7, 32.9),
                ],
            },
            crs="EPSG:4326",
        )
        places = gpd.GeoDataFrame(
            {
                "osm_id": ["n10", "w20"],
                "osm_type": ["n", "w"],
                "name:he": ["כפר מל\"ל", "כפר מל\"ל"],
                "place": ["village", "village"],
                "geometry": [
                    Point(34.9000, 32.1700),
                    box(34.8990, 32.1690, 34.9040, 32.1730),  # centroid slightly farther than node
                ],
            },
            crs="EPSG:4326",
        )
        osm_places = build_osm_places(boundaries=boundaries, places=places)
        index = SettlementNameIndex(osm_places)

        # Polygon wins over node even when node is right on top of the LAMAS centre
        decision = index.decide(['כפר מל"ל'], center=(34.9000, 32.1700))
        self.assertIsNotNone(decision.place)
        self.assertEqual(decision.place.osm_type, "way")
        self.assertTrue(decision.place.has_polygon)

        # Regional councils & subdistricts must never win a settlement match
        rc_decision = index.decide(["שער שומרון"], center=(35.15, 32.25))
        self.assertIsNone(rc_decision.place)
        sub_decision = index.decide(["כנרת"], center=(35.55, 32.75))
        self.assertIsNone(sub_decision.place)

    def test_place_point_creates_offline_search_area(self):
        places = gpd.GeoDataFrame(
            {
                "osm_id": ["n42"],
                "name:he": ["נחלים"],
                "place": ["village"],
                "geometry": [Point(34.80, 32.00)],
            },
            crs="EPSG:4326",
        )
        area = osm_pbf_loader.find_settlement_place_area(
            "נחלים", places_gdf=places, expected_center=(34.80, 32.00)
        )
        self.assertIsNotNone(area)
        self.assertTrue(area.contains(Point(34.80, 32.00)))

    def test_extract_places_parses_geojson_sequence_and_cleans_temp_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pbf_path = os.path.join(temp_dir, "region.osm.pbf")
            places_pkl = os.path.join(temp_dir, "places.pkl")
            with open(pbf_path, "wb") as handle:
                handle.write(b"pbf")

            def fake_export(command, *args, **kwargs):
                output = command[command.index("-o") + 1]
                records = [
                    {
                        "properties": {
                            "@id": "n42", "name:he": "נחלים", "place": "village",
                        },
                        "geometry": {"type": "Point", "coordinates": [34.8, 32.0]},
                    },
                    {
                        "properties": {"@id": "n43", "highway": "residential"},
                        "geometry": {"type": "Point", "coordinates": [34.9, 32.0]},
                    },
                ]
                with open(output, "w", encoding="utf-8") as handle:
                    for record in records:
                        handle.write("\x1e" + json.dumps(record, ensure_ascii=False) + "\n")
                return None

            with (
                patch.object(osm_pbf_loader, "DATA_DIR", temp_dir),
                patch.object(osm_pbf_loader, "_find_osmium_executable",
                             return_value="/usr/bin/osmium"),
                patch.object(osm_pbf_loader, "_run_osmium_with_pbf_retry"),
                patch.object(osm_pbf_loader.subprocess, "run", side_effect=fake_export),
                patch.object(osm_pbf_loader, "download_osm_pbf"),
            ):
                result = osm_pbf_loader.extract_places_to_cache(
                    pbf_path=pbf_path, places_pkl=places_pkl, force=True
                )

            self.assertEqual(result, places_pkl)
            frame = pd.read_pickle(places_pkl)
            self.assertEqual(frame["osm_id"].tolist(), ["n42"])
            self.assertEqual(frame["osm_type"].tolist(), ["n"])
            self.assertEqual(frame["name:he"].tolist(), ["נחלים"])
            self.assertFalse(
                os.path.exists(os.path.join(temp_dir, "israel_places.geojsonseq"))
            )

    def test_prepare_national_caches_builds_all_three_indexes(self):
        with (
            patch.object(osm_pbf_loader, "extract_pbf_to_cache",
                         return_value=("streets.pkl", "boundaries.pkl")) as extract,
            patch.object(osm_pbf_loader, "extract_places_to_cache",
                         return_value="places.pkl") as extract_places,
        ):
            caches = osm_pbf_loader.prepare_national_caches(
                pbf_path="region.osm.pbf", force=True
            )
        self.assertEqual(set(caches), {"streets", "boundaries", "places"})
        extract.assert_called_once_with(pbf_path="region.osm.pbf", force=True)
        extract_places.assert_called_once_with(
            pbf_path="region.osm.pbf", force=True
        )


if __name__ == "__main__":
    unittest.main()
