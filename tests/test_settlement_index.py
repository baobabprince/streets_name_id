import csv
import os
import tempfile
import unittest
from unittest.mock import Mock, call, patch

import geopandas as gpd
from shapely.geometry import Point, box

from settlement_index import (
    SettlementMatchStore,
    SettlementNameIndex,
    SettlementResolver,
    build_osm_places,
    load_settlement_centers,
    match_settlements_offline,
    write_match_report,
)
from settlement_matcher import SettlementMatch
from settlement_names import canonical_key, load_aliases


def boundaries_frame(rows):
    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


class TestSettlementIndex(unittest.TestCase):
    def test_exact_canonical_match_and_geo_tie_break(self):
        # Same settlement name twice: only the one near the LAMAS centre wins.
        frame = boundaries_frame([
            {
                "osm_id": "r1", "name:he": "חיפה", "name": "Haifa",
                "admin_level": "8",
                "geometry": box(34.98, 32.78, 35.00, 32.81),
            },
            {
                "osm_id": "r2", "name:he": "חיפה", "name": "Haifa",
                "admin_level": "8",
                "geometry": box(35.40, 31.00, 35.42, 31.02),
            },
        ])
        index = SettlementNameIndex(build_osm_places(frame))
        centers = {canonical_key("חיפה"): (34.99, 32.79)}
        matches = match_settlements_offline(["חיפה"], index, centers=centers, aliases={})

        self.assertIn("חיפה", matches)
        match = matches["חיפה"]
        self.assertEqual(match.osm_id, "1")
        self.assertEqual(match.osm_type, "relation")
        self.assertEqual(match.match_source, "pbf")
        self.assertEqual(match.name_score, 100.0)
        self.assertLess(match.center_distance_m, 3000)

    def test_lamas_dash_spelling_matches_osm_maqaf_name(self):
        frame = boundaries_frame([{
            "osm_id": "r7", "name:he": "תל אביב־יפו", "name": "Tel Aviv-Yafo",
            "admin_level": "8",
            "geometry": box(34.77, 32.07, 34.79, 32.09),
        }])
        index = SettlementNameIndex(build_osm_places(frame))
        centers = {canonical_key("תל אביב - יפו"): (34.78, 32.08)}
        matches = match_settlements_offline(
            ["תל אביב - יפו"], index, centers=centers, aliases=load_aliases()
        )
        self.assertIn("תל אביב - יפו", matches)

    def test_renamed_settlement_resolves_via_curated_alias(self):
        frame = boundaries_frame([{
            "osm_id": "n55", "osm_type": "node", "name:he": "נצרת עילית",
            "place": "town", "geometry": Point(35.3306, 32.7207),
        }])
        index = SettlementNameIndex(build_osm_places(places=frame))
        matches = match_settlements_offline(
            ["נוף הגליל"], index,
            centers=load_settlement_centers(), aliases=load_aliases(),
        )
        self.assertIn("נוף הגליל", matches)
        self.assertEqual(matches["נוף הגליל"].osm_id, "55")

    def test_candidate_far_from_centre_is_rejected(self):
        frame = boundaries_frame([{
            "osm_id": "r99", "name:he": "עפולה", "admin_level": "8",
            "geometry": box(35.40, 31.00, 35.42, 31.02),
        }])
        index = SettlementNameIndex(build_osm_places(frame))
        centers = {canonical_key("עפולה"): (35.28, 32.61)}
        self.assertEqual(
            match_settlements_offline(["עפולה"], index, centers=centers, aliases={}),
            {},
        )

    def test_duplicate_osm_id_is_not_assigned_to_two_settlements(self):
        frame = boundaries_frame([{
            "osm_id": "r9", "name:he": "יישוב א", "name": "יישוב ב",
            "admin_level": "8", "geometry": box(34.99, 32.79, 35.00, 32.80),
        }])
        index = SettlementNameIndex(build_osm_places(frame))
        centers = {
            canonical_key("יישוב א"): (34.995, 32.795),  # exact centre
            canonical_key("יישוב ב"): (35.05, 32.85),    # ~7 km away
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            resolver = SettlementResolver(
                store=SettlementMatchStore(os.path.join(temp_dir, "store.json")),
                offline_index=index, use_pbf=False, allow_nominatim=False,
                centers=centers, aliases={},
            )
            result = resolver.resolve(["יישוב א", "יישוב ב"])

        self.assertEqual(result.stats["matched"], 1)
        self.assertIn("יישוב א", result.matches)
        self.assertIn("Duplicate OSM id", result.unresolved["יישוב ב"])

    def test_resolve_reuses_store_and_writes_once(self):
        frame = boundaries_frame([{
            "osm_id": "r1", "name:he": "חיפה", "admin_level": "8",
            "geometry": box(34.98, 32.78, 35.00, 32.81),
        }])
        index = SettlementNameIndex(build_osm_places(frame))
        centers = {canonical_key("חיפה"): (34.99, 32.79)}
        nominatim_match = SettlementMatch(
            settlement_name="פלוני", osm_id="42", display_name="פלוני, Israel",
            lat=32.0, lon=34.8, boundingbox=(31.9, 32.1, 34.7, 34.9),
            place_type="village", importance=0.0, is_valid=True,
            validation_message="ok", osm_type="node", category="place",
            country_code="il", name_score=100.0,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            store_path = os.path.join(temp_dir, "store.json")
            store = SettlementMatchStore(store_path)
            matcher = Mock()
            matcher.search_settlement.return_value = nominatim_match
            with patch.object(store, "save", wraps=store.save) as save:
                resolver = SettlementResolver(
                    store=store, offline_index=index, use_pbf=False,
                    allow_nominatim=True, matcher=matcher,
                    nominatim_cache=Mock(), centers=centers, aliases={},
                )
                first = resolver.resolve(["חיפה", "פלוני"])

            self.assertEqual(save.call_count, 1)
            self.assertEqual(first.stats["matched_offline"], 1)
            self.assertEqual(first.stats["matched_nominatim"], 1)
            matcher.search_settlement.assert_has_calls([call("פלוני")])
            self.assertEqual(matcher.search_settlement.call_count, 1)

            # A second run reads the single consolidated file, no network.
            second_matcher = Mock()
            second = SettlementResolver(
                store=SettlementMatchStore(store_path),
                offline_index=index, use_pbf=False,
                allow_nominatim=True, matcher=second_matcher,
                nominatim_cache=Mock(), centers=centers, aliases={},
            ).resolve(["חיפה", "פלוני"])
            self.assertEqual(second.stats["reused_from_store"], 2)
            second_matcher.search_settlement.assert_not_called()
            self.assertEqual(set(second.matches), {"חיפה", "פלוני"})

    def test_nominatim_result_far_from_lamas_centre_is_rejected(self):
        far_match = SettlementMatch(
            settlement_name="חיפה", osm_id="42", display_name="חיפה הדרומית",
            lat=31.0, lon=35.4, boundingbox=(30.9, 31.1, 35.3, 35.5),
            place_type="village", importance=0.0, is_valid=True,
            validation_message="ok", osm_type="node", category="place",
            country_code="il",
        )
        matcher = Mock()
        matcher.search_settlement.return_value = far_match
        with tempfile.TemporaryDirectory() as temp_dir:
            resolver = SettlementResolver(
                store=SettlementMatchStore(os.path.join(temp_dir, "store.json")),
                offline_index=None, use_pbf=False, allow_nominatim=True,
                matcher=matcher, nominatim_cache=Mock(), aliases={},
            )
            result = resolver.resolve(["חיפה"])
        self.assertNotIn("חיפה", result.matches)
        self.assertIn("LAMAS centre", result.unresolved["חיפה"])

    def test_nominatim_rejection_reason_is_written_to_the_audit(self):
        matcher = Mock()
        matcher.search_settlement.return_value = None
        matcher.last_rejection_reason = (
            "Non-settlement OSM feature (place/suburb): חיפה, Israel"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            result = SettlementResolver(
                store=SettlementMatchStore(os.path.join(temp_dir, "store.json")),
                offline_index=None, use_pbf=False, allow_nominatim=True,
                matcher=matcher, nominatim_cache=Mock(), centers={}, aliases={},
            ).resolve(["חיפה"])
        self.assertIn("Non-settlement OSM feature", result.unresolved["חיפה"])

    def test_suburb_does_not_steal_city_match(self):
        # A suburb with the *exact* same name sits even closer to the LAMAS
        # centre than the real city boundary: the city must still win.
        boundaries = boundaries_frame([{
            "osm_id": "r1", "name:he": "חיפה", "admin_level": "8",
            "geometry": box(34.98, 32.78, 35.00, 32.81),
        }])
        places = boundaries_frame([{
            "osm_id": "n2", "name:he": "חיפה", "place": "suburb",
            "geometry": Point(34.99, 32.79),
        }])
        index = SettlementNameIndex(build_osm_places(boundaries, places))
        centers = {canonical_key("חיפה"): (34.99, 32.79)}
        matches = match_settlements_offline(
            ["חיפה"], index, centers=centers, aliases={}, pinned={}
        )

        self.assertEqual(matches["חיפה"].osm_id, "1")
        self.assertEqual(matches["חיפה"].osm_type, "relation")
        self.assertIn("ignored", matches["חיפה"].validation_message)
        self.assertIn("suburb", matches["חיפה"].validation_message)

    def test_only_suburb_candidate_is_rejected_with_reason(self):
        places = boundaries_frame([{
            "osm_id": "n3", "name:he": "אבו כביר", "place": "neighbourhood",
            "geometry": Point(34.78, 32.05),
        }])
        index = SettlementNameIndex(build_osm_places(places=places))
        rejections: dict = {}
        matches = match_settlements_offline(
            ["אבו כביר"], index, centers={}, aliases={}, pinned={},
            rejections=rejections,
        )
        self.assertEqual(matches, {})
        self.assertIn("non-settlement", rejections["אבו כביר"].lower())
        self.assertIn("neighbourhood", rejections["אבו כביר"])

    def test_neighbourhood_inside_another_city_is_marked_and_rejected(self):
        boundaries = boundaries_frame([{
            "osm_id": "r10", "name:he": "תל אביב־יפו", "admin_level": "8",
            "geometry": box(34.75, 32.03, 34.82, 32.10),
        }])
        places = boundaries_frame([{
            "osm_id": "n11", "name:he": "אבו כביר", "place": "suburb",
            "geometry": Point(34.78, 32.05),
        }])
        records = build_osm_places(boundaries, places)
        suburb = next(p for p in records if p.osm_id == "11")
        self.assertFalse(suburb.is_settlement)
        self.assertTrue(suburb.inside_city)

        index = SettlementNameIndex(records)
        self.assertNotIn(
            "אבו כביר",
            match_settlements_offline(
                ["אבו כביר"], index, centers={}, aliases={}, pinned={}
            ),
        )

    def test_admin_level_9_and_district_scale_boundaries_are_not_settlements(self):
        frame = boundaries_frame([
            {
                "osm_id": "r20", "name:he": "שכונת בדיקה", "admin_level": "9",
                "geometry": box(34.98, 32.78, 35.00, 32.81),
            },
            {
                "osm_id": "r21", "name:he": "מחוז בדיקה", "admin_level": "6",
                "geometry": box(34.0, 31.0, 35.5, 33.0),  # > 0.6° span
            },
        ])
        records = build_osm_places(frame)
        self.assertFalse(records[0].is_settlement)
        self.assertFalse(records[1].is_settlement)
        index = SettlementNameIndex(records)
        rejections: dict = {}
        self.assertEqual(
            match_settlements_offline(
                ["שכונת בדיקה"], index, centers={}, aliases={}, pinned={},
                rejections=rejections,
            ),
            {},
        )
        self.assertIn("non-settlement", rejections["שכונת בדיקה"].lower())

    def test_pinned_override_is_the_only_way_to_accept_a_suburb(self):
        places = boundaries_frame([{
            "osm_id": "n77", "name:he": "יישוב חריג", "place": "suburb",
            "geometry": Point(34.80, 32.00),
        }])
        index = SettlementNameIndex(build_osm_places(places=places))
        pinned = {canonical_key("יישוב חריג"): "n77"}
        matches = match_settlements_offline(
            ["יישוב חריג"], index, centers={}, aliases={}, pinned=pinned
        )
        self.assertIn("יישוב חריג", matches)
        self.assertEqual(matches["יישוב חריג"].osm_id, "77")
        self.assertEqual(matches["יישוב חריג"].match_source, "pinned")
        self.assertIn("pinned override", matches["יישוב חריג"].validation_message)

    def test_an_exact_name_on_a_non_place_feature_cannot_win(self):
        places = boundaries_frame([{
            "osm_id": "n88", "name:he": "יישוב בדיקה", "place": "house",
            "geometry": Point(34.80, 32.00),
        }])
        index = SettlementNameIndex(build_osm_places(places=places))
        rejections: dict = {}
        self.assertEqual(
            match_settlements_offline(
                ["יישוב בדיקה"], index, centers={}, aliases={}, pinned={},
                rejections=rejections,
            ),
            {},
        )
        self.assertIn("only non-settlement", rejections["יישוב בדיקה"].lower())

    def test_typed_pin_must_match_the_exact_osm_object(self):
        places = boundaries_frame([{
            "osm_id": "n77", "name:he": "יישוב חריג", "place": "suburb",
            "geometry": Point(34.80, 32.00),
        }])
        index = SettlementNameIndex(build_osm_places(places=places))
        self.assertEqual(index.decide(["יישוב חריג"], None, pinned_id="n77").place.osm_id, "77")
        self.assertEqual(
            index.decide(["יישוב חריג"], None, pinned_id="r77").reason,
            "Pinned OSM id 'r77' is not in the local OSM index",
        )
        self.assertIn(
            "too far",
            index.decide(["יישוב חריג"], (35.0, 31.0), pinned_id="n77").reason,
        )

    def test_offline_rejection_reason_is_retained_when_nominatim_succeeds(self):
        # The offline index may only know a suburb; Nominatim can still return
        # the real town.  It must be a match, not also an unresolved entry.
        places = boundaries_frame([{
            "osm_id": "n3", "name:he": "אבו כביר", "place": "neighbourhood",
            "geometry": Point(34.78, 32.05),
        }])
        index = SettlementNameIndex(build_osm_places(places=places))
        real_town = SettlementMatch(
            settlement_name="אבו כביר", osm_id="42",
            display_name="אבו כביר, Israel", lat=32.05, lon=34.78,
            boundingbox=(32.04, 32.06, 34.77, 34.79), place_type="town",
            importance=0.0, is_valid=True, validation_message="ok",
            osm_type="relation", category="boundary", country_code="il",
        )
        matcher = Mock()
        matcher.search_settlement.return_value = real_town
        with tempfile.TemporaryDirectory() as temp_dir:
            result = SettlementResolver(
                store=SettlementMatchStore(os.path.join(temp_dir, "store.json")),
                offline_index=index, use_pbf=False, allow_nominatim=True,
                matcher=matcher, nominatim_cache=Mock(), centers={}, aliases={},
            ).resolve(["אבו כביר"])
        self.assertIn("אבו כביר", result.matches)
        self.assertNotIn("אבו כביר", result.unresolved)
        self.assertEqual(result.stats["matched_nominatim"], 1)
        self.assertEqual(result.stats["rejected_non_settlement"], 1)

    def test_match_report_covers_every_settlement(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "report.csv")
            write_match_report(
                ["א", "ב"],
                {"א": {
                    "match_source": "pbf", "osm_id": "1", "display_name": "א",
                    "validation_message": "ignored 1 suburb",
                }},
                {"ב": "no local match"},
                path=path,
            )
            with open(path, encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual([row["settlement"] for row in rows], ["א", "ב"])
        self.assertEqual(rows[0]["status"], "matched")
        self.assertEqual(rows[0]["validation_message"], "ignored 1 suburb")
        self.assertEqual(rows[1]["reason"], "no local match")


if __name__ == "__main__":
    unittest.main()
