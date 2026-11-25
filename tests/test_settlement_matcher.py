import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from settlement_matcher import NominatimCache, SettlementMatcher
from settlement_names import canonical_key


def place_result(
    name,
    *,
    category="place",
    place_type="village",
    country_code="il",
    display_name=None,
):
    return {
        "osm_id": 123,
        "osm_type": "way",
        "name": name,
        "category": category,
        "type": place_type,
        "lat": "31.5",
        "lon": "34.8",
        "boundingbox": ["31.4", "31.6", "34.7", "34.9"],
        "display_name": display_name or f"{name}, Israel",
        "address": {"country_code": country_code},
    }


class TestSettlementMatcher(unittest.TestCase):
    def setUp(self):
        self.matcher = SettlementMatcher()

    def test_normalizes_reversed_lamas_tribe_annotation(self):
        self.assertEqual(
            self.matcher.normalize_settlement_name("אבו ג'ווייעד )שבט("),
            "אבו ג'ווייעד",
        )

    def test_rejects_geographically_valid_but_wrong_settlement(self):
        result = place_result(
            "אבו כביר",
            display_name="אבו כביר, תל אביב, ישראל",
        )

        valid, reason = self.matcher._validate_result(
            result, "אבו ג'ווייעד )שבט(", ["אבו גווייעד"]
        )

        self.assertFalse(valid)
        self.assertIn("does not match", reason)

    def test_accepts_verified_name_alias_for_a_place_feature(self):
        result = place_result("איבטין", display_name="איבטין, ישראל")
        result["namedetails"] = {"name:en": "Ibtin"}

        valid, _ = self.matcher._validate_result(
            result, "אבטין", self.matcher._query_variants("אבטין")
        )

        self.assertTrue(valid)

    def test_accepts_exact_arabic_settlement_name_on_residential_area(self):
        result = place_result(
            "أبو ربية",
            category="landuse",
            place_type="residential",
            display_name="أبو ربية, إسرائيل",
        )
        result["namedetails"] = {"name:ar": "أبو ربية"}

        valid, _ = self.matcher._validate_result(
            result,
            "אבו רובייעה )שבט(",
            self.matcher._query_variants("אבו רובייעה )שבט("),
        )

        self.assertTrue(valid)

    def test_rejects_matching_name_on_non_settlement_feature(self):
        result = place_result(
            "אבו ג'ווייעד",
            category="highway",
            place_type="residential",
        )

        valid, reason = self.matcher._validate_result(
            result, "אבו ג'ווייעד"
        )

        self.assertFalse(valid)
        self.assertIn("Not a settlement feature", reason)

    def test_rejects_exact_name_outside_allowed_countries(self):
        result = place_result("אבו ג'ווייעד", country_code="sy")

        valid, reason = self.matcher._validate_result(
            result, "אבו ג'ווייעד"
        )

        self.assertFalse(valid)
        self.assertIn("Country is not Israel/Palestine", reason)

    def test_does_not_match_parent_city_address_to_feature_name(self):
        result = place_result("יישוב אחר", display_name="יישוב אחר, אבו סנאן, ישראל")
        result["address"]["village"] = "אבו סנאן"

        valid, reason = self.matcher._validate_result(result, "אבו סנאן")

        self.assertFalse(valid)
        self.assertIn("does not match", reason)

    def test_rejects_a_neighbourhood_that_shares_the_settlement_name(self):
        for place_type in ("suburb", "neighbourhood", "quarter", "borough"):
            with self.subTest(place_type=place_type):
                result = place_result("חיפה", place_type=place_type)
                valid, reason = self.matcher._validate_result(result, "חיפה")
                self.assertFalse(valid)
                self.assertIn("Non-settlement OSM feature", reason)

    def test_rejects_neighbourhood_level_administrative_boundary(self):
        result = place_result("חיפה", place_type="administrative")
        result["extratags"] = {"admin_level": "9"}
        valid, reason = self.matcher._validate_result(result, "חיפה")
        self.assertFalse(valid)
        self.assertIn("admin_level=9", reason)

    def test_only_an_exact_verified_pin_can_override_the_suburb_guard(self):
        result = place_result("יישוב חריג", place_type="suburb")
        with patch(
            "settlement_matcher.load_osm_id_overrides",
            return_value={canonical_key("יישוב חריג"): "w123"},
        ):
            valid, reason = self.matcher._validate_result(result, "יישוב חריג")
            self.assertTrue(valid)
            self.assertIn("Pinned OSM id override", reason)
        with patch(
            "settlement_matcher.load_osm_id_overrides",
            return_value={canonical_key("יישוב חריג"): "r123"},
        ):
            valid, reason = self.matcher._validate_result(result, "יישוב חריג")
            self.assertFalse(valid)
            self.assertIn("Non-settlement", reason)

    def test_rejects_feature_inside_another_city_from_address_hierarchy(self):
        result = place_result("הוזייל")
        result["address"] = {
            "country_code": "il", "city": "רהט", "suburb": "הוזייל",
        }
        valid, reason = self.matcher._validate_result(result, "הוזייל")
        self.assertFalse(valid)
        self.assertIn("inside another settlement", reason)
        self.assertIn("city=רהט", reason)

    def test_rejects_address_subdivision_that_names_the_feature_itself(self):
        result = place_result("אבו כביר")
        result["address"] = {
            "country_code": "il", "suburb": "אבו כביר",
        }
        valid, reason = self.matcher._validate_result(result, "אבו כביר")
        self.assertFalse(valid)
        self.assertIn("address suburb=אבו כביר", reason)

    def test_allows_city_address_that_matches_the_requested_settlement(self):
        result = place_result("חיפה")
        result["address"] = {"country_code": "il", "city": "חיפה"}
        valid, _ = self.matcher._validate_result(result, "חיפה")
        self.assertTrue(valid)

    def test_english_city_address_matching_an_alias_is_allowed(self):
        result = place_result("תל אביב־יפו")
        result["address"] = {"country_code": "il", "city": "Tel Aviv-Yafo"}
        valid, _ = self.matcher._validate_result(
            result, "תל אביב - יפו",
            ["תל אביב - יפו", "Tel Aviv-Yafo"],
        )
        self.assertTrue(valid)

    def test_pinned_override_allows_feature_with_parent_city_address(self):
        result = place_result("יישוב חריג", place_type="suburb")
        result["address"] = {"country_code": "il", "city": "עיר אחרת"}
        with patch(
            "settlement_matcher.load_osm_id_overrides",
            return_value={canonical_key("יישוב חריג"): "w123"},
        ):
            valid, reason = self.matcher._validate_result(result, "יישוב חריג")
        self.assertTrue(valid)
        self.assertIn("Pinned OSM id override", reason)

    def test_search_does_not_fall_back_to_individual_words(self):
        response = Mock(status_code=200)
        response.json.return_value = []
        with tempfile.TemporaryDirectory() as temp_dir:
            self.matcher.cache = NominatimCache(os.path.join(temp_dir, "cache.json"))
            with (
                patch("settlement_matcher.requests.get", return_value=response) as get,
                patch.object(self.matcher, "_rate_limit"),
            ):
                match = self.matcher.search_settlement(
                    "אבו ג'ווייעד )שבט(", max_retries=1
                )

        self.assertIsNone(match)
        get.assert_called_once()
        params = get.call_args.kwargs["params"]
        self.assertEqual(params["q"], "אבו ג'ווייעד")
        self.assertEqual(params["countrycodes"], "il,ps")
        self.assertEqual(params["format"], "jsonv2")
        self.assertEqual(params["namedetails"], 1)

    def test_allows_village_inside_regional_council_address(self):
        result = place_result("בר גיורא")
        result["address"] = {
            "country_code": "il",
            "city": "מועצה אזורית מטה יהודה",
        }
        valid, reason = self.matcher._validate_result(result, "בר גיורא")
        self.assertTrue(valid, reason)

    def test_rejects_jatt_for_kibbutz_gat_via_geresh_guard(self):
        result = place_result("ג'ת", display_name="ג'ת, ישראל")
        result["namedetails"] = {"name:he": "ג'ת", "name:en": "Jatt"}
        valid, reason = self.matcher._validate_result(result, "גת )קיבוץ(", ["גת"])
        self.assertFalse(valid)
        self.assertIn("Geresh mismatch", reason)

    def test_pinned_override_allows_institutional_settlement_class(self):
        result = place_result(
            "בית הספר החקלאי מקווה ישראל",
            category="amenity",
            place_type="school",
            display_name="בית הספר החקלאי מקווה ישראל, חולון, ישראל",
        )
        result["osm_id"] = 1004703155
        result["osm_type"] = "way"
        result["address"] = {"country_code": "il", "city": "חולון"}
        valid, reason = self.matcher._validate_result(
            result, "מקווה ישראל", self.matcher._query_variants("מקווה ישראל")
        )
        self.assertTrue(valid, reason)
        self.assertIn("Pinned OSM id override", reason)


if __name__ == "__main__":
    unittest.main()
