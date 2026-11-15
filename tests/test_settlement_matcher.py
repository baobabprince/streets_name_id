import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from settlement_matcher import NominatimCache, SettlementMatcher


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


if __name__ == "__main__":
    unittest.main()
