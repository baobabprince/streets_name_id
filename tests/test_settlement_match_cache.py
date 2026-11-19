import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from settlement_matcher import NominatimCache, SettlementMatcher


class TestNominatimCache(unittest.TestCase):
    def test_deferred_write_persists_positive_and_negative_results_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "nominatim_cache.json")
            cache = NominatimCache(path, auto_save=False)
            cache.set("v3:חיפה", {"osm_id": "1", "display_name": "חיפה"})
            cache.set_unresolved("v3:פלוני", "no results")
            self.assertFalse(os.path.exists(path))

            cache.flush()
            self.assertTrue(os.path.exists(path))
            with open(path, encoding="utf-8") as handle:
                payload = json.load(handle)
            entries = payload["entries"]
            self.assertEqual(entries["v3:חיפה"]["status"], "matched")
            self.assertEqual(entries["v3:פלוני"]["status"], "unresolved")
            self.assertEqual(cache.cached_match("v3:חיפה")["osm_id"], "1")
            self.assertTrue(cache.is_fresh_unresolved("v3:פלוני"))

    def test_legacy_flat_cache_still_loads(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "cache.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"v2:חיפה": {"osm_id": "7"}}, handle)
            cache = NominatimCache(path)
            self.assertEqual(cache.cached_match("v2:חיפה")["osm_id"], "7")

    def test_unresolved_result_avoids_a_second_network_request(self):
        response = Mock(status_code=200)
        response.json.return_value = []
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = NominatimCache(os.path.join(temp_dir, "cache.json"))
            matcher = SettlementMatcher(cache=cache)
            with (
                patch("settlement_matcher.requests.get", return_value=response) as get,
                patch.object(matcher, "_rate_limit"),
            ):
                first = matcher.search_settlement("יישוב בדיקה", max_retries=1)
                second = matcher.search_settlement("יישוב בדיקה", max_retries=1)
        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()
