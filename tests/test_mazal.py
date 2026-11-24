"""Offline contract tests. JSON-LD below is representative, not a live capture."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

from mazal import (DC, DCT, MazalClient, build_registry, choose_entity,
                   enrich_dataframe, enrich_from_registry, name_key, parse_records)

MID = "987007123456705171"  # Synthetic authority IDs, not asserted real identities.
OTHER = "987007123456805171"


def authority(mid=MID, title="מונטיפיורי, משה, 1784-1885", aliases=()):
    return {"@id": "https://www.nli.org.il/he/authorities/" + mid,
            DC + "title": [{"@value": title}],
            DCT + "alternative": [{"@value": a} for a in aliases]}


def response(payload=None, status=200, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = [authority()] if payload is None else payload
    return result


class MazalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cache = Path(self.temp.name) / "cache.json"
        self.session = Mock()
        self.session.get.return_value = response()
        self.client = self.make_client()

    def make_client(self, **kwargs):
        return MazalClient("private-test-secret", self.cache, delay=0,
                           session=self.session, **kwargs)

    def test_order_prefix_and_niqqud_share_key(self):
        self.assertEqual(name_key("משה מונטיפיורי"), name_key("שד׳ מונטיפיורי מֹשה"))

    def test_unique_exact_heading_and_dates(self):
        result = self.client.resolve("משה מונטיפיורי")
        self.assertEqual(result["status"], "resolved")
        self.assertEqual(result["entity_key"], "mazal:" + MID)
        call = self.session.get.call_args.kwargs
        self.assertEqual(call["params"]["api_key"], "private-test-secret")
        self.assertEqual(call["params"]["material_type"], "Identity")
        self.assertEqual(call["params"]["items_per_page"], 50)
        self.assertFalse(call["allow_redirects"])
        self.assertNotIn("private-test-secret", self.cache.read_text())

    def test_exact_alias_is_allowed_but_fuzzy_is_not(self):
        records = parse_records([authority(aliases=["משה מונטיפיורה"])])
        self.assertEqual(choose_entity(name_key("משה מונטיפיורה"), records)["status"], "resolved")
        self.assertEqual(choose_entity(name_key("משה מונטיפורי"), records)["status"], "not_found")

    def test_ambiguous_never_selects_first_hit(self):
        self.session.get.return_value = response([authority(), authority(OTHER)])
        result = self.client.resolve("משה מונטיפיורי")
        self.assertEqual(result["status"], "ambiguous")
        self.assertEqual(result["candidate_ids"], [MID, OTHER])
        self.assertNotIn("entity_key", result)

    def test_duplicate_records_for_same_id_are_not_ambiguity(self):
        result = choose_entity(name_key("משה מונטיפיורי"), parse_records([authority(), authority()]))
        self.assertEqual(result["status"], "resolved")

    def test_single_word_numeric_and_blank_names_need_review_without_calls(self):
        for name in ["מונטיפיורי", "הרצל", "", "רחוב 12", "משה 123 כהן"]:
            self.assertEqual(self.client.resolve(name)["status"], "needs_review")
        self.session.get.assert_not_called()

    def test_expiring_cache_and_reuse_across_clients(self):
        self.client.resolve("משה מונטיפיורי")
        self.make_client().resolve("מונטיפיורי משה")
        self.assertEqual(self.session.get.call_count, 1)
        cache = json.loads(self.cache.read_text())
        cache["entries"][name_key("משה מונטיפיורי")]["cached_at"] = 0
        self.cache.write_text(json.dumps(cache))
        self.make_client().resolve("משה מונטיפיורי")
        self.assertEqual(self.session.get.call_count, 2)

    def test_negative_cache_reused(self):
        self.session.get.return_value = response([])
        self.assertEqual(self.client.resolve("שם חסר")["status"], "not_found")
        self.assertEqual(self.make_client().resolve("שם חסר")["status"], "not_found")
        self.assertEqual(self.session.get.call_count, 1)

    def test_http_and_network_errors_stop_requests_and_are_not_cached(self):
        for failure in [response(status=429), response(status=401), response(status=302),
                        requests.ConnectionError("URL contains private-test-secret")]:
            self.session.get.side_effect = None
            self.session.get.return_value = failure
            if isinstance(failure, Exception):
                self.session.get.side_effect = failure
            client = self.make_client()
            result = client.resolve("משה מונטיפיורי")
            self.assertIn(result["status"], {"http_error", "network_error"})
            self.assertNotIn("private-test-secret", json.dumps(result))
            count = self.session.get.call_count
            client.resolve("חיים נחמן ביאליק")
            self.assertEqual(self.session.get.call_count, count)
            self.assertFalse(self.cache.exists())

    def test_ignored_filter_is_not_silently_accepted(self):
        self.session.get.return_value = response(headers={"Errors": "invalid filter"})
        self.assertEqual(self.client.resolve("משה מונטיפיורי")["status"], "api_error")
        self.assertFalse(self.cache.exists())

    def test_unrecognized_schema_and_bibliographic_records_fail_closed(self):
        for payload in [{"items": []}, [{"@id": "https://www.nli.org.il/he/books/" + MID}],
                        [authority() | {"@id": "https://example.com/he/authorities/" + MID}],
                        [{"title": "משה מונטיפיורי", "id": MID}], [None]]:
            self.session.get.return_value = response(payload)
            client = self.make_client()
            self.assertEqual(client.resolve("משה מונטיפיורי")["status"], "schema_error")
            self.assertIsNotNone(client.response_shape)
            self.assertFalse(self.cache.exists())

    def test_pagination_detects_ambiguity_on_later_page(self):
        self.session.get.side_effect = [response([authority()] * 50), response([authority(OTHER)])]
        result = self.client.resolve("משה מונטיפיורי")
        self.assertEqual(result["status"], "ambiguous")
        self.assertEqual(self.session.get.call_args.kwargs["params"]["result_page"], 2)

    def test_full_page_without_completion_is_not_unique(self):
        self.session.get.return_value = response([authority()] * 50)
        client = self.make_client(max_pages=1)
        self.assertEqual(client.resolve("משה מונטיפיורי")["status"], "truncated")
        self.assertFalse(self.cache.exists())

    def test_budget_exhaustion_mid_pagination_never_resolves(self):
        self.session.get.return_value = response([authority()] * 50)
        client = self.make_client(max_requests=1)
        self.assertEqual(client.resolve("משה מונטיפיורי")["status"], "budget_exhausted")
        self.assertEqual(self.session.get.call_count, 1)

    def test_registry_groups_variants_and_does_not_duplicate_calls(self):
        registry = build_registry(["משה מונטיפיורי", "מונטיפיורי משה", "משה מונטיפיורי"], self.client)
        self.assertEqual(len(registry["entities"]), 1)
        self.assertEqual(len(registry["entities"]["mazal:" + MID]["names"]), 2)
        self.assertEqual(self.session.get.call_count, 1)
        self.assertNotIn("private-test-secret", json.dumps(registry))

    def test_enrichment_preserves_physical_streets_and_candidate_is_not_evidence(self):
        registry = build_registry(["משה מונטיפיורי"], self.client)
        original = pd.DataFrame({"osm_name": ["משה מונטיפיורי", "מונטיפיורי משה", "רחוב אחר"],
                                 "final_LAMAS_id": ["101", "202", "303"],
                                 "final_LAMAS_key": ["1:101", "2:202", "3:303"],
                                 "best_LAMAS_name": ["משה מונטיפיורי"] * 3})
        result = enrich_dataframe(original, registry)
        pd.testing.assert_frame_equal(result[original.columns], original)
        self.assertEqual(result["entity_key"].tolist(), ["mazal:" + MID, "mazal:" + MID, ""])
        self.assertEqual(result["mazal_status"].tolist(), ["resolved", "resolved", "unresolved"])
        self.assertNotIn("mazal_id", original.columns)

    def test_conflicting_final_name_withholds_entity(self):
        self.session.get.return_value = response([authority(OTHER, "חיים נחמן ביאליק")])
        registry = build_registry(["משה מונטיפיורי", "חיים נחמן ביאליק"], self.client)
        # Resolve the Montefiore side separately (same synthetic fixture ID).
        registry["entries"][name_key("משה מונטיפיורי")] = {
            "status": "resolved", "mazal_id": MID, "preferred_name": "משה מונטיפיורי"}
        frame = pd.DataFrame([{"osm_name": "משה מונטיפיורי", "final_LAMAS_id": "101",
                               "final_LAMAS_name": "חיים נחמן ביאליק"}])
        result = enrich_dataframe(frame, registry)
        self.assertEqual(result.iloc[0].mazal_status, "conflict")
        self.assertEqual(result.iloc[0].entity_key, "")

    def test_missing_or_corrupt_offline_registry_is_optional(self):
        frame = pd.DataFrame({"osm_name": ["רחוב אחר"]})
        with patch.dict(os.environ, {"MAZAL_ENTITIES_PATH": str(self.cache)}):
            self.assertIs(enrich_from_registry(frame), frame)
            self.cache.write_text("broken")
            self.assertIs(enrich_from_registry(frame), frame)

    def test_cli_reports_schema_error_and_writes_report_without_key(self):
        from scripts.build_mazal_entities import main
        self.session.get.return_value = response({"unexpected": []})
        output = Path(self.temp.name) / "registry.json"
        with patch.dict(os.environ, {"MAZAL_API_KEY": "private-test-secret"}), \
             patch("scripts.build_mazal_entities.MazalClient", return_value=self.client):
            self.assertEqual(main(["--name", "משה מונטיפיורי", "--output", str(output)]), 1)
        self.assertNotIn("private-test-secret", output.read_text())
        self.assertIn("schema_error", json.loads(output.read_text())["statistics"])


if __name__ == "__main__":
    unittest.main()
