"""Offline data-contract tests for the isolated comparison-room prototype."""
import importlib.util
import hashlib
import re
import json
import math
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("comparison_builder", ROOT / "scripts/build_comparison_prototype.py")
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class ComparisonPrototypeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = builder.build()

    def test_checked_in_fixture_is_reproducible(self):
        checked_in = json.loads((ROOT / "prototype/data.json").read_text(encoding="utf-8"))
        # The build stamp is the only intentionally time-dependent field.
        self.assertTrue(checked_in.pop("builtAt"))
        rebuilt = dict(self.data)
        rebuilt.pop("builtAt")
        self.assertEqual(rebuilt, checked_in)

    def test_both_coverage_denominators_reconcile(self):
        records = self.data["records"]
        approved = [r for r in records if r[3] and r[0] not in {"geo-review", "unmatched", "unnamed"}]
        self.assertEqual(len(records), 989)
        self.assertEqual(len(approved), 885)
        self.assertEqual(len({r[3] for r in approved}), 863)
        self.assertEqual(len(self.data["missingOfficial"]), 307)
        self.assertEqual(863 + 307, 1170)
        self.assertEqual(round(885 / 989 * 100, 1), 89.5)
        self.assertEqual(round(863 / 1170 * 100, 1), 73.8)

    def test_ai_is_approved_in_this_source(self):
        ai = [r for r in self.data["records"] if r[0] == "ai"]
        self.assertEqual(len(ai), 9)
        self.assertTrue(all(r[3] for r in ai))

    def test_geometry_survives_extraction(self):
        self.assertEqual(len(self.data["geometry"]), len(self.data["records"]))
        self.assertEqual(sum(map(len, self.data["geometry"])), 2284)
        for street in self.data["geometry"]:
            self.assertTrue(street)
            for line in street:
                self.assertGreaterEqual(len(line), 2)
                for lat, lon in line:
                    self.assertTrue(math.isfinite(lat) and math.isfinite(lon))
                    self.assertTrue(31.9 < lat < 32.3)
                    self.assertTrue(34.7 < lon < 35.1)

    def test_missing_names_are_decoded_not_html(self):
        self.assertIn('א"ת פארק ינאי', self.data["missingOfficial"])
        self.assertFalse(any("&quot;" in name for name in self.data["missingOfficial"]))
        self.assertEqual(len(set(self.data["missingOfficial"])), 307)

    def test_all_local_scripts_and_styles_use_current_content_hashes(self):
        page = (ROOT / "prototype/index.html").read_text(encoding="utf-8")
        assets = re.findall(r'(?:src|href)="([^"?]+)\?v=([a-f0-9]{16})"', page)
        self.assertEqual(len(assets), 5)
        for path, digest in assets:
            expected = hashlib.sha256((ROOT / "prototype" / path).read_bytes()).hexdigest()[:16]
            self.assertEqual(digest, expected, f"Run python scripts/stamp_comparison_assets.py after editing {path}")

    def test_no_invented_source_dates(self):
        # The current Petah Tikva page predates provenance recording.
        self.assertIsNone(self.data["sourceDates"])

    def test_build_stamp_honours_source_date_epoch(self):
        from unittest import mock

        with mock.patch.dict("os.environ", {"SOURCE_DATE_EPOCH": "1700000000"}):
            built = builder.build()["builtAt"]
        self.assertEqual(built["iso"], "2023-11-14T22:13:20+00:00")
        self.assertEqual(built["display"], "15.11.2023, 00:13")


if __name__ == "__main__":
    unittest.main()
