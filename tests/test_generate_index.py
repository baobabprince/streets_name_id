import json
import os
import tempfile
import unittest
from pathlib import Path

from generate_index import aggregate, build_dashboard, parse_page, scan_settlements


NEW_PAGE = """<!DOCTYPE html><html lang="he"><head><title>x</title>
<script id="street-records" type="application/json">[]</script>
<script id="page-meta" type="application/json">{"place":"עיר חדשה","match_pct":80.0,
"total_segments":4,"counts":{"perfect":3,"partial":1,"ai":1,"geo-review":0,"unmatched":1,"unnamed":0},
"diagnostics":{"total_osm_streets":5,"total_lamas_streets":8,"total_matched":4,
"unmatched_osm_streets":1,"ai_proposed_matches":1,"geographic_review_matches":0}}</script>
</head><body><h1>עיר חדשה</h1>
<path class="street-path" data-status="perfect"></path>
</body></html>"""

LEGACY_PAGE = """<!DOCTYPE html><html lang="he"><head><title>x</title></head><body>
<h1>מפת רחובות — כפר ותיק</h1>
<div class="diagnostics">
  <div class="diagnostics-grid">
    <div class="diagnostics-card"><div class="label">סה"כ רחובות OSM</div><div class="value">10</div></div>
    <div class="diagnostics-card"><div class="label">סה"כ רחובות למ"ס</div><div class="value">12</div></div>
    <div class="diagnostics-card"><div class="label">התאמות ודאיות</div><div class="value">3</div></div>
    <div class="diagnostics-card"><div class="label">התאמות בעזרת AI</div><div class="value">2</div></div>
    <div class="diagnostics-card"><div class="label">סה"כ התאמות</div><div class="value">3</div></div>
    <div class="diagnostics-card"><div class="label">רחובות OSM ללא התאמה</div><div class="value">7</div></div>
  </div>
</div>
<path class="street-path" data-status="unmatched"></path>
<path class="street-path" data-status="unmatched"></path>
</body></html>"""


class TestGenerateIndex(unittest.TestCase):
    def _write_pages(self, directory: Path):
        (directory / "עיר_חדשה_roads.html").write_text(NEW_PAGE, encoding="utf-8")
        (directory / "כפר_ותיק_roads.html").write_text(LEGACY_PAGE, encoding="utf-8")
        # duplicate legacy export should be deduplicated
        shutil_copy = (directory / "כפר_ותיק__Israel_roads.html")
        shutil_copy.write_text(LEGACY_PAGE, encoding="utf-8")

    def test_scan_extracts_meta_and_legacy_diagnostics(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self._write_pages(directory)
            cities = scan_settlements(str(directory))

        self.assertEqual([c["name"] for c in cities], ["כפר ותיק", "עיר חדשה"])
        new_city = next(c for c in cities if c["name"] == "עיר חדשה")
        self.assertEqual(new_city["osm"], 5)
        self.assertEqual(new_city["matched"], 4)
        self.assertEqual(new_city["coverage"], 80.0)
        self.assertEqual(new_city["tier"], "good")
        self.assertEqual(new_city["counts"]["perfect"], 3)

        legacy = next(c for c in cities if c["name"] == "כפר ותיק")
        self.assertEqual(legacy["osm"], 10)
        self.assertEqual(legacy["matched"], 3)
        self.assertEqual(legacy["unmatched"], 7)
        self.assertEqual(legacy["coverage"], 30.0)
        self.assertEqual(legacy["tier"], "poor")

    def test_aggregate_and_dashboard_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self._write_pages(directory)
            cities = scan_settlements(str(directory))
            totals = aggregate(cities)
            output = directory / "index.html"
            data_out = directory / "dashboard_data.json"
            build_dashboard(cities, output=str(output), data_out=str(data_out))

            html = output.read_text(encoding="utf-8")
            payload = json.loads(data_out.read_text(encoding="utf-8"))

        self.assertEqual(totals["cities"], 2)
        self.assertEqual(totals["osm"], 15)
        self.assertEqual(totals["matched"], 7)
        self.assertEqual(totals["coverage"], 46.7)
        self.assertEqual(totals["buckets"][3], 1)  # legacy city, 30%
        self.assertEqual(totals["buckets"][8], 1)  # new city, 80%
        self.assertIn("כל הרחובות", html)
        self.assertIn('id="dash-data"', html)
        self.assertEqual(len(payload["cities"]), 2)
        self.assertEqual(payload["totals"]["osm"], 15)

    def test_parse_page_handles_missing_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x_roads.html"
            path.write_text("<html><body><h1>מפת רחובות — מבחן</h1></body></html>", encoding="utf-8")
            parsed = parse_page(str(path))
        self.assertEqual(parsed["name"], "מבחן")
        self.assertEqual(parsed["tier"], "none")
        self.assertEqual(parsed["coverage"], 0.0)


if __name__ == "__main__":
    unittest.main()
