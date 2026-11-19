import unittest

from migrate_legacy_pages import convert_page, parse_tooltip, round_path

LEGACY = """<!DOCTYPE html><html lang="he"><head><title>x</title></head><body>
<h1>מפת רחובות — עיר בדיקה</h1>
<svg viewBox="0 0 100.123456 200.654321">
<path d="M 12.123456,34.987654 L 56.111111,78.222222" stroke="#f8fafc" stroke-width="5"
 stroke-dasharray="none" vector-effect="non-scaling-stroke" stroke-linecap="round"
 stroke-linejoin="round" fill="none" class="street-path"
 data-tooltip="רחוב האלון -> רחוב האלון הרשמי&#10;ציון: 100.0&#10;מזהה: 123"
 data-status="perfect" data-name="רחוב האלון"></path>
<path d="M 20.1,30.2 L 40.3,50.4" stroke="#00BFFF" stroke-width="5"
 stroke-dasharray="none" vector-effect="non-scaling-stroke" stroke-linecap="round"
 stroke-linejoin="round" fill="none" class="street-path"
 data-tooltip="רחוב חדש&#10;הצעת AI (לא מאושרת): 456" data-status="ai" data-name="רחוב חדש"></path>
</svg>
<div class="diagnostics">
  <div class="diagnostics-grid">
    <div class="diagnostics-card"><div class="label">סה"כ רחובות OSM</div><div class="value">2</div></div>
    <div class="diagnostics-card"><div class="label">סה"כ רחובות למ"ס</div><div class="value">3</div></div>
    <div class="diagnostics-card"><div class="label">סה"כ התאמות</div><div class="value">1</div></div>
    <div class="diagnostics-card"><div class="label">רחובות OSM ללא התאמה</div><div class="value">1</div></div>
  </div>
</div>
<div id="tooltip"></div>
</body></html>"""


class TestMigrateLegacyPages(unittest.TestCase):
    def test_converts_legacy_page_to_compact_records(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "עיר_בדיקה_roads.html"
            path.write_text(LEGACY, encoding="utf-8")
            html, info = convert_page(path)

        self.assertIn('id="street-records"', html)
        self.assertIn("assets/map.js", html)
        self.assertIn('id="gaps-panel"', html)
        self.assertIn('id="govmap-panel"', html)
        self.assertIn('id="context-layer"', html)
        self.assertNotIn("@@", html)
        self.assertNotIn("const filterBtns", html)
        # coordinates are rounded to 0.1 m
        self.assertIn('M 12.1,35.0', html)
        self.assertNotIn("12.123456", html)
        # metadata is shared through records; AI keeps its review notice
        self.assertIn("data-rec=", html)
        self.assertIn("הצעת AI (לא מאושרת): 456", html)
        self.assertEqual(info["segments"], 2)
        self.assertEqual(info["counts"]["perfect"], 1)
        self.assertEqual(info["counts"]["ai"], 1)
        self.assertEqual(info["coverage"], 50.0)

    def test_parse_tooltip_supports_legacy_separator(self):
        info = parse_tooltip("שדרות בן גוריון -> בן גוריון#10;ציון: 91.4#10;מזהה: 270")
        self.assertEqual(info["name"], "שדרות בן גוריון")
        self.assertEqual(info["lamas"], "בן גוריון")
        self.assertEqual(info["score"], 91.4)
        self.assertEqual(info["id"], "270")

    def test_round_path(self):
        self.assertEqual(round_path("M 1.2345,6.789 L 10.0,20.05"), "M 1.2,6.8 L 10.0,20.1")


if __name__ == "__main__":
    unittest.main()
