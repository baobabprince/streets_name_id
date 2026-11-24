"""Upgrading existing pages to the comparison room without re-matching."""
import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path

from pyproj import Transformer

from settlement_page import decode_line

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("convert_pages", ROOT / "scripts/convert_pages_to_room.py")
convert_pages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(convert_pages)


def block(html, name):
    return json.loads(re.search(rf'<script id="{name}" type="application/json">(.*?)</script>', html, re.S).group(1))


def svg_page():
    """A page in the earlier SVG layout, with real coordinates around (32.0, 34.8)."""
    to_merc = Transformer.from_crs(4326, 3857, always_xy=True)
    to_itm = Transformer.from_crs(3857, 2039, always_xy=True)
    lines = [[(34.80, 32.00), (34.81, 32.01)], [(34.82, 32.00), (34.83, 32.02)]]
    merc = [[to_merc.transform(lon, lat) for lon, lat in line] for line in lines]
    xs = [x for line in merc for x, _ in line]
    ys = [y for line in merc for _, y in line]
    minx, maxy = min(xs), max(ys)
    width, height = max(xs) - minx, maxy - min(ys)
    cx, cy = to_itm.transform(minx + width / 2, maxy - height / 2)
    paths = "".join(
        f'<path d="M {" L ".join(f"{x - minx:.1f},{maxy - y:.1f}" for x, y in line)}" class="street-path" data-rec="{i}"></path>'
        for i, line in enumerate(merc)
    )
    meta = {
        "place": "עיר בדיקה",
        "counts": {"perfect": 1, "unmatched": 1},
        "diagnostics": {"total_osm_streets": 2, "total_lamas_streets": 3, "total_matched": 1, "unmatched_lamas_count": 2},
        "govmap": {"x": round(cx), "y": round(cy)},
        "provenance": {"generated_at": "2026-11-20T10:00:00+00:00"},
    }
    records = [["perfect", "הרצל", "הרצל", "7", 100, "", "", "", ""], ["unmatched", "סמטה", "", "", "", "", "", "", ""]]
    return f"""<html><body><h1>עיר בדיקה</h1>
<svg id="street-map" viewBox="0 0 {width:.1f} {height:.1f}">{paths}</svg>
<div class="unmatched-list"><h3>רחובות OSM ללא התאמה מאושרת (1)</h3><ul><li><a href="#">סמטה</a></li></ul></div>
<div class="unmatched-list"><h3>רחובות למ״ס ללא התאמה (2 · 66%)</h3><ul><li><a href="#">א&quot;ת הדר</a></li><li><a href="#">הגפן</a></li></ul></div>
<script id="page-meta" type="application/json">{json.dumps(meta, ensure_ascii=False)}</script>
<script id="street-records" type="application/json">{json.dumps(records, ensure_ascii=False)}</script>
</body></html>"""


class ConvertPagesTests(unittest.TestCase):
    def test_svg_page_becomes_room_with_recovered_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "עיר_בדיקה_roads.html"
            path.write_text(svg_page(), encoding="utf-8")
            html = convert_pages.convert(path)
        self.assertIn('id="osm-map"', html)
        self.assertNotIn('id="street-map"', html)
        room = block(html, "room-data")
        self.assertEqual(room["missingOfficial"], ['א"ת הדר', "הגפן"])
        self.assertEqual(room["approvedOfficialCount"], 1)
        start = decode_line(room["geometry"][0][0])[0]
        end = decode_line(room["geometry"][1][0])[-1]
        # Recovered from SVG offsets + rounded ITM centre: about a metre.
        self.assertAlmostEqual(start[0], 32.00, delta=2e-5)
        self.assertAlmostEqual(start[1], 34.80, delta=2e-5)
        self.assertAlmostEqual(end[0], 32.02, delta=2e-5)
        self.assertAlmostEqual(end[1], 34.83, delta=2e-5)
        # The recorded creation time is kept; results did not change.
        self.assertEqual(block(html, "page-meta")["provenance"]["generated_at"], "2026-11-20T10:00:00+00:00")
        self.assertIn('datetime="2026-11-20T10:00:00+00:00"', html)

    def test_conversion_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "עיר_בדיקה_roads.html"
            path.write_text(svg_page(), encoding="utf-8")
            first = convert_pages.convert(path)
            path.write_text(first, encoding="utf-8")
            self.assertEqual(convert_pages.convert(path), first)

    def test_single_street_pages_are_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x_roads.html"
            path.write_text(
                '<script id="page-meta" type="application/json">{"place": "x", "single_street": true}</script>'
                '<script id="street-records" type="application/json">[]</script>',
                encoding="utf-8",
            )
            self.assertIsNone(convert_pages.convert(path))


class PublishedPagesTests(unittest.TestCase):
    """Checked-in pages must match what the current template renders."""

    PETAH_TIKVA = ROOT / "HTML/פתח_תקווה_roads.html"

    def test_every_results_page_uses_the_room_and_is_current(self):
        pages = [p for p in sorted((ROOT / "HTML").glob("*.html")) if 'id="street-records"' in p.read_text(encoding="utf-8")]
        self.assertGreater(len(pages), 900)
        stale = []
        for path in pages:
            raw = path.read_text(encoding="utf-8")
            if '"single_street": true' in raw:
                continue
            if 'id="street-map"' in raw or convert_pages.convert(path) != raw:
                stale.append(path.name)
        self.assertEqual(stale, [], "Run python scripts/convert_pages_to_room.py")

    def test_petah_tikva_figures_reconcile(self):
        html = self.PETAH_TIKVA.read_text(encoding="utf-8")
        records, room, meta = block(html, "street-records"), block(html, "room-data"), block(html, "page-meta")
        approved = [r for r in records if r[3] and r[0] not in {"geo-review", "unmatched", "unnamed"}]
        self.assertEqual(len(records), 989)
        self.assertEqual(len(approved), 885)
        self.assertEqual(room["approvedOfficialCount"], 863)
        self.assertEqual(len(room["missingOfficial"]), 307)
        self.assertEqual(863 + 307, meta["diagnostics"]["total_lamas_streets"])
        self.assertEqual(sum(map(len, room["geometry"])), 2284)
        self.assertEqual(len([r for r in records if r[0] == "ai"]), 9)
        for lines in room["geometry"]:
            self.assertTrue(lines)
            for line in lines:
                for lat, lon in decode_line(line):
                    self.assertTrue(31.9 < lat < 32.3 and 34.7 < lon < 35.1)
