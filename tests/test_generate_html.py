"""The pipeline's page generator emits the comparison-room settlement page."""
import json
import os
import re
import tempfile
import unittest

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

from generate_html import create_html_from_gdf
from settlement_page import ROOT, decode_line


def render(gdf, diagnostics=None, place="יישוב בדיקה", provenance=None):
    original_dir = os.getcwd()
    with tempfile.TemporaryDirectory() as output_dir:
        try:
            os.chdir(output_dir)
            create_html_from_gdf(gdf, place, diagnostics, provenance=provenance)
            name = os.listdir("HTML")[0]
            with open(os.path.join("HTML", name), encoding="utf-8") as handle:
                return handle.read()
        finally:
            os.chdir(original_dir)


def block(html, name):
    match = re.search(rf'<script id="{name}" type="application/json">(.*?)</script>', html, re.S)
    return json.loads(match.group(1))


def two_streets():
    return gpd.GeoDataFrame(
        {
            "osm_name": ["רחוב יחיד", "רחוב חלקי"],
            "final_LAMAS_id": [None, 55],
            "best_LAMAS_name": [None, "חלקי"],
            "best_score": [None, 80.0],
            "status": ["NO_MATCH", "CONFIDENT"],
            "geometry": [
                LineString([(34.8, 32.0), (34.801, 32.001)]),
                LineString([(34.802, 32.0), (34.803, 32.001)]),
            ],
        },
        crs="EPSG:4326",
    )


class TestGenerateHtml(unittest.TestCase):
    def test_pipeline_output_is_the_comparison_room(self):
        html = render(two_streets(), {"total_osm_streets": 2, "total_lamas_streets": 2, "total_matched": 1})
        self.assertNotIn("@@", html)
        # Comparison room: OSM map, GovMap reference, full table, technical details.
        for marker in ('id="osm-map"', 'id="gov-map"', 'id="rows"', 'id="technical"', "חדר השוואה"):
            self.assertIn(marker, html)
        # Not the earlier SVG layout.
        self.assertNotIn('id="street-map"', html)
        self.assertNotIn("map.js", html)
        self.assertNotIn("prototype", html)
        self.assertIn("<title>יישוב בדיקה ·", html)
        self.assertIn('<h1 id="page-title">יישוב בדיקה</h1>', html)
        # GovMap is never loaded from the HTML itself, only by the script.
        self.assertNotIn('src="https://www.govmap.gov.il', html)

    def test_shared_assets_are_local_and_content_hashed(self):
        html = render(two_streets())
        assets = re.findall(r'(?:src|href)="\.\./assets/([^"?]+)\?v=([a-f0-9]{16})"', html)
        self.assertEqual(
            sorted(path for path, _ in assets),
            sorted(["room.js", "room.css", "score-tools.js", "vendor/leaflet/leaflet.js", "vendor/leaflet/leaflet.css"]),
        )
        import hashlib

        for path, digest in assets:
            self.assertEqual(digest, hashlib.sha256((ROOT / "assets" / path).read_bytes()).hexdigest()[:16])
        self.assertNotIn("unpkg.com", html)

    def test_unmatched_lamas_names_and_totals_are_embedded(self):
        diagnostics = {
            "total_osm_streets": 2,
            "total_lamas_streets": 2,
            "unmatched_lamas_street_names": ['"חדש" & ותיק'],
            "unmatched_lamas_count": 1,
        }
        html = render(two_streets(), diagnostics)
        room = block(html, "room-data")
        self.assertEqual(room["missingOfficial"], ['"חדש" & ותיק'])
        self.assertEqual(room["missingOfficialTotal"], 1)
        self.assertEqual(room["approvedOfficialCount"], 1)
        meta = block(html, "page-meta")
        self.assertIsInstance(meta["govmap"]["x"], int)
        self.assertIsInstance(meta["govmap"]["y"], int)
        self.assertEqual(meta["diagnostics"]["total_lamas_streets"], 2)

    def test_geometry_is_wgs84_per_record_including_multilines(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב יחיד", "רחוב מפוצל"],
                "final_LAMAS_id": [123, None],
                "best_score": [100, None],
                "geometry": [
                    LineString([(34.8, 32.0), (34.801, 32.001)]),
                    MultiLineString([[(34.802, 32.0), (34.803, 32.001)], [(34.804, 32.0), (34.805, 32.001)]]),
                ],
            },
            crs="EPSG:4326",
        ).to_crs(epsg=3857)
        html = render(gdf)
        room = block(html, "room-data")
        records = block(html, "street-records")
        self.assertEqual(room["geometryEncoding"], "delta-e5")
        self.assertEqual([len(lines) for lines in room["geometry"]], [1, 2])
        self.assertEqual([r[0] for r in records], ["perfect", "unmatched"])
        first = decode_line(room["geometry"][0][0])
        self.assertAlmostEqual(first[0][0], 32.0, places=4)
        self.assertAlmostEqual(first[0][1], 34.8, places=4)
        self.assertAlmostEqual(first[1][1], 34.801, places=4)

    def test_ai_approved_matches_keep_final_id(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב ליה"],
                "best_LAMAS_name": ["רחוב הרצל"],
                "final_LAMAS_id": ["123"],
                "ai_suggested_LAMAS_id": ["123"],
                "ai_confidence": [0.88],
                "mapping_status": ["APPROVED_AI"],
                "geometry": [LineString([(34.8, 32.0), (34.801, 32.001)])],
            },
            crs="EPSG:4326",
        )
        records = block(render(gdf), "street-records")
        self.assertEqual(records, [["ai", "רחוב ליה", "רחוב הרצל", "123", "", "123", 0.88, "", ""]])

    def test_segments_of_one_street_share_a_record(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["שדרות רוטשילד", "שדרות רוטשילד"],
                "final_LAMAS_id": [123, 123],
                "best_LAMAS_name": ["רוטשילד", "רוטשילד"],
                "best_score": [93.4, 93.4],
                "geometry": [
                    LineString([(34.8, 32.0), (34.801, 32.001)]),
                    LineString([(34.802, 32.0), (34.803, 32.001)]),
                ],
            },
            crs="EPSG:4326",
        )
        html = render(gdf)
        records = block(html, "street-records")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][:5], ["partial", "שדרות רוטשילד", "רוטשילד", "123", 93.4])
        self.assertEqual(len(block(html, "room-data")["geometry"][0]), 2)
        self.assertEqual(block(html, "page-meta")["counts"]["partial"], 1)

    def test_names_cannot_break_out_of_json_blocks(self):
        gdf = two_streets()
        gdf.loc[0, "osm_name"] = "</script><b>x"
        html = render(gdf)
        self.assertNotIn("</script><b>", html)
        self.assertEqual(block(html, "street-records")[0][1], "</script><b>x")

    def test_mazal_entity_is_separate_from_official_street_id(self):
        from mazal import enrich_dataframe, name_key
        mid = "987007123456705171"  # Synthetic fixture ID.
        registry = {"entries": {name_key("משה מונטיפיורי"): {
            "status": "resolved", "mazal_id": mid, "preferred_name": "מונטיפיורי, משה"}}}
        frame = pd.DataFrame({"osm_name": ["משה מונטיפיורי", "מונטיפיורי משה"],
                              "final_LAMAS_id": ["101", "202"], "best_score": [100, 100]})
        html = render(enrich_dataframe(frame, registry))
        records = block(html, "street-records")
        self.assertEqual(html.count(f'href="https://www.nli.org.il/he/authorities/{mid}"'), 1)
        self.assertIn("ישויות משותפות במז״ל", html)
        self.assertNotIn("@@MAZAL_ENTITIES@@", html)
        self.assertEqual([r[3] for r in records], ["101", "202"])
        self.assertEqual([r[9] for r in records], [mid, mid])
        self.assertEqual([r[10] for r in records], ["מונטיפיורי, משה"] * 2)

    def test_optional_mazal_name_cannot_break_json_blocks(self):
        frame = two_streets()
        frame["mazal_id"] = "987007123456705171"
        frame["mazal_name"] = "</script><b>entity"
        html = render(frame)
        self.assertNotIn("</script><b>entity", html)
        self.assertEqual(block(html, "street-records")[0][10], "</script><b>entity")

    def test_report_without_geometry_still_renders_table(self):
        df = pd.DataFrame({"osm_name": ["רחוב"], "final_LAMAS_id": [1], "best_score": [100]})
        html = render(df, {"total_osm_streets": 1, "total_lamas_streets": 1})
        room = block(html, "room-data")
        self.assertEqual(room["geometry"], [[]])
        self.assertNotIn("govmap", block(html, "page-meta"))

    def test_mirrored_lamas_parentheses_are_displayed_correctly(self):
        html = render(two_streets(), place="גבעת חיים )איחוד(")
        self.assertIn('<h1 id="page-title">גבעת חיים (איחוד)</h1>', html)


if __name__ == "__main__":
    unittest.main()
