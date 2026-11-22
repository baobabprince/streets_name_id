import json
import os
import tempfile
import unittest
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse

import geopandas as gpd
from shapely.geometry import LineString, MultiLineString

from generate_html import COLOR_PERFECT, create_html_from_gdf


class TestGenerateHtml(unittest.TestCase):
    def test_links_unmatched_lamas_streets_to_govmap(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב יחיד", "רחוב חלקי"],
                "final_LAMAS_id": [None, 55],
                "best_score": [None, 80.0],
                "status": ["NO_MATCH", "CONFIDENT"],
                "geometry": [
                    LineString([(34.8, 32.0), (34.801, 32.001)]),
                    LineString([(34.802, 32.0), (34.803, 32.001)]),
                ],
            },
            crs="EPSG:4326",
        )
        diagnostics = {
            "unmatched_lamas_street_names": ['"חדש" & ותיק'],
            "unmatched_lamas_count": 1,
            "unmatched_lamas_percentage": "100%",
        }

        original_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as output_dir:
            try:
                os.chdir(output_dir)
                create_html_from_gdf(gdf, "יישוב בדיקה", diagnostics)
                with open("HTML/יישוב_בדיקה_roads.html", encoding="utf-8") as html_file:
                    html = html_file.read()
            finally:
                os.chdir(original_dir)

        class GovMapLinkParser(HTMLParser):
            def __init__(self):
                super().__init__()
                self.links = []
                self.current_link = None

            def handle_starttag(self, tag, attrs):
                if tag == "a":
                    self.current_link = dict(attrs)

            def handle_data(self, data):
                if self.current_link is not None:
                    self.current_link["text"] = self.current_link.get("text", "") + data

            def handle_endtag(self, tag):
                if tag == "a" and self.current_link is not None:
                    self.links.append(self.current_link)
                    self.current_link = None

        # GovMap is offered as a lazy reference panel, never loaded on page open.
        self.assertIn('id="govmap-panel"', html)
        self.assertIn('id="govmap-toggle"', html)
        self.assertIn('id="govmap-frame"', html)
        self.assertIn('loading="lazy"', html)
        self.assertNotIn('src="https://www.govmap.gov.il', html)
        self.assertIn('id="gaps-panel"', html)
        self.assertIn('id="gap-osm-list"', html)
        self.assertNotIn("@@", html)
        meta = json.loads(
            html.split('<script id="page-meta" type="application/json">', 1)[1].split("</script>", 1)[0]
        )
        self.assertIn("govmap", meta)
        self.assertIsInstance(meta["govmap"]["x"], int)
        self.assertIsInstance(meta["govmap"]["y"], int)

        # Partially approved and unmatched must be visually distinct: gold vs red.
        self.assertIn('stroke="#f6c453"', html)
        self.assertIn('stroke="#f43f5e"', html)
        self.assertNotIn("#fb923c", html)

        parser = GovMapLinkParser()
        parser.feed(html)
        self.assertEqual(len(parser.links), 1)
        link = parser.links[0]
        self.assertEqual(link["text"], '"חדש" & ותיק')
        self.assertEqual(
            parse_qs(urlparse(link["href"]).query)["q"][0],
            '"חדש" & ותיק, יישוב בדיקה',
        )
        self.assertEqual(parse_qs(urlparse(link["href"]).query)["z"], ["12"])
        self.assertEqual(link["target"], "_blank")
        self.assertIn("noopener", link["rel"].split())

    def test_renders_multiline_parts_and_scale_independent_strokes(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב יחיד", "רחוב מפוצל"],
                "final_LAMAS_id": [123, None],
                "best_score": [100, None],
                "status": ["CONFIDENT", "NO_MATCH"],
                "geometry": [
                    LineString([(34.8, 32.0), (34.801, 32.001)]),
                    MultiLineString(
                        [
                            [(34.802, 32.0), (34.803, 32.001)],
                            [(34.804, 32.0), (34.805, 32.001)],
                        ]
                    ),
                ],
            },
            crs="EPSG:4326",
        )

        original_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as output_dir:
            try:
                os.chdir(output_dir)
                create_html_from_gdf(gdf, "יישוב בדיקה")
                with open("HTML/יישוב_בדיקה_roads.html", encoding="utf-8") as html_file:
                    html = html_file.read()
            finally:
                os.chdir(original_dir)

        self.assertEqual(html.count('class="street-path"'), 3)
        self.assertEqual(html.count('vector-effect="non-scaling-stroke"'), 3)
        self.assertEqual(html.count(f'stroke="{COLOR_PERFECT}"'), 1)

    def test_ai_approved_matches_are_displayed_with_lamas_id(self):
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

        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as output_dir:
                os.chdir(output_dir)
                create_html_from_gdf(gdf, "יישוב בדיקה")
                with open("HTML/יישוב_בדיקה_roads.html", encoding="utf-8") as html_file:
                    html = html_file.read()
        finally:
            os.chdir(original_dir)

        self.assertIn('data-status="ai"', html)
        self.assertIn("מזהה: 123", html)
        self.assertIn("התאמת AI (מאושרת)", html)

    def test_page_embeds_compact_records_instead_of_per_path_metadata(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["שדרות רוטשילד", "שדרות רוטשילד"],
                "final_LAMAS_id": [123, 123],
                "best_LAMAS_name": ["רוטשילד", "רוטשילד"],
                "best_score": [93.4, 93.4],
                "status": ["CONFIDENT", "CONFIDENT"],
                "geometry": [
                    LineString([(34.8, 32.0), (34.801, 32.001)]),
                    LineString([(34.802, 32.0), (34.803, 32.001)]),
                ],
            },
            crs="EPSG:4326",
        )

        original_dir = os.getcwd()
        with tempfile.TemporaryDirectory() as output_dir:
            try:
                os.chdir(output_dir)
                create_html_from_gdf(gdf, "יישוב בדיקה")
                with open("HTML/יישוב_בדיקה_roads.html", encoding="utf-8") as html_file:
                    html = html_file.read()
            finally:
                os.chdir(original_dir)

        self.assertIn('id="street-records"', html)
        self.assertEqual(html.count("data-rec="), 2)
        self.assertNotIn("data-osm=", html)
        import json
        import re

        payload = re.search(
            r'id="street-records" type="application/json">(.*?)</script>', html, re.S
        ).group(1)
        records = json.loads(payload)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][0], "partial")
        self.assertEqual(records[0][1], "שדרות רוטשילד")
        self.assertEqual(records[0][3], "123")
        self.assertAlmostEqual(records[0][4], 93.4)
        # a single decimal keeps geometry compact but sub-metre accurate
        self.assertNotRegex(html, r"34\.\d{5,}")


if __name__ == "__main__":
    unittest.main()
