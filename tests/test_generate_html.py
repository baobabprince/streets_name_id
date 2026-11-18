import os
import tempfile
import unittest
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse

import geopandas as gpd
from shapely.geometry import LineString, MultiLineString

from generate_html import create_html_from_gdf


class TestGenerateHtml(unittest.TestCase):
    def test_links_unmatched_lamas_streets_to_govmap(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב יחיד"],
                "final_LAMAS_id": [None],
                "best_score": [None],
                "status": ["NO_MATCH"],
                "geometry": [LineString([(34.8, 32.0), (34.801, 32.001)])],
            },
            crs="EPSG:4326",
        )
        diagnostics = {
            "unmatched_lamas_street_names": ['"חדש" & ותיק'],
            "unmatched_lamas_count": 1,
            "unmatched_lamas_percentage": "100%",
        }

        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as output_dir:
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

        parser = GovMapLinkParser()
        parser.feed(html)
        self.assertEqual(len(parser.links), 1)
        link = parser.links[0]
        self.assertEqual(link["text"], '"חדש" & ותיק')
        self.assertEqual(
            parse_qs(urlparse(link["href"]).query)["q"][0],
            '"חדש" & ותיק, יישוב בדיקה',
        )
        self.assertEqual(parse_qs(urlparse(link["href"]).query)["z"], ["16"])
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
        try:
            with tempfile.TemporaryDirectory() as output_dir:
                os.chdir(output_dir)
                create_html_from_gdf(gdf, "יישוב בדיקה")
                with open("HTML/יישוב_בדיקה_roads.html", encoding="utf-8") as html_file:
                    html = html_file.read()
        finally:
            os.chdir(original_dir)

        self.assertEqual(html.count('class="street-path"'), 3)
        self.assertEqual(html.count('vector-effect="non-scaling-stroke"'), 3)
        self.assertEqual(html.count('stroke="#f8fafc"'), 1)

    def test_ai_suggestions_are_displayed_as_unapproved(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב מוצע"],
                "final_LAMAS_id": [None],
                "ai_suggested_LAMAS_id": ["123"],
                "ai_confidence": [0.8],
                "mapping_status": ["AI_PROPOSED"],
                "geographic_validation": ["NO_EXACT_NAME_REFERENCE"],
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
        self.assertIn("הצעת AI (לא מאושרת): 123", html)
        self.assertIn("לא מאושרת", html)


if __name__ == "__main__":
    unittest.main()
