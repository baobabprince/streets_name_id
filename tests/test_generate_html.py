import os
import tempfile
import unittest

import geopandas as gpd
from shapely.geometry import LineString, MultiLineString

from generate_html import create_html_from_gdf


class TestGenerateHtml(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
