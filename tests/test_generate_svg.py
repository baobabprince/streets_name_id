import os
import tempfile
import unittest

import geopandas as gpd
from shapely.geometry import LineString

from generate_svg import create_svg_from_gdf


class TestGenerateSvg(unittest.TestCase):
    def test_ai_approved_match_is_rendered_with_lamas_id(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב ליה"],
                "final_LAMAS_id": ["123"],
                "ai_suggested_LAMAS_id": ["123"],
                "mapping_status": ["APPROVED_AI"],
                "geometry": [LineString([(34.8, 32.0), (34.801, 32.001)])],
            },
            crs="EPSG:4326",
        )

        original_dir = os.getcwd()
        try:
            with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as output_dir:
                os.chdir(output_dir)
                os.makedirs("SVG")
                create_svg_from_gdf(gdf, "test-city")
                with open("SVG/test-city_roads.svg", encoding="utf-8") as svg_file:
                    svg = svg_file.read()
        finally:
            os.chdir(original_dir)

        self.assertIn('stroke="#00BFFF"', svg)
        self.assertIn("LAMAS ID: 123 (AI approved)", svg)


if __name__ == "__main__":
    unittest.main()
