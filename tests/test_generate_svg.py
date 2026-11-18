import os
import tempfile
import unittest

import geopandas as gpd
from shapely.geometry import LineString

from generate_svg import create_svg_from_gdf


class TestGenerateSvg(unittest.TestCase):
    def test_ai_suggestion_is_not_rendered_as_an_approved_match(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["רחוב מוצע"],
                "final_LAMAS_id": [None],
                "ai_suggested_LAMAS_id": ["123"],
                "mapping_status": ["AI_PROPOSED"],
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
        self.assertIn("AI suggestion, not approved: 123", svg)
        self.assertNotIn("LAMAS ID: 123", svg)


if __name__ == "__main__":
    unittest.main()
