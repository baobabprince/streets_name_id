# tests/test_pipeline_integration.py
import unittest
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads
from unittest.mock import patch
from pipeline import run_pipeline

class TestPipelineIntegration(unittest.TestCase):

    @patch('pipeline.load_or_fetch_osm')
    @patch('pipeline.load_or_fetch_LAMAS')
    def test_even_sapir_100_percent_match(self, mock_load_lamas, mock_load_osm):
        """
        Tests the full pipeline with a known dataset for "Even Sapir"
        and expects a 100% match rate.
        """
        # Load the test data
        osm_df = pd.read_csv('tests/test_data/even_sapir/osm_even_sapir.csv')
        osm_df['geometry'] = osm_df['geometry'].apply(loads)
        osm_df['all_osm_names'] = osm_df['osm_name'].apply(lambda x: [x])
        osm_df['city'] = 'אבן ספיר'
        osm_gdf = gpd.GeoDataFrame(osm_df, crs="EPSG:4326")

        lamas_df = pd.read_csv('tests/test_data/even_sapir/lamas_even_sapir.csv')
        lamas_df['city'] = 'אבן ספיר'

        # Set the mock return values
        mock_load_osm.return_value = osm_gdf
        mock_load_lamas.return_value = lamas_df

        # Run the pipeline
        with patch('generate_html.create_html_from_gdf') as mock_create_html:
            run_pipeline(place="אבן ספיר", force_refresh=True, use_ai=False)

            # Get the gdf that was passed to the HTML generation
            self.assertTrue(mock_create_html.called)
            result_gdf = mock_create_html.call_args[0][0]

            # --- Assertions ---
            # 1. Check that all streets were matched
            self.assertEqual(len(result_gdf), 3)
            self.assertTrue(result_gdf['final_LAMAS_id'].notna().all())

            # 2. Check that the matches are correct
            expected_matches = {
                'דרך ספיר': '101',
                'האלה': '102',
                'התאנה': '103'
            }
            for _, row in result_gdf.iterrows():
                self.assertEqual(str(row['final_LAMAS_id']), expected_matches[row['osm_name']])

if __name__ == '__main__':
    unittest.main()
