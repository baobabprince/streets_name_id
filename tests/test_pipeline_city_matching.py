import unittest
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

import pipeline
from pipeline import PipelineStatus


class TestPipelineCityMatching(unittest.TestCase):
    def _candidates(self):
        return pd.DataFrame({
            "osm_id": ["osm-1"],
            "status": ["CONFIDENT"],
            "best_score": [100.0],
            "best_LAMAS_id": ["101"],
            "best_LAMAS_name": ["רחוב הרצל"],
        })

    def test_canonical_city_key_matches_lamas_rows_despite_name_mismatch(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-1"],
                "osm_name": ["רחוב הרצל"],
                "city": ["Tel Aviv-Yafo, Israel"],
                "geometry": [LineString([(34.78, 32.08), (34.781, 32.08)])],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame({
            "LAMAS_id": ["101"],
            "LAMAS_name": ["רחוב הרצל"],
            "city": ["תל אביב יפו"],  # LAMAS spelling without dash
        })
        captured = {}
        saved = {}

        def capture(osm_frame, lamas_city_df, *args, **kwargs):
            captured["lamas_rows"] = lamas_city_df.copy()
            return self._candidates()

        with (
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(pipeline, "find_fuzzy_candidates", side_effect=capture),
            patch.object(
                pipeline, "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = pipeline.run_pipeline(
                {"display_name": "Tel Aviv-Yafo, Israel", "osm_id": "1",
                 "osm_type": "relation", "lat": 32.08, "lon": 34.78,
                 "boundingbox": (32.07, 32.09, 34.77, 34.79)},
                lamas_df=lamas_df,
                lamas_city="תל אביב - יפו",
                use_ai=False,
                skip_html=True,
                save_intermediates=False,
            )

        self.assertEqual(status, PipelineStatus.SUCCESS)
        self.assertEqual(len(captured["lamas_rows"]), 1)
        self.assertIn("diagnostic_report", saved)
        # Bulky pickles are skipped, CSV audit output is kept.
        self.assertNotIn("step1_lamas_raw", saved)
        self.assertNotIn("step2_osm_normalized", saved)
        self.assertEqual(
            saved["diagnostic_report"].iloc[0]["city"], "תל אביב - יפו"
        )

    def test_filter_lamas_by_city_has_tight_fuzzy_fallback(self):
        lamas_df = pd.DataFrame({
            "city": ["פתח תקווה", "חיפה"],
            "city_key": ["פתחתקווה", "חיפה"],
        })
        rows = pipeline._filter_lamas_by_city(lamas_df, "פתחתקוה")
        self.assertEqual(rows["city"].tolist(), ["פתח תקווה"])
        self.assertTrue(
            pipeline._filter_lamas_by_city(lamas_df, "ישוב לא קיים").empty
        )


if __name__ == "__main__":
    unittest.main()
