import unittest
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

import pipeline
from pipeline import PipelineStatus, _merge_ai_suggestions


class TestPipelineApproval(unittest.TestCase):
    def test_ai_suggestions_do_not_become_approved_mappings(self):
        candidates = pd.DataFrame(
            {
                "osm_id": ["fuzzy", "ai", "geographic"],
                "status": ["CONFIDENT", "NEEDS_AI", "CONFIDENT"],
                "best_score": [100.0, 68.0, 82.0],
                "best_LAMAS_id": ["101", "202", "303"],
                "geographic_validation": [
                    "EXACT_NAME_ANCHOR",
                    "NO_EXACT_NAME_REFERENCE",
                    "DISTANT_FROM_EXACT_REFERENCE",
                ],
            }
        )
        ai_decisions = pd.DataFrame(
            {
                "osm_id": ["ai", "geographic"],
                "ai_suggested_LAMAS_id": ["202", "303"],
            }
        )

        merged, approved = _merge_ai_suggestions(candidates, ai_decisions)

        statuses = merged.set_index("osm_id")["mapping_status"]
        self.assertEqual(statuses["fuzzy"], "APPROVED_FUZZY")
        self.assertEqual(statuses["ai"], "AI_PROPOSED")
        self.assertEqual(statuses["geographic"], "GEOGRAPHIC_REVIEW")
        self.assertEqual(approved["osm_id"].tolist(), ["fuzzy"])
        self.assertEqual(approved["final_LAMAS_id"].tolist(), ["101"])

    def test_pipeline_keeps_gemini_result_as_a_separate_proposal(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-1"],
                "osm_name": ["שם רחוב"],
                "city": ["יישוב בדיקה"],
                "geometry": [LineString([(34.80, 32.00), (34.801, 32.00)])],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101"],
                "LAMAS_name": ["שם למ״ס"],
                "city": ["יישוב בדיקה"],
            }
        )
        candidates = pd.DataFrame(
            {
                "osm_id": ["osm-1"],
                "status": ["NEEDS_AI"],
                "best_score": [68.0],
                "best_LAMAS_id": ["101"],
                "best_LAMAS_name": ["שם למ״ס"],
                "all_candidates": ["ID: 101, Names: [שם למ״ס], Score: 68.0"],
                "candidate_details": [
                    [
                        {
                            "lamas_id": "101",
                            "lamas_name": "שם למ״ס",
                            "matched_alias": "שם למ״ס",
                            "score": 68.0,
                            "aliases": ["שם למ״ס"],
                        }
                    ]
                ],
            }
        )
        saved = {}

        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(pipeline, "build_adjacency_map", return_value={}),
            patch.object(pipeline, "find_fuzzy_candidates", return_value=candidates),
            patch.object(
                pipeline,
                "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
            patch.object(pipeline, "API_KEY", "configured"),
            patch.object(pipeline, "get_ai_resolution", return_value="101"),
        ):
            status = pipeline.run_pipeline(
                "יישוב בדיקה",
                use_ai=True,
                use_local_ai=False,
                use_gemini=True,
                skip_html=True,
            )

        diagnostic = saved["diagnostic_report"]
        self.assertEqual(status, PipelineStatus.SUCCESS)
        self.assertEqual(diagnostic.iloc[0]["ai_suggested_LAMAS_id"], "101")
        self.assertEqual(diagnostic.iloc[0]["mapping_status"], "AI_PROPOSED")
        self.assertTrue(pd.isna(diagnostic.iloc[0]["final_LAMAS_id"]))

    def test_diagnostic_report_includes_globally_unique_lamas_key(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-1"],
                "osm_name": ["רחוב ארוך"],
                "city": ["עיר בדיקה"],
                "geometry": [LineString([(34.80, 32.00), (34.801, 32.00)])],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["1457"],
                "LAMAS_name": ["רחוב ארוך"],
                "city": ["עיר בדיקה"],
                "city_code": ["3000"],
                "official_code": ["1457"],
                "street_code": ["1457"],
                "street_name_status": ["official"],
                "is_official": [True],
                "LAMAS_key": ["3000:1457"],
            }
        )
        candidates = pd.DataFrame(
            {
                "osm_id": ["osm-1"],
                "status": ["CONFIDENT"],
                "best_score": [100.0],
                "best_LAMAS_id": ["1457"],
                "best_LAMAS_name": ["רחוב ארוך"],
                "all_candidates": ["ID: 1457"],
                "candidate_details": [[]],
                "geographic_validation": ["EXACT_NAME_ANCHOR"],
                "geographic_distance_m": [0.0],
            }
        )
        saved = {}
        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(pipeline, "build_adjacency_map", return_value={}),
            patch.object(pipeline, "find_fuzzy_candidates", return_value=candidates),
            patch.object(
                pipeline, "validate_candidate_geography", return_value=candidates
            ),
            patch.object(
                pipeline, "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = pipeline.run_pipeline(
                "עיר בדיקה", use_ai=False, skip_html=True,
            )
        diagnostic = saved["diagnostic_report"]
        self.assertEqual(status, PipelineStatus.SUCCESS)
        self.assertEqual(diagnostic.iloc[0]["final_LAMAS_id"], "1457")
        self.assertEqual(diagnostic.iloc[0]["final_LAMAS_key"], "3000:1457")
        self.assertEqual(diagnostic.iloc[0]["final_city_code"], "3000")
        self.assertEqual(diagnostic.iloc[0]["final_official_code"], "1457")


if __name__ == "__main__":
    unittest.main()
