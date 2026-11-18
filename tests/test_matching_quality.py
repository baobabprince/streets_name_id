import os
import tempfile
import unittest
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

import data_loader
from geospatial_validation import validate_candidate_geography
from local_ai_resolver import LocalAIResolver
from normalization import find_fuzzy_candidates, select_ai_candidates
from pipeline import _build_candidate_score_report, _merge_ai_suggestions


class TestMatchingQuality(unittest.TestCase):
    def test_candidate_score_report_is_saved_as_csv(self):
        candidate_scores = pd.DataFrame(
            {
                "osm_id": ["osm-1"],
                "lamas_id": ["101"],
                "fuzzy_score": [93.5],
            }
        )
        with tempfile.TemporaryDirectory() as output_dir:
            with patch.object(data_loader, "CACHE_DIR", output_dir):
                data_loader.save_intermediate_df(
                    candidate_scores, "step4_candidate_scores", "test-city"
                )
            path = os.path.join(
                output_dir, "step4_candidate_scores_test-city.csv"
            )
            saved = pd.read_csv(path)

        self.assertEqual(saved.loc[0, "lamas_id"], 101)
        self.assertEqual(saved.loc[0, "fuzzy_score"], 93.5)

    def test_fuzzy_matching_keeps_every_candidate_score_above_threshold(self):
        osm_gdf = pd.DataFrame(
            {"osm_id": ["osm-1"], "normalized_name": ["שדרות רוטשילד"]}
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "202", "303"],
                "LAMAS_name": ["שדרות רוטשילד", "רוטשילד", "שדרות רוטשילד מזרח"],
                "normalized_name": ["שדרות רוטשילד", "רוטשילד", "שדרות רוטשילד מזרח"],
            }
        )

        candidates = find_fuzzy_candidates(
            osm_gdf, lamas_df, confident_threshold=100, needs_ai_threshold=50
        )
        details = candidates.iloc[0]["candidate_details"]

        self.assertGreaterEqual(len(details), 2)
        self.assertEqual(details[0]["lamas_id"], "101")
        self.assertEqual(details[0]["score"], 100.0)
        self.assertTrue(all(candidate["score"] >= 50 for candidate in details))
        osm_for_report = osm_gdf.assign(
            osm_name="שדרות רוטשילד", city="יישוב בדיקה"
        )
        report = _build_candidate_score_report(candidates, osm_for_report)
        self.assertEqual(len(report), len(details))
        self.assertEqual(report.iloc[0]["osm_name"], "שדרות רוטשילד")
        self.assertGreaterEqual(report.iloc[0]["fuzzy_score"], 50)
        prompt_candidates = select_ai_candidates(details, "שדרות רוטשילד")
        self.assertLessEqual(len(prompt_candidates), 8)
        self.assertTrue(all(len(candidate["names"]) <= 3 for candidate in prompt_candidates))

    def test_local_ai_prompt_is_bounded_and_omits_city_lexicon(self):
        candidates = [
            {
                "id": str(index),
                "name": f"רחוב {index}",
                "names": [f"שם חלופי {alias}" for alias in range(20)],
                "score": 80.0 - index,
            }
            for index in range(20)
        ]
        prompt = LocalAIResolver.prepare_prompt(
            None,
            "רחוב בדיקה",
            "יישוב בדיקה",
            candidates,
            [f"רחוב סמוך {index}" for index in range(20)],
        )

        self.assertIn("עד 8 מועמדים", prompt)
        self.assertIn("עד 3 כינויים", prompt)
        self.assertIn("שם חלופי 2", prompt)
        self.assertNotIn("לקסיקון רחובות", prompt)
        self.assertNotIn("ID: 8", prompt)

    def test_geography_uses_exact_anchors_and_marks_distant_candidates(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["anchor", "nearby", "distant", "unreferenced"],
                "geometry": [
                    LineString([(34.80, 32.00), (34.801, 32.00)]),
                    LineString([(34.801, 32.00), (34.802, 32.00)]),
                    LineString([(34.82, 32.00), (34.821, 32.00)]),
                    LineString([(34.83, 32.00), (34.831, 32.00)]),
                ],
            },
            crs="EPSG:4326",
        )
        candidate = lambda lamas_id: [
            {"lamas_id": lamas_id, "lamas_name": "רחוב בדיקה", "score": 75.0}
        ]
        candidates_df = pd.DataFrame(
            {
                "osm_id": ["anchor", "nearby", "distant", "unreferenced"],
                "status": ["CONFIDENT", "NEEDS_AI", "NEEDS_AI", "NEEDS_AI"],
                "best_score": [100.0, 75.0, 75.0, 60.0],
                "best_LAMAS_id": ["101", "101", "101", "202"],
                "candidate_details": [
                    candidate("101"),
                    candidate("101"),
                    candidate("101"),
                    candidate("202"),
                ],
            }
        )

        validated = validate_candidate_geography(
            osm_gdf,
            candidates_df,
            {
                "anchor": ["nearby"],
                "nearby": ["anchor"],
                "distant": [],
                "unreferenced": [],
            },
        ).set_index("osm_id")

        self.assertEqual(
            validated.loc["anchor", "geographic_validation"], "EXACT_NAME_ANCHOR"
        )
        self.assertEqual(
            validated.loc["nearby", "geographic_validation"], "TOPOLOGY_CONNECTED"
        )
        self.assertEqual(
            validated.loc["distant", "geographic_validation"],
            "DISTANT_FROM_EXACT_REFERENCE",
        )
        self.assertEqual(
            validated.loc["unreferenced", "geographic_validation"],
            "NO_EXACT_NAME_REFERENCE",
        )
        self.assertGreater(
            validated.loc["distant", "geographic_distance_m"], 250
        )
        merged, approved = _merge_ai_suggestions(
            validated.reset_index(),
            pd.DataFrame(columns=["osm_id", "ai_suggested_LAMAS_id"]),
        )
        self.assertEqual(
            merged.set_index("osm_id").loc["distant", "mapping_status"],
            "GEOGRAPHIC_REVIEW",
        )
        self.assertNotIn("distant", approved["osm_id"].tolist())


if __name__ == "__main__":
    unittest.main()
