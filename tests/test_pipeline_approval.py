import unittest
from unittest.mock import Mock, patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

import pipeline
from pipeline import PipelineStatus, _finalize_mappings


class TestPipelineApproval(unittest.TestCase):
    def test_finalize_mappings_approves_fuzzy_and_verified_ai_matches(self):
        candidates = pd.DataFrame(
            {
                "osm_id": ["fuzzy", "ai", "miss"],
                "normalized_name": ["רחוב הרצל", "דוד בן גוריון", "רחוב לא קיים"],
                "status": ["CONFIDENT", "NEEDS_AI", "NEEDS_AI"],
                "best_score": [95.0, 82.0, 20.0],
                "best_LAMAS_id": ["101", "202", "999"],
                "best_LAMAS_name": ["רחוב הרצל", "בן גוריון", "שם שגוי"],
            }
        )
        ai_decisions = pd.DataFrame(
            {
                "osm_id": ["ai", "miss"],
                "ai_suggested_LAMAS_id": ["202", None],
                "ai_confidence": [0.89, 0.9],
                "ai_reasoning": ["Laya selected street_2", "Laya selected no_match"],
                "ai_method": ["local_laya", "local_laya"],
            }
        )

        merged, approved = _finalize_mappings(
            candidates,
            ai_decisions,
            lamas_name_by_id={"101": "רחוב הרצל", "202": "שדרות בן גוריון"},
            city_name="עיר בדיקה",
        )

        by_id = merged.set_index("osm_id")
        self.assertEqual(by_id.loc["fuzzy", "mapping_status"], "APPROVED_FUZZY")
        self.assertEqual(by_id.loc["ai", "mapping_status"], "APPROVED_AI")
        self.assertEqual(by_id.loc["ai", "best_LAMAS_name"], "שדרות בן גוריון")
        self.assertEqual(by_id.loc["miss", "mapping_status"], "UNMATCHED")

        approved_by_id = approved.set_index("osm_id")["final_LAMAS_id"].to_dict()
        self.assertEqual(approved_by_id, {"fuzzy": "101", "ai": "202"})

    def test_finalize_mappings_rejects_magnets_and_guardrail_violations(self):
        candidates = pd.DataFrame(
            {
                "osm_id": [
                    "disagree",
                    "low-conf",
                    "low-sim",
                    "digit-mismatch",
                    "city-name",
                    "mag-1",
                    "mag-2",
                    "mag-3",
                    "multi-seg-a",
                    "multi-seg-b",
                    "multi-seg-c",
                ],
                "normalized_name": [
                    "שדרות בן גוריון",
                    "דוד בן גוריון",
                    "אלקטרה מול הנוף",
                    "גדוד 21",
                    "צומת יודפת",
                    "סמטת האלון",
                    "דרך האלון",
                    "כיכר האלון",
                    "סמטת אורן",
                    "סמטת אורן",
                    "סמטת אורן",
                ],
                "status": ["NEEDS_AI"] * 11,
                "best_score": [82.0] * 11,
                "best_LAMAS_id": [
                    "999",  # disagrees with AI 202
                    "202",  # conf 0.65 < 0.70
                    "303",  # core_wratio < 75
                    "404",  # 21 != 22
                    "505",  # LAMAS name equals city_name
                    "606",  # 3 unique streets -> post_hits > 2
                    "606",
                    "606",
                    "707",  # 3 segments of SAME normalized_name -> 1 unique street (OK!)
                    "707",
                    "707",
                ],
                "best_LAMAS_name": [
                    "שם שגוי",
                    "בן גוריון",
                    "בנין המשטרה",
                    "גדוד 22",
                    "יודפת",
                    "האלון",
                    "האלון",
                    "האלון",
                    "אורן",
                    "אורן",
                    "אורן",
                ],
            }
        )
        ai_decisions = pd.DataFrame(
            {
                "osm_id": candidates["osm_id"].tolist(),
                "ai_suggested_LAMAS_id": [
                    "202",
                    "202",
                    "303",
                    "404",
                    "505",
                    "606",
                    "606",
                    "606",
                    "707",
                    "707",
                    "707",
                ],
                "ai_confidence": [
                    0.99,
                    0.65,
                    0.98,
                    0.95,
                    0.99,
                    0.95,
                    0.95,
                    0.95,
                    0.85,
                    0.85,
                    0.85,
                ],
                "ai_reasoning": ["Laya"] * 11,
                "ai_method": ["local_laya"] * 11,
            }
        )
        lamas_names = {
            "202": "בן גוריון",
            "303": "בנין המשטרה",
            "404": "גדוד 22",
            "505": "יודפת",
            "606": "האלון",
            "707": "אורן",
        }

        merged, approved = _finalize_mappings(
            candidates,
            ai_decisions,
            lamas_name_by_id=lamas_names,
            city_name="יודפת",
        )
        by_id = merged.set_index("osm_id")
        for rejected in (
            "disagree",
            "low-conf",
            "low-sim",
            "digit-mismatch",
            "city-name",
            "mag-1",
            "mag-2",
            "mag-3",
        ):
            self.assertEqual(by_id.loc[rejected, "mapping_status"], "UNMATCHED", rejected)
            self.assertIn("[filtered:", by_id.loc[rejected, "ai_reasoning"])

        # Same street split into 3 OSM segments counts as 1 unique street hit -> APPROVED_AI
        for seg in ("multi-seg-a", "multi-seg-b", "multi-seg-c"):
            self.assertEqual(by_id.loc[seg, "mapping_status"], "APPROVED_AI")
        self.assertEqual(set(approved["osm_id"]), {"multi-seg-a", "multi-seg-b", "multi-seg-c"})

    def test_pipeline_sends_all_below_90_streets_in_city_to_laya_and_approves(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-exact", "osm-low"],
                "osm_name": ["הרצל", "דוד בן גוריון"],
                "city": ["יישוב בדיקה", "יישוב בדיקה"],
                "geometry": [
                    LineString([(34.80, 32.00), (34.801, 32.00)]),
                    LineString([(34.802, 32.00), (34.803, 32.00)]),
                ],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "202"],
                "LAMAS_name": ["הרצל", "בן גוריון"],
                "city": ["יישוב בדיקה", "יישוב בדיקה"],
            }
        )
        resolver = Mock()
        resolver.resolve_city.return_value = {
            "דוד בן גוריון": {
                "lamas_id": "202",
                "confidence": 0.91,
                "reasoning": "Laya selected street_2",
                "method": "local_laya",
            }
        }
        saved = {}

        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(pipeline, "_get_laya_resolver", return_value=resolver),
            patch.object(
                pipeline,
                "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = pipeline.run_pipeline(
                "יישוב בדיקה",
                use_ai=True,
                skip_html=True,
            )

        self.assertEqual(status, PipelineStatus.SUCCESS)
        resolver.resolve_city.assert_called_once_with(
            "יישוב בדיקה",
            ["דוד בן גוריון"],
            {"101": ["הרצל"], "202": ["בן גוריון"]},
        )

        diagnostic = saved["diagnostic_report"].set_index("osm_id")
        self.assertEqual(diagnostic.loc["osm-exact", "mapping_status"], "APPROVED_FUZZY")
        self.assertEqual(diagnostic.loc["osm-exact", "final_LAMAS_id"], "101")
        self.assertEqual(diagnostic.loc["osm-low", "mapping_status"], "APPROVED_AI")
        self.assertEqual(diagnostic.loc["osm-low", "final_LAMAS_id"], "202")
        self.assertEqual(diagnostic.loc["osm-low", "best_LAMAS_name"], "בן גוריון")

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
        saved = {}
        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=osm_gdf),
            patch.object(
                pipeline,
                "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = pipeline.run_pipeline(
                "עיר בדיקה", use_ai=False, skip_html=True
            )
        diagnostic = saved["diagnostic_report"]
        self.assertEqual(status, PipelineStatus.SUCCESS)
        self.assertEqual(diagnostic.iloc[0]["final_LAMAS_id"], "1457")
        self.assertEqual(diagnostic.iloc[0]["final_LAMAS_key"], "3000:1457")
        self.assertEqual(diagnostic.iloc[0]["final_city_code"], "3000")
        self.assertEqual(diagnostic.iloc[0]["final_official_code"], "1457")


if __name__ == "__main__":
    unittest.main()
