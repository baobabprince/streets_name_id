"""Steps 3-4 of the log-analysis fixes:

Step 3 — the Laya input gate: every unmatched OSM street is sent to Laya
(previously the hardest streets were filtered out *before* the AI ran), while
precision stays protected by the output gates in ``_finalize_mappings``
(including overrides: Laya may replace a wrong fuzzy top-1 with stronger
evidence).

Step 4 — dedicated "nothing to map" outcomes: ``NO_LAMAS_STREETS`` for
settlements missing from LAMAS or present only as their own-name pseudo-record,
automatic documentation of no-data settlements in the batch report, and
NON_ADDRESSABLE (infrastructure) OSM streets reported separately in the
diagnostics instead of inflating the unmatched percentage.
"""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

import pipeline
from diagnostics import calculate_diagnostics
from pipeline import PipelineStatus, _finalize_mappings, run_pipeline


def _line(**offset):
    x = offset.get("x", 34.80)
    y = offset.get("y", 32.00)
    return LineString([(x, y), (x + 0.001, y)])


class TestAllUnmatchedGoToLaya(unittest.TestCase):
    """Step 3: no input-side eligibility filter before the AI runs."""

    def setUp(self):
        self.original_resolver = pipeline._LAYA_RESOLVER
        pipeline._LAYA_RESOLVER = None

    def tearDown(self):
        pipeline._LAYA_RESOLVER = self.original_resolver

    def test_run_pipeline_sends_every_unmatched_street_to_laya(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-exact", "osm-low-sim", "osm-harder"],
                "osm_name": ["הרצל", "אלקטרה מול הנוף", "צריח בלורית עתיק"],
                "city": ["יישוב בדיקה"] * 3,
                "geometry": [_line(), _line(x=34.81), _line(x=34.82)],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "303", "505"],
                "LAMAS_name": ["הרצל", "בנין המשטרה", "יישוב בדיקה"],
                "city": ["יישוב בדיקה"] * 3,
            }
        )
        resolver = Mock()
        # Laya returns no_match for both hard streets; the assertion is on the
        # *input*: both must reach the resolver at all.
        resolver.resolve_city.return_value = {}
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
            status = run_pipeline("יישוב בדיקה", use_ai=True, skip_html=True)

        self.assertEqual(status, PipelineStatus.SUCCESS)
        sent_names = resolver.resolve_city.call_args.args[1]
        # Both streets fail the OLD eligibility gate (their fuzzy top-1 scores
        # a core WRatio far below 75) — under the old flow Laya never saw
        # them.  Both must reach the resolver now.
        self.assertIn("אלקטרה מול הנוף", sent_names)
        self.assertIn("צריח בלורית עתיק", sent_names)
        self.assertNotIn("הרצל", sent_names)  # the CONFIDENT street stays out
    def test_laya_override_survives_end_to_end(self):
        # The fuzzy stage's top-1 for the unmatched street is wrong; Laya
        # contradicts it with strong evidence and the override is approved.
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-exact", "osm-unmatched"],
                "osm_name": ["הרצל", "דוד בן גוריון"],
                "city": ["יישוב בדיקה"] * 2,
                "geometry": [_line(), _line(x=34.81)],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "202"],
                "LAMAS_name": ["הרצל", "בן גוריון"],
                "city": ["יישוב בדיקה"] * 2,
            }
        )
        resolver = Mock()
        resolver.resolve_city.return_value = {
            "דוד בן גוריון": {
                "lamas_id": "202",
                "confidence": 0.88,
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
            status = run_pipeline("יישוב בדיקה", use_ai=True, skip_html=True)

        self.assertEqual(status, PipelineStatus.SUCCESS)
        diagnostic = saved["diagnostic_report"].set_index("osm_id")
        self.assertEqual(diagnostic.loc["osm-unmatched", "mapping_status"], "APPROVED_AI")
        self.assertEqual(diagnostic.loc["osm-unmatched", "final_LAMAS_id"], "202")
        self.assertEqual(diagnostic.loc["osm-unmatched", "best_LAMAS_name"], "בן גוריון")


class TestNoLamasStreetsStatus(unittest.TestCase):
    """Step 4: settlements with no street-level LAMAS records bail out early."""

    def _only_pseudo_record_lamas(self, settlement="יישוב קטן"):
        return pd.DataFrame(
            {
                "LAMAS_id": ["505"],
                "LAMAS_name": [settlement],
                "city": [settlement],
            }
        )

    def _osm_gdf(self, settlement="יישוב קטן"):
        return gpd.GeoDataFrame(
            {
                "osm_id": ["osm-1", "osm-2"],
                "osm_name": ["הגפן", "הכרם"],
                "city": [settlement] * 2,
                "geometry": [_line(), _line(x=34.81)],
            },
            crs="EPSG:4326",
        )

    def test_only_settlement_name_record_returns_no_lamas_streets(self):
        saved = {}
        with (
            patch.object(
                pipeline, "load_or_fetch_LAMAS", return_value=self._only_pseudo_record_lamas()
            ),
            patch.object(pipeline, "load_or_fetch_osm", return_value=self._osm_gdf()),
            patch.object(
                pipeline,
                "save_intermediate_df",
                side_effect=lambda df, name, place: saved.setdefault(name, df.copy()),
            ),
        ):
            status = run_pipeline("יישוב קטן", use_ai=False, skip_html=True)

        self.assertEqual(status, PipelineStatus.NO_LAMAS_STREETS)
        self.assertIn("diagnostic_report", saved)
        diagnostic = saved["diagnostic_report"]
        self.assertTrue((diagnostic["status"] == "NO_LAMAS_STREETS").all())
        self.assertTrue((diagnostic["mapping_status"] == "UNMATCHED").all())
        self.assertIn("settlement-name record", diagnostic["ai_reasoning"].iloc[0])

    def test_no_lamas_streets_skips_html_page(self):
        with (
            patch.object(
                pipeline, "load_or_fetch_LAMAS", return_value=self._only_pseudo_record_lamas()
            ),
            patch.object(pipeline, "load_or_fetch_osm", return_value=self._osm_gdf()),
            patch.object(pipeline, "save_intermediate_df"),
            patch("generate_html.create_html_from_gdf") as render,
        ):
            status = run_pipeline("יישוב קטן", use_ai=False)

        self.assertEqual(status, PipelineStatus.NO_LAMAS_STREETS)
        # With no CBS streets there is nothing to match against, so the pipeline
        # must NOT emit a misleading 0%-coverage HTML page. Such settlements are
        # surfaced via build_no_results instead.
        render.assert_not_called()

    def test_empty_city_slice_returns_no_lamas_streets(self):
        # LAMAS has no records at all for this settlement's key.
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101"],
                "LAMAS_name": ["רחוב כלשהו"],
                "city": ["יישוב אחר לגמרי"],
            }
        )
        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=self._osm_gdf()),
            patch.object(pipeline, "save_intermediate_df"),
        ):
            status = run_pipeline("יישוב קטן", use_ai=False, skip_html=True)

        self.assertEqual(status, PipelineStatus.NO_LAMAS_STREETS)

    def test_real_street_records_still_run_the_full_pipeline(self):
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "505"],
                "LAMAS_name": ["הגפן", "יישוב קטן"],
                "city": ["יישוב קטן"] * 2,
            }
        )
        with (
            patch.object(pipeline, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(pipeline, "load_or_fetch_osm", return_value=self._osm_gdf()),
            patch.object(pipeline, "save_intermediate_df"),
        ):
            status = run_pipeline("יישוב קטן", use_ai=False, skip_html=True)

        self.assertEqual(status, PipelineStatus.SUCCESS)


class TestBatchNoDataStatus(unittest.TestCase):
    def test_worker_wrapper_maps_no_data_statuses(self):
        import batch_process_settlements as batch

        for status in (PipelineStatus.NO_DATA, PipelineStatus.NO_LAMAS_STREETS):
            with patch.object(batch, "run_pipeline", return_value=status):
                result = batch.worker_wrapper("יישוב", "יישוב, Israel", use_ai=False)
            self.assertTrue(result["pipeline_success"], status)
            self.assertEqual(result["status"], "no_data")
            self.assertIn("nothing to map", result["message"])

    def test_worker_wrapper_failure_still_fails(self):
        import batch_process_settlements as batch

        with patch.object(batch, "run_pipeline", return_value=PipelineStatus.FAILURE):
            result = batch.worker_wrapper("יישוב", "יישוב, Israel", use_ai=False)
        self.assertFalse(result["pipeline_success"])
        self.assertEqual(result["status"], "failed_pipeline")

    def test_no_data_file_is_written_and_unions_existing_entries(self):
        import batch_process_settlements as batch

        processor = batch.BatchProcessor()
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "no_data_settlements.txt")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("## manual header\nיישוב ישן\n")

            written = processor._update_no_data_file(["יישוב חדש", "יישוב ישן"], path=path)
            self.assertEqual(written, path)

            with open(path, "r", encoding="utf-8") as handle:
                lines = [line.strip() for line in handle]
            self.assertIn("יישוב ישן", lines)  # preserved
            self.assertIn("יישוב חדש", lines)  # added
            self.assertTrue(any(line.startswith("##") for line in lines))

            # Empty input must not rewrite the file.
            mtime = os.path.getmtime(path)
            self.assertIsNone(processor._update_no_data_file([], path=path))
            self.assertEqual(os.path.getmtime(path), mtime)

    def test_run_all_exit_code_counts_no_data_as_completed(self):
        import run_all

        self.assertEqual(
            run_all._exit_code({"statistics": {
                "total_settlements": 3,
                "successful": 1,
                "no_data": 2,
                "failed_pipeline": 0,
                "skipped_already_processed": 0,
            }}),
            0,
        )
        self.assertEqual(
            run_all._exit_code({"statistics": {
                "total_settlements": 3,
                "successful": 1,
                "no_data": 1,
                "failed_pipeline": 1,
                "skipped_already_processed": 0,
            }}),
            1,
        )


class TestNonAddressableDiagnostics(unittest.TestCase):
    """Step 4: infrastructure ways are reported separately from unmatched."""

    def _diagnose(self, osm_rows):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": [f"osm-{i}" for i in range(len(osm_rows))],
                "osm_name": [row[0] for row in osm_rows],
                "normalized_name": [row[0] for row in osm_rows],
                "highway": [row[1] for row in osm_rows],
                "geometry": [Point(1, 1)] * len(osm_rows),
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101"],
                "LAMAS_name": ["רחוב מותאם"],
                "city": ["יישוב בדיקה"],
            }
        )
        diagnostic_df = pd.DataFrame(
            {
                "normalized_name": [row[0] for row in osm_rows],
                "final_LAMAS_id": ["101" if row[0] == "רחוב מותאם" else None
                                   for row in osm_rows],
            }
        )
        return calculate_diagnostics(lamas_df, diagnostic_df, osm_gdf)

    def test_footway_streets_are_reported_as_non_addressable(self):
        diagnostics = self._diagnose(
            [
                ("רחוב מותאם", "residential"),
                ("שביל הליכה", "footway"),
                ("ציר שירות", "service"),
            ]
        )
        self.assertEqual(diagnostics["unmatched_osm_streets"], 0)
        self.assertEqual(diagnostics["non_addressable_osm_streets"], 2)
        self.assertEqual(
            sorted(diagnostics["non_addressable_osm_street_names"]),
            ["ציר שירות", "שביל הליכה"],
        )
        self.assertEqual(diagnostics["unmatched_osm_street_names"], [])

    def test_mixed_highway_types_stay_unmatched(self):
        # One named street, two segments: footway + residential.  Because one
        # segment is addressable the street must NOT be classified as
        # non-addressable noise.
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-a", "osm-b"],
                "osm_name": ["דרך חצר", "דרך חצר"],
                "normalized_name": ["דרך חצר", "דרך חצר"],
                "highway": ["footway", "residential"],
                "geometry": [Point(1, 1), Point(2, 1)],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101"],
                "LAMAS_name": ["רחוב מותאם"],
                "city": ["יישוב בדיקה"],
            }
        )
        diagnostic_df = pd.DataFrame(
            {
                "normalized_name": ["רחוב מותאם", "דרך חצר"],
                "final_LAMAS_id": ["101", None],
            }
        )
        diagnostics = calculate_diagnostics(lamas_df, diagnostic_df, osm_gdf)
        self.assertEqual(diagnostics["unmatched_osm_streets"], 1)
        self.assertEqual(diagnostics["non_addressable_osm_streets"], 0)
        self.assertEqual(diagnostics["unmatched_osm_street_names"], ["דרך חצר"])

    def test_without_highway_column_behaviour_is_unchanged(self):
        osm_gdf = gpd.GeoDataFrame(
            {
                "osm_id": ["osm-1", "osm-2"],
                "normalized_name": ["רחוב מותאם", "שביל הליכה"],
                "geometry": [Point(1, 1), Point(2, 1)],
            },
            crs="EPSG:4326",
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101"],
                "LAMAS_name": ["רחוב מותאם"],
                "city": ["יישוב בדיקה"],
            }
        )
        diagnostic_df = pd.DataFrame(
            {
                "normalized_name": ["רחוב מותאם", "שביל הליכה"],
                "final_LAMAS_id": ["101", None],
            }
        )
        diagnostics = calculate_diagnostics(lamas_df, diagnostic_df, osm_gdf)
        self.assertEqual(diagnostics["unmatched_osm_streets"], 1)
        self.assertEqual(diagnostics["unmatched_osm_street_names"], ["שביל הליכה"])
        self.assertEqual(diagnostics["non_addressable_osm_streets"], 0)


if __name__ == "__main__":
    unittest.main()
