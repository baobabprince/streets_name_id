import unittest
from unittest.mock import patch

import pandas as pd

import batch_process_settlements as batch
import run_all
from pipeline import PipelineStatus
from settlement_index import ResolveResult


class TestBatchOrchestration(unittest.TestCase):
    def test_worker_passes_lamas_city_and_intermediate_flag(self):
        with patch.object(batch, "run_pipeline", return_value=PipelineStatus.SUCCESS) as run:
            batch.worker_wrapper(
                "תל אביב - יפו", "תל אביב - יפו, Israel",
                False, False, save_intermediates=False,
            )
        self.assertEqual(run.call_args.kwargs["lamas_city"], "תל אביב - יפו")
        self.assertFalse(run.call_args.kwargs["save_intermediates"])

    def test_single_run_acquires_matches_processes_and_refreshes_dashboard(self):
        args = batch.build_cli_parser().parse_args(["--cities", "--force", "--no-pbf"])
        lamas_df = pd.DataFrame({
            "city": ["חיפה", "אילת"],
            "LAMAS_id": ["1", "2"],
            "LAMAS_name": ["רחוב", "רחוב"],
        })
        resolution = ResolveResult(
            matches={"חיפה": {"osm_id": "1", "display_name": "חיפה"}},
            unresolved={"אילת": "not found locally"},
            stats={"requested": 2, "matched": 1, "unresolved": 1,
                   "matched_offline": 1, "matched_nominatim": 0,
                   "reused_from_store": 0},
        )
        with (
            patch.object(batch, "load_or_fetch_LAMAS", return_value=lamas_df) as load,
            patch.object(batch, "_prepare_osm_caches") as prepare,
            patch.object(batch, "SettlementMatchStore") as store_cls,
            patch.object(batch, "SettlementResolver") as resolver_cls,
            patch.object(batch, "write_match_report", return_value="report.csv") as report,
            patch.object(batch, "BatchProcessor") as processor_cls,
            patch.object(batch, "_refresh_dashboard") as dashboard,
            patch.object(batch, "filter_to_official_cities",
                         side_effect=lambda names: names),
        ):
            resolver_cls.return_value.resolve.return_value = resolution
            processor_cls.return_value.run_batch.return_value = {
                "statistics": {"total_settlements": 2, "successful": 1,
                               "failed_pipeline": 0,
                               "skipped_already_processed": 0},
            }
            summary = batch.run_everything(args)

        load.assert_called_once()
        prepare.assert_called_once()
        store_cls.assert_called_once()
        resolver_cls.return_value.resolve.assert_called_once_with(
            ["אילת", "חיפה"], refresh=False
        )
        report.assert_called_once()
        processor_cls.return_value.run_batch.assert_called_once()
        self.assertTrue(
            processor_cls.return_value.run_batch.call_args.kwargs["force"]
        )
        dashboard.assert_called_once()
        self.assertEqual(summary["statistics"]["successful"], 1)

    def test_match_only_skips_pipelines_and_dashboard(self):
        args = batch.build_cli_parser().parse_args(["--match-only", "--no-pbf"])
        lamas_df = pd.DataFrame({"city": ["חיפה"]})
        resolution = ResolveResult(matches={}, unresolved={"חיפה": "x"}, stats={})
        with (
            patch.object(batch, "load_or_fetch_LAMAS", return_value=lamas_df),
            patch.object(batch, "_prepare_osm_caches"),
            patch.object(batch, "SettlementMatchStore"),
            patch.object(batch, "SettlementResolver") as resolver_cls,
            patch.object(batch, "write_match_report", return_value="report.csv"),
            patch.object(batch, "BatchProcessor") as processor_cls,
            patch.object(batch, "_refresh_dashboard") as dashboard,
        ):
            resolver_cls.return_value.resolve.return_value = resolution
            summary = batch.run_everything(args)
        processor_cls.assert_not_called()
        dashboard.assert_not_called()
        self.assertIn("match_report", summary)

    def test_dashboard_only_does_not_fetch_sources(self):
        args = batch.build_cli_parser().parse_args(["--dashboard-only"])
        with (
            patch.object(batch, "load_or_fetch_LAMAS") as load,
            patch.object(batch, "_refresh_dashboard") as dashboard,
        ):
            summary = batch.run_everything(args)
        load.assert_not_called()
        dashboard.assert_called_once()
        self.assertEqual(summary, {"dashboard_only": True})

    def test_run_all_presets_and_strict_exit_code(self):
        captured = {}

        def fake_run(args):
            captured["args"] = args
            return {
                "statistics": {"total_settlements": 2, "successful": 1,
                               "failed_pipeline": 1,
                               "skipped_already_processed": 0},
                "settlement_matching": {},
            }

        with patch.object(run_all, "run_everything", side_effect=fake_run):
            code = run_all.main(["--fast", "--refresh-all"])
        args = captured["args"]
        self.assertTrue(args.refresh)
        self.assertTrue(args.refresh_matches)
        self.assertTrue(args.force)
        self.assertTrue(args.skip_html)
        self.assertTrue(args.no_intermediates)
        self.assertFalse(args.use_ai)
        self.assertEqual(code, 1)  # one pipeline failed -> strict failure

        self.assertEqual(run_all._exit_code({"statistics": {
            "total_settlements": 2, "successful": 1,
            "failed_pipeline": 0, "skipped_already_processed": 1,
        }}), 0)


if __name__ == "__main__":
    unittest.main()
