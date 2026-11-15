import unittest
from unittest.mock import patch

from batch_process_settlements import worker_wrapper
from pipeline import PipelineStatus


class TestBatchSkipHtml(unittest.TestCase):
    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.SUCCESS)
    def test_worker_passes_skip_html_to_pipeline(self, run_pipeline):
        worker_wrapper(
            "יישוב",
            "יישוב, Israel",
            use_ai=False,
            use_local_ai=False,
            skip_html=True,
        )

        self.assertTrue(run_pipeline.call_args.kwargs["skip_html"])

    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.NO_DATA)
    def test_non_success_pipeline_status_is_not_treated_as_success(self, run_pipeline):
        result = worker_wrapper(
            "יישוב",
            "יישוב, Israel",
            use_ai=False,
            use_local_ai=False,
        )

        self.assertFalse(result["pipeline_success"])
        self.assertEqual(result["status"], "failed_pipeline")
        self.assertEqual(result["message"], "Pipeline returned NO_DATA")

    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.SUCCESS)
    def test_worker_passes_structured_osm_match_to_pipeline(self, run_pipeline):
        place = {
            "display_name": "אבו גוש, ישראל",
            "osm_id": "123",
            "osm_type": "relation",
            "lat": 31.8,
            "lon": 35.1,
        }

        worker_wrapper(
            "אבו גוש",
            place,
            use_ai=False,
            use_local_ai=False,
        )

        self.assertEqual(run_pipeline.call_args.kwargs["place"], place)


if __name__ == "__main__":
    unittest.main()
