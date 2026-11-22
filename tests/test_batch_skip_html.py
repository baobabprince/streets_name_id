import json
import unittest
from unittest.mock import patch

from batch_process_settlements import (
    OFFICIAL_CITIES_FILE,
    filter_to_official_cities,
    worker_wrapper,
)
from pipeline import PipelineStatus


class TestBatchSkipHtml(unittest.TestCase):
    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.SUCCESS)
    def test_worker_passes_skip_html_to_pipeline(self, run_pipeline):
        worker_wrapper(
            "יישוב",
            "יישוב, Israel",
            use_ai=False,
            skip_html=True,
        )

        self.assertTrue(run_pipeline.call_args.kwargs["skip_html"])

    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.NO_DATA)
    def test_non_success_pipeline_status_is_not_treated_as_success(self, run_pipeline):
        result = worker_wrapper(
            "יישוב",
            "יישוב, Israel",
            use_ai=False,
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
        )

        self.assertEqual(run_pipeline.call_args.kwargs["place"], place)

    @patch("batch_process_settlements.run_pipeline", return_value=PipelineStatus.SUCCESS)
    def test_worker_passes_use_ai_to_pipeline(self, run_pipeline):
        worker_wrapper(
            "יישוב",
            "יישוב, Israel",
            use_ai=True,
        )

        self.assertTrue(run_pipeline.call_args.kwargs["use_ai"])

    def test_city_filter_selects_exactly_the_configured_83_cities(self):
        with open(OFFICIAL_CITIES_FILE, "r", encoding="utf-8") as file:
            cities = json.load(file)["cities"]

        settlements = filter_to_official_cities(["יישוב נוסף", *cities])

        self.assertEqual(len(settlements), 83)
        self.assertNotIn("יישוב נוסף", settlements)
        self.assertEqual(set(settlements), set(cities))

    def test_city_filter_fails_if_lamas_data_is_missing_a_city(self):
        with open(OFFICIAL_CITIES_FILE, "r", encoding="utf-8") as file:
            cities = json.load(file)["cities"]

        with self.assertRaisesRegex(ValueError, "missing official city names"):
            filter_to_official_cities(cities[:-1])


if __name__ == "__main__":
    unittest.main()
