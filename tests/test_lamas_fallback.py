import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

import data_loader
import lamas_streets


class TestLamasFallback(unittest.TestCase):
    def test_api_uses_large_sql_pages_and_saves_complete_data(self):
        metadata_response = Mock()
        metadata_response.json.return_value = {"result": {"total": 2}}
        sql_response = Mock()
        sql_response.json.return_value = {
            "result": {
                "records": [
                    {
                        "_id": 1,
                        "official_code": "123",
                        "street_name": "רחוב הבדיקה",
                        "city_name": "אבו סנאן",
                    },
                    {
                        "_id": 2,
                        "official_code": "456",
                        "street_name": "רחוב נוסף",
                        "city_name": "אבו סנאן",
                    },
                ]
            }
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "lamas_data.csv")
            with (
                patch.object(lamas_streets, "CACHE_FILE", csv_path),
                patch.object(
                    lamas_streets.requests,
                    "get",
                    side_effect=[metadata_response, sql_response],
                ) as get,
            ):
                result = lamas_streets.fetch_all_LAMAS_data(force_refresh=True)

            self.assertTrue(os.path.exists(csv_path))

        self.assertEqual(len(result), 2)
        self.assertEqual(get.call_count, 2)
        self.assertIn("LIMIT 32000 OFFSET 0", get.call_args_list[1].kwargs["params"]["sql"])

    def test_manual_raw_csv_is_used_when_api_returns_403(self):
        manual_data = pd.DataFrame(
            {
                "official_code": [123],
                "street_name": ["רחוב הבדיקה"],
                "city_name": ["אבו סנאן"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "lamas_data.csv")
            pickle_path = os.path.join(temp_dir, "LAMAS_data.pkl")
            manual_data.to_csv(csv_path, index=False)
            with (
                patch.object(lamas_streets, "CACHE_FILE", csv_path),
                patch.object(data_loader, "LAMAS_CACHE", pickle_path),
                patch.object(
                    lamas_streets.requests,
                    "get",
                    side_effect=requests.HTTPError("403 Forbidden"),
                ),
            ):
                result = data_loader.load_or_fetch_LAMAS(force_refresh=True)

            self.assertTrue(os.path.exists(pickle_path))
        self.assertEqual(
            result.to_dict("records"),
            [{"LAMAS_id": "123", "LAMAS_name": "רחוב הבדיקה", "city": "אבו סנאן"}],
        )

    def test_stale_pickle_is_used_when_api_refresh_fails(self):
        cached_data = pd.DataFrame(
            {
                "LAMAS_id": ["123"],
                "LAMAS_name": ["רחוב הבדיקה"],
                "city": ["אבו סנאן"],
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = os.path.join(temp_dir, "LAMAS_data.pkl")
            cached_data.to_pickle(cache_path)
            with (
                patch.object(data_loader, "LAMAS_CACHE", cache_path),
                patch.object(
                    data_loader,
                    "fetch_all_LAMAS_data",
                    side_effect=requests.HTTPError("403 Forbidden"),
                ),
            ):
                result = data_loader.load_or_fetch_LAMAS(force_refresh=True)

        pd.testing.assert_frame_equal(result, cached_data)

    def test_api_error_is_raised_when_no_fallback_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = os.path.join(temp_dir, "missing.pkl")
            with (
                patch.object(data_loader, "LAMAS_CACHE", cache_path),
                patch.object(
                    data_loader,
                    "fetch_all_LAMAS_data",
                    side_effect=requests.HTTPError("403 Forbidden"),
                ),
            ):
                with self.assertRaises(requests.HTTPError):
                    data_loader.load_or_fetch_LAMAS(force_refresh=True)


if __name__ == "__main__":
    unittest.main()
