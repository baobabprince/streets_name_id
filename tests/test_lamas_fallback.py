import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

import data_loader
import lamas_streets


class TestLamasFallback(unittest.TestCase):
    def test_api_uses_datastore_search_and_saves_complete_data(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "success": True,
            "result": {
                "total": 2,
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
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        session.headers = {}
        session.get.return_value = response

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, "lamas_data.csv")
            with (
                patch.object(lamas_streets, "CACHE_FILE", csv_path),
                patch.object(lamas_streets.requests, "Session", return_value=session),
            ):
                result = lamas_streets.fetch_all_LAMAS_data(force_refresh=True)

            self.assertTrue(os.path.exists(csv_path))

        self.assertEqual(len(result), 2)
        session.get.assert_called_once()
        self.assertEqual(
            session.get.call_args.kwargs["params"],
            {
                "resource_id": lamas_streets.RESOURCE_ID,
                "limit": lamas_streets.LIMIT,
                "offset": 0,
            },
        )
        self.assertEqual(
            session.headers["User-Agent"], "datagov-external-client"
        )

    def test_api_paginates_until_reported_total(self):
        def page(offset, records, total=3):
            response = Mock()
            response.status_code = 200
            response.json.return_value = {
                "success": True,
                "result": {"total": total, "records": records},
            }
            return response

        pages = [
            page(0, [
                {"official_code": "1", "street_name": "א", "city_name": "עיר"},
                {"official_code": "2", "street_name": "ב", "city_name": "עיר"},
            ]),
            page(2, [
                {"official_code": "3", "street_name": "ג", "city_name": "עיר"},
            ]),
        ]
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        session.headers = {}
        session.get.side_effect = pages

        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(lamas_streets, "CACHE_FILE", os.path.join(temp_dir, "lamas.csv")),
            patch.object(lamas_streets, "LIMIT", 2),
            patch.object(lamas_streets.requests, "Session", return_value=session),
        ):
            result = lamas_streets.fetch_all_LAMAS_data(force_refresh=True)

        self.assertEqual(len(result), 3)
        self.assertEqual(
            [call.kwargs["params"]["offset"] for call in session.get.call_args_list],
            [0, 2],
        )

    def test_api_retries_transient_server_errors(self):
        response_503 = Mock(status_code=503)
        response_ok = Mock(status_code=200)
        response_ok.json.return_value = {
            "success": True,
            "result": {
                "total": 1,
                "records": [
                    {"official_code": "1", "street_name": "א", "city_name": "עיר"}
                ],
            },
        }
        session = Mock()
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        session.headers = {}
        session.get.side_effect = [response_503, response_ok]
        response_503.headers = {}

        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.object(lamas_streets, "CACHE_FILE", os.path.join(temp_dir, "lamas.csv")),
            patch.object(lamas_streets.requests, "Session", return_value=session),
            patch.object(lamas_streets.time, "sleep"),
        ):
            result = lamas_streets.fetch_all_LAMAS_data(force_refresh=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(session.get.call_count, 2)

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
            session = Mock()
            session.__enter__ = Mock(return_value=session)
            session.__exit__ = Mock(return_value=False)
            session.get.side_effect = requests.HTTPError("403 Forbidden")
            with (
                patch.object(lamas_streets, "CACHE_FILE", csv_path),
                patch.object(data_loader, "LAMAS_CACHE", pickle_path),
                patch.object(lamas_streets.requests, "Session", return_value=session),
            ):
                result = data_loader.load_or_fetch_LAMAS(force_refresh=True)

            self.assertTrue(os.path.exists(pickle_path))
        record = result.to_dict("records")[0]
        self.assertEqual(record["LAMAS_id"], "123")
        self.assertEqual(record["official_code"], "123")
        self.assertEqual(record["LAMAS_name"], "רחוב הבדיקה")
        self.assertEqual(record["city"], "אבו סנאן")
        self.assertTrue(record["is_official"])
        self.assertTrue(record["LAMAS_key"].endswith(":123"))

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

        self.assertEqual(result.iloc[0]["LAMAS_id"], "123")
        self.assertEqual(result.iloc[0]["LAMAS_name"], "רחוב הבדיקה")
        self.assertEqual(result.iloc[0]["city"], "אבו סנאן")
        self.assertEqual(result.iloc[0]["official_code"], "123")
        self.assertEqual(result.iloc[0]["LAMAS_key"], "אבוסנאנ:123")
        self.assertTrue(result.iloc[0]["is_official"])

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
