import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

import data_loader


class TestDataLoaderCaches(unittest.TestCase):
    def setUp(self):
        data_loader._LAMAS_MEMORY_CACHE.clear()

    def test_lamas_pickle_is_read_once_per_process(self):
        frame = pd.DataFrame({
            "LAMAS_id": ["101"], "LAMAS_name": ["רחוב"], "city": ["חיפה"],
        })
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = os.path.join(temp_dir, "LAMAS_data.pkl")
            frame.to_pickle(cache_path)
            real_read = pd.read_pickle
            with (
                patch.object(data_loader, "LAMAS_CACHE", cache_path),
                patch.object(data_loader.pd, "read_pickle", wraps=real_read) as read,
            ):
                first = data_loader.load_or_fetch_LAMAS()
                second = data_loader.load_or_fetch_LAMAS()

            pd.testing.assert_frame_equal(first, frame)
            pd.testing.assert_frame_equal(second, frame)
            self.assertEqual(read.call_count, 1)

    def test_city_key_uses_shared_canonical_settlement_key(self):
        keys = data_loader.city_key(
            pd.Series(["תל אביב - יפו", "אבו קרינאת )יישוב(", "קרית גת"])
        ).tolist()
        self.assertEqual(keys, ["תלאביביפו", "אבוקרינאת", "קרייתגת"])


if __name__ == "__main__":
    unittest.main()
