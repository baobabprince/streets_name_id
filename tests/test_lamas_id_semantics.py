import unittest

import pandas as pd

from lamas_streets import _normalize_lamas_data


class TestLamasIdSemantics(unittest.TestCase):
    def test_official_code_is_canonical_and_synonym_keeps_same_lamas_id(self):
        raw = pd.DataFrame([
            {
                "region_code": "11", "city_code": "578", "city_name": "עיר בדיקה",
                "street_code": "101", "street_name": "רחוב ראשי",
                "street_name_status": "official", "official_code": "101",
            },
            {
                "region_code": "11", "city_code": "578", "city_name": "עיר בדיקה",
                "street_code": "9521", "street_name": "הרחוב הראשי",
                "street_name_status": "synonym of 101", "official_code": "101",
            },
            {
                "region_code": "21", "city_code": "730", "city_name": "עיר אחרת",
                "street_code": "101", "street_name": "רחוב ראשי",
                "street_name_status": "official", "official_code": "101",
            },
        ])
        result = _normalize_lamas_data(raw)
        self.assertEqual(result["LAMAS_id"].tolist(), ["101", "101", "101"])
        self.assertEqual(result["LAMAS_key"].tolist(), [
            "578:101", "578:101", "730:101",
        ])
        self.assertEqual(result["is_official"].tolist(), [True, False, True])
        self.assertEqual(result["id_source"].unique().tolist(), ["official_code"])
        # Same official_code in two cities must not collapse into one row.
        self.assertEqual(len(result), 3)

    def test_four_digit_official_code_is_not_discarded(self):
        raw = pd.DataFrame([{
            "city_code": "3000", "city_name": "עיר בדיקה",
            "street_code": "1457", "street_name": "רחוב ארוך",
            "street_name_status": "official", "official_code": "1457",
        }])
        result = _normalize_lamas_data(raw)
        self.assertEqual(result.iloc[0]["LAMAS_id"], "1457")
        self.assertEqual(result.iloc[0]["LAMAS_key"], "3000:1457")
        self.assertTrue(result.iloc[0]["is_official"])

    def test_missing_official_code_falls_back_for_official_row_only(self):
        raw = pd.DataFrame([
            {
                "city_code": "52", "city_name": "עיר בדיקה",
                "street_code": "5835", "street_name": "רחוב תקין",
                "street_name_status": "official", "official_code": "",
            },
            {
                "city_code": "52", "city_name": "עיר בדיקה",
                "street_code": "9521", "street_name": "שם נרדף ללא קוד",
                "street_name_status": "synonym of 5835", "official_code": "",
            },
        ])
        result = _normalize_lamas_data(raw)
        self.assertEqual(result["LAMAS_id"].tolist(), ["5835"])
        self.assertEqual(result.iloc[0]["id_source"], "street_code_fallback")

    def test_legacy_normalized_cache_gets_city_based_key(self):
        cached = pd.DataFrame([{
            "LAMAS_id": "101", "LAMAS_name": "רחוב ישן", "city": "אבו סנאן",
        }])
        result = _normalize_lamas_data(cached)
        self.assertEqual(result.iloc[0]["LAMAS_id"], "101")
        self.assertEqual(result.iloc[0]["LAMAS_key"], "אבוסנאנ:101")
        self.assertTrue(result.iloc[0]["is_official"])

    def test_blank_or_punctuation_only_names_are_dropped(self):
        raw = pd.DataFrame([
            {
                "city_code": "52", "city_name": "עיר בדיקה",
                "street_code": "9000", "street_name": "-",
                "street_name_status": "official", "official_code": "9000",
            },
            {
                "city_code": "52", "city_name": "עיר בדיקה",
                "street_code": "101", "street_name": "רחוב אמיתי",
                "street_name_status": "official", "official_code": "101",
            },
        ])
        result = _normalize_lamas_data(raw)
        self.assertEqual(result["LAMAS_name"].tolist(), ["רחוב אמיתי"])


if __name__ == "__main__":
    unittest.main()
