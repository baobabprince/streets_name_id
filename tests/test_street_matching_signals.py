"""Synthetic street-pair regressions; these are not a labelled accuracy corpus."""
import unittest

import pandas as pd

from normalization import (
    find_fuzzy_candidates,
    hebrew_phonetic_key,
    normalize_street_name,
)
from pipeline import _finalize_mappings


def match(osm_names, names_and_ids, threshold=90, city=None):
    osm = pd.DataFrame({
        "osm_id": [str(i) for i in range(len(osm_names))],
        "normalized_name": [normalize_street_name(n) for n in osm_names],
    })
    if city:
        osm["city"] = city
    lamas = pd.DataFrame(names_and_ids, columns=["LAMAS_name", "LAMAS_id"])
    lamas["normalized_name"] = lamas["LAMAS_name"].map(normalize_street_name)
    return find_fuzzy_candidates(osm, lamas, confident_threshold=threshold)


class TestHebrewPhoneticKey(unittest.TestCase):
    def test_spelling_variants(self):
        for left, right in [
            ("תקוה", "תקווה"), ("הרצל", "הרציל"),
            ("טשרניחובסקי", "תשרניחובסקי"), ("אלימלך", "אלימלכ"),
        ]:
            with self.subTest(left=left, right=right):
                self.assertTrue(hebrew_phonetic_key(left))
                self.assertEqual(hebrew_phonetic_key(left), hebrew_phonetic_key(right))

    def test_prefix_unicode_and_order(self):
        self.assertEqual(
            hebrew_phonetic_key("רח׳ מֹשֶׁה מונטיפיורי"),
            hebrew_phonetic_key("מונטיפיורי משה"),
        )

    def test_unsupported_or_too_short_names_are_not_keys(self):
        for name in (None, "", "123", "רחוב 10", "בן גוריון", "ים", "Herzl", "شارع النور", "הרצל Street"):
            with self.subTest(name=name):
                self.assertEqual(hebrew_phonetic_key(name), "")

    def test_no_soundex_truncation(self):
        self.assertNotEqual(hebrew_phonetic_key("אברהם"), hebrew_phonetic_key("אברהמי" + "צ"))


class TestTokenOrder(unittest.TestCase):
    def test_complete_reordered_name_is_confident(self):
        row = match(["משה מונטיפיורי"], [("מונטיפיורי משה", "1")]).iloc[0]
        self.assertEqual(row["best_score"], 100)
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["match_method"], "token_order")

    def test_prefix_and_order(self):
        row = match(["רחוב דוד בן גוריון"], [("בן גוריון דוד", "1")]).iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["match_method"], "token_order")

    def test_extra_or_repeated_word_is_not_complete_equality(self):
        for name in ("משה", "משה משה מונטיפיורי", "משה מונטיפיורי מזרח"):
            row = match([name], [("מונטיפיורי משה", "1")]).iloc[0]
            self.assertLess(row["best_score"], 100)
            self.assertNotEqual(row["match_method"], "token_order")

    def test_exact_beats_reordered_even_if_listed_second(self):
        row = match(["משה מונטיפיורי"], [
            ("מונטיפיורי משה", "1"), ("משה מונטיפיורי", "2"),
        ]).iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["best_LAMAS_id"], "2")
        self.assertEqual(row["match_method"], "exact")

    def test_competing_core_order_matches_require_review(self):
        row = match(["משה מונטיפיורי"], [
            ("שדרות מונטיפיורי משה", "1"), ("דרך מונטיפיורי משה", "2"),
        ]).iloc[0]
        self.assertEqual(row["status"], "NEEDS_AI")
        self.assertEqual(row["match_review_reason"], "ambiguous_candidates")

    def test_same_id_aliases_are_not_ambiguous(self):
        row = match(["משה מונטיפיורי"], [
            ("שדרות מונטיפיורי משה", "1"), ("דרך מונטיפיורי משה", "1"),
        ]).iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")

    def test_duplicate_exact_label_across_ids_requires_review(self):
        row = match(["הרצל"], [("הרצל", "1"), ("הרצל", "2")]).iloc[0]
        self.assertEqual(row["status"], "NEEDS_AI")
        self.assertEqual(row["match_review_reason"], "ambiguous_candidates")

    def test_settlement_guard_not_bypassed_by_token_order(self):
        row = match(["דרך תקווה פתח"], [("פתח תקווה", "1")], city="פתח תקווה").iloc[0]
        self.assertEqual(row["status"], "NEEDS_AI")
        self.assertNotEqual(row["match_method"], "token_order")


class TestPhoneticSuggestions(unittest.TestCase):
    def test_phonetic_only_stays_below_custom_thresholds(self):
        for threshold in (85, 90, 95):
            row = match(["תיקווה"], [("תקוה", "1")], threshold=threshold).iloc[0]
            self.assertEqual(row["best_LAMAS_id"], "1")
            self.assertEqual(row["match_method"], "hebrew_phonetic")
            self.assertEqual(row["match_review_reason"], "phonetic_only")
            self.assertEqual(row["status"], "NEEDS_AI")
            self.assertLess(row["best_score"], threshold)

    def test_exact_still_wins_over_phonetic_collision(self):
        row = match(["תיקווה"], [("תקוה", "1"), ("תיקווה", "2")]).iloc[0]
        self.assertEqual(row["best_LAMAS_id"], "2")
        self.assertEqual(row["status"], "CONFIDENT")

    def test_phonetic_can_improve_candidate_ranking(self):
        row = match(["תיקווה"], [("תיקומה", "1"), ("תקוה", "2")]).iloc[0]
        self.assertEqual(row["best_LAMAS_id"], "2")
        self.assertEqual(row["match_method"], "hebrew_phonetic")

    def test_short_unrelated_names_not_boosted(self):
        row = match(["דן"], [("גן", "1")]).iloc[0]
        self.assertLess(row["best_score"], 89)
        self.assertNotEqual(row["match_method"], "hebrew_phonetic")

    def test_no_phonetic_boost_for_settlement_pseudorecord(self):
        row = match(["דרך תיקווה"], [("תקוה", "1")], city="תקוה").iloc[0]
        self.assertNotEqual(row["match_method"], "hebrew_phonetic")
        self.assertEqual(row["status"], "NEEDS_AI")

    def test_phonetic_suggestion_does_not_become_final_mapping(self):
        candidates = match(["תיקווה"], [("תקוה", "1")])
        _, final = _finalize_mappings(candidates, pd.DataFrame(columns=[
            "osm_id", "ai_suggested_LAMAS_id", "ai_confidence", "ai_reasoning", "ai_method",
        ]))
        self.assertTrue(final.empty)


class TestNumberGuardsAndShape(unittest.TestCase):
    def test_digit_difference_in_long_name_cannot_be_approved(self):
        row = match(["שדרות הלוחמים הגדוד העברי 12"], [
            ("שדרות הלוחמים הגדוד העברי 13", "1"),
        ]).iloc[0]
        self.assertEqual(row["status"], "NEEDS_AI")
        self.assertIsNone(row["best_LAMAS_id"])
        self.assertEqual(row["match_review_reason"], "number_conflict")

    def test_number_must_not_disappear(self):
        for left, right in (("רחוב 10", "רחוב"), ("הגדוד העברי", "הגדוד העברי 10")):
            row = match([left], [(right, "1")]).iloc[0]
            self.assertEqual(row["status"], "NEEDS_AI")
            self.assertIsNone(row["best_LAMAS_id"])

    def test_valid_number_candidate_beats_conflicting_exact_looking_one(self):
        row = match(["הגדוד העברי 12"], [
            ("הגדוד העברי 13", "1"), ("הגדוד 12", "2"),
        ]).iloc[0]
        self.assertEqual(row["best_LAMAS_id"], "2")

    def test_arabic_digits_still_match_after_normalization(self):
        row = match(["רחוב ١٢"], [("רחוב 12", "1")]).iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["match_method"], "exact")

    def test_mixed_valid_and_zero_scoring_candidates_do_not_choose_conflict(self):
        row = match(["אאא 1"], [("אאא 2", "1"), ("zzz 1", "2")]).iloc[0]
        self.assertEqual(row["best_LAMAS_id"], "2")

    def test_repeated_segments_keep_same_decision_and_missing_has_schema(self):
        rows = match(["תיקווה", None, "תיקווה"], [("תקוה", "1")])
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows.iloc[0]["best_LAMAS_id"], rows.iloc[2]["best_LAMAS_id"])
        self.assertEqual(rows.iloc[1]["status"], "MISSING")
        self.assertEqual(rows.iloc[1]["match_method"], "missing")

    def test_empty_inputs_keep_explanation_columns(self):
        for rows in (match([], [("הרצל", "1")]), match(["הרצל"], [])):
            self.assertIn("match_method", rows.columns)
            self.assertIn("match_review_reason", rows.columns)


if __name__ == "__main__":
    unittest.main()
