"""Normalization upgrades and core-name (prefix-stripped) fuzzy matching.

Covers the fixes derived from the batch-run log analysis:

1. Bare Hebrew abbreviation expansion ("סמ אורן" -> "סמטת אורן",
   "שכ נתיב האבות" -> "שכונת נתיב האבות") — the LAMAS style writes
   abbreviations without a geresh.
2. Hebrew/Arabic punctuation folding: geresh ׳ vs ASCII ', gershayim ״ vs
   ASCII ", the maqaf ־, niqqud and Arabic-Indic digits.
3. Core-name scoring in ``find_fuzzy_candidates``: street-type prefixes are
   stripped before a second fuzzy pass, with safeguards (exact match wins
   ties, cross-id core ties demote to NEEDS_AI, settlement pseudo-records
   never participate in the core boost).
"""

import unittest

import pandas as pd

from normalization import (
    find_fuzzy_candidates,
    normalize_street_name,
    strip_street_prefix,
)


class TestBareAbbreviations(unittest.TestCase):
    """Abbreviations written without a geresh — the common LAMAS form."""

    def test_sam_without_geresh_expands_to_samtat(self):
        # From the log: אלישמע LAMAS "סמ אורן"..."סמ תמר" stayed unmatched.
        self.assertEqual(normalize_street_name("סמ אורן"), "סמטת אורן")
        self.assertEqual(normalize_street_name("סמ חרוב"), "סמטת חרוב")

    def test_sam_with_geresh_and_period_still_expands(self):
        self.assertEqual(normalize_street_name("סמ' אורן"), "סמטת אורן")
        self.assertEqual(normalize_street_name("סמ. אורן"), "סמטת אורן")
        self.assertEqual(normalize_street_name("סמט' אורן"), "סמטת אורן")

    def test_shchuna_prefix_expands(self):
        # From the log: אלעזר LAMAS "שכ נתיב האבות" stayed unmatched.
        self.assertEqual(
            normalize_street_name("שכ נתיב האבות"), "שכונת נתיב האבות"
        )
        self.assertEqual(
            normalize_street_name("שכ' נתיב האבות"), "שכונת נתיב האבות"
        )
        self.assertEqual(
            normalize_street_name("שכ. נתיב האבות"), "שכונת נתיב האבות"
        )

    def test_rechov_and_kikar_bare_expansion(self):
        self.assertEqual(normalize_street_name("רח הרצל"), "רחוב הרצל")
        self.assertEqual(normalize_street_name("כי המדינה"), "כיכר המדינה")
        self.assertEqual(normalize_street_name("ככר המדינה"), "כיכר המדינה")
        self.assertEqual(normalize_street_name("פרופ' שור"), "פרופסור שור")
        self.assertEqual(normalize_street_name("א.ד.גורדון"), "אד גורדון")
        self.assertEqual(normalize_street_name("ש.י. עגנון"), "שי עגנון")

    def test_re_and_dar_are_not_expanded(self):
        # "ר" may mean רבי and "דר" may mean ד"ר — never force-expand them.
        self.assertEqual(
            normalize_street_name("ר' אליעזר בן יעקב"), "ר אליעזר בן יעקב"
        )
        self.assertEqual(normalize_street_name("דר הרצל"), "דר הרצל")

    def test_full_words_are_not_touched(self):
        self.assertEqual(normalize_street_name("רחובות העיר"), "רחובות העיר")
        self.assertEqual(normalize_street_name("שדרות רוטשילד"), "שדרות רוטשילד")
        self.assertEqual(normalize_street_name("סמטת אורן"), "סמטת אורן")


class TestUnicodeFolding(unittest.TestCase):
    """Both data sources must converge to one canonical spelling."""

    def test_hebrew_and_ascii_geresh_converge(self):
        # From the log: אסד/אעבלין OSM used ׳ while LAMAS uses '.
        self.assertEqual(normalize_street_name("אלג׳בל"), "אלגבל")
        self.assertEqual(normalize_street_name("אלג׳בל"), normalize_street_name("אלג'בל"))

    def test_hebrew_and_ascii_gershayim_converge(self):
        # From the log: אלעד LAMAS "הב\"ח".
        self.assertEqual(normalize_street_name("הב״ח"), "הבח")
        self.assertEqual(normalize_street_name("הב״ח"), normalize_street_name('הב"ח'))

    def test_rabbi_name_converges_across_quote_styles(self):
        # From the log: אלעד LAMAS "ר' אליעזר בן יעקב" unmatched.
        self.assertEqual(
            normalize_street_name("ר׳ אליעזר בן יעקב"),
            normalize_street_name("ר' אליעזר בן יעקב"),
        )

    def test_hebrew_maqaf_becomes_space(self):
        # From the log: אסד OSM "א־סלאם".
        self.assertEqual(normalize_street_name("א־סלאם"), "א סלאם")

    def test_niqqud_removed(self):
        self.assertEqual(
            normalize_street_name("רַחוב הַנֶּבִיאִים"), "רחוב הנביאים"
        )

    def test_arabic_indic_digits_fold(self):
        # From the log: אסיף OSM "١٠٠".
        self.assertEqual(normalize_street_name("١٠٠"), "100")

    def test_arabic_letters_fold(self):
        self.assertEqual(normalize_street_name("أبو النمر"), "ابو النمر")
        self.assertEqual(
            normalize_street_name("أبو النمر"), normalize_street_name("ابو النمر")
        )


class TestStripStreetPrefix(unittest.TestCase):
    def test_single_and_repeated_prefixes(self):
        # Stripping is repeated by design: every leading street-type word goes.
        self.assertEqual(strip_street_prefix("סמטת אורן"), "אורן")
        self.assertEqual(strip_street_prefix("משעול סייפן"), "סייפן")
        self.assertEqual(strip_street_prefix("דרך נתיב האבות"), "האבות")
        self.assertEqual(strip_street_prefix("שכונת נתיב האבות"), "האבות")
        self.assertEqual(strip_street_prefix("נתיב האבות"), "האבות")
        self.assertEqual(strip_street_prefix("הרצל"), "הרצל")
        self.assertEqual(strip_street_prefix(""), "")

    def test_pipeline_shares_the_same_strip_logic(self):
        # The AI gate in pipeline.py must strip exactly like the fuzzy stage.
        from pipeline import _core_wratio

        self.assertEqual(_core_wratio("סמ אורן", "אורן"), 100.0)
        self.assertEqual(_core_wratio("משעול סייפן", "סייפן"), 100.0)


def _run_match(osm_names, lamas_rows, city=None):
    osm_gdf = pd.DataFrame(
        {
            "osm_id": [f"osm-{i}" for i in range(len(osm_names))],
            "normalized_name": osm_names,
        }
    )
    lamas_df = pd.DataFrame(lamas_rows)
    if city:
        osm_gdf["city"] = city
        lamas_df["city"] = city
    return find_fuzzy_candidates(osm_gdf, lamas_df).set_index("osm_id")


class TestCoreNameMatching(unittest.TestCase):
    """Streets that differ only by their street-type word now match."""

    def test_plain_name_matches_alley_record(self):
        # From the log: אלישמע OSM "אורן"-like names vs LAMAS "סמ אורן".
        normalized = normalize_street_name("סמ אורן")  # -> "סמטת אורן"
        result = _run_match(
            ["אורן"],
            [
                {
                    "LAMAS_id": "101",
                    "LAMAS_name": "סמ אורן",
                    "normalized_name": normalized,
                }
            ],
        )
        row = result.iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["best_LAMAS_id"], "101")
        self.assertEqual(row["best_score"], 100.0)

    def test_mishol_matches_plain_name(self):
        # From the log: אלפי מנשה "משעול סייפן" vs "סייפן".
        result = _run_match(
            ["משעול סייפן"],
            [{"LAMAS_id": "101", "LAMAS_name": "סייפן", "normalized_name": "סייפן"}],
        )
        self.assertEqual(result.iloc[0]["status"], "CONFIDENT")

    def test_neighborhood_record_matches_street(self):
        # From the log: אלעזר "דרך נתיב האבות" vs "שכ נתיב האבות".
        result = _run_match(
            ["דרך נתיב האבות"],
            [
                {
                    "LAMAS_id": "101",
                    "LAMAS_name": "שכ נתיב האבות",
                    "normalized_name": "שכונת נתיב האבות",
                }
            ],
        )
        self.assertEqual(result.iloc[0]["status"], "CONFIDENT")

    def test_exact_match_preferred_over_core_match(self):
        # "ירושלים" must go to the exact "ירושלים" record, not to
        # "שדרות ירושלים" whose core name is also "ירושלים".
        result = _run_match(
            ["ירושלים"],
            [
                {"LAMAS_id": "10", "LAMAS_name": "שדרות ירושלים", "normalized_name": "שדרות ירושלים"},
                {"LAMAS_id": "20", "LAMAS_name": "ירושלים", "normalized_name": "ירושלים"},
            ],
        )
        row = result.iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["best_LAMAS_id"], "20")
        self.assertEqual(row["best_score"], 100.0)

    def test_core_tie_across_ids_is_demoted_to_needs_ai(self):
        # "הרצל" core-matches both "שדרות הרצל" and "רחוב הרצל" — genuinely
        # ambiguous, so it must not be approved without AI/review.
        result = _run_match(
            ["הרצל"],
            [
                {"LAMAS_id": "10", "LAMAS_name": "שדרות הרצל", "normalized_name": "שדרות הרצל"},
                {"LAMAS_id": "20", "LAMAS_name": "רחוב הרצל", "normalized_name": "רחוב הרצל"},
            ],
        )
        row = result.iloc[0]
        self.assertEqual(row["status"], "NEEDS_AI")

    def test_core_tie_with_a_fully_confident_candidate_is_resolved(self):
        # The exact "נתיב האבות" record is fully confident; the stripped tie
        # with "דרך נתיב האבות" must not demote it.
        result = _run_match(
            ["נתיב האבות"],
            [
                {"LAMAS_id": "10", "LAMAS_name": "דרך נתיב האבות", "normalized_name": "דרך נתיב האבות"},
                {"LAMAS_id": "20", "LAMAS_name": "נתיב האבות", "normalized_name": "נתיב האבות"},
            ],
        )
        row = result.iloc[0]
        self.assertEqual(row["status"], "CONFIDENT")
        self.assertEqual(row["best_LAMAS_id"], "20")

    def test_word_reorder_honorific_ktiv_semicolon_and_feature_guard(self):
        # Exact reversed word order, honorific+ktiv differences, and semicolon dual
        # names match confidently, while squares/parks do NOT match plain streets.
        osm = pd.DataFrame(
            {
                "osm_id": ["1", "2", "3", "4", "5", "6", "7"],
                "osm_name": [
                    "דוד רמז",
                    "הרב גד מכלוף",
                    "הרב חיי טייב",
                    "שדרות ויצמן;ויצמן",
                    "כיכר יוסף גרינוולד",
                    "כיכר מארק שאגאל",
                    "פארק גילה",
                ],
                "normalized_name": [
                    normalize_street_name(n)
                    for n in [
                        "דוד רמז",
                        "הרב גד מכלוף",
                        "הרב חיי טייב",
                        "שדרות ויצמן;ויצמן",
                        "כיכר יוסף גרינוולד",
                        "כיכר מארק שאגאל",
                        "פארק גילה",
                    ]
                ],
                "city": ["נתניה"] * 7,
            }
        )
        lamas = pd.DataFrame(
            [
                {"LAMAS_id": "10", "LAMAS_name": "רמז דוד", "normalized_name": normalize_street_name("רמז דוד")},
                {"LAMAS_id": "20", "LAMAS_name": "מכלוף גד", "normalized_name": normalize_street_name("מכלוף גד")},
                {"LAMAS_id": "30", "LAMAS_name": "הרב טייב חי", "normalized_name": normalize_street_name("הרב טייב חי")},
                {"LAMAS_id": "40", "LAMAS_name": "שד ויצמן", "normalized_name": normalize_street_name("שד ויצמן")},
                {"LAMAS_id": "50", "LAMAS_name": "יוסי גרינוולד", "normalized_name": normalize_street_name("יוסי גרינוולד")},
                {"LAMAS_id": "60", "LAMAS_name": "מארק שאגל", "normalized_name": normalize_street_name("מארק שאגל")},
                {"LAMAS_id": "70", "LAMAS_name": "שכון גילה", "normalized_name": normalize_street_name("שכון גילה")},
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"])[:4], ["CONFIDENT"] * 4)
        self.assertEqual(list(res["best_LAMAS_id"])[:4], ["10", "20", "30", "40"])
        self.assertEqual(list(res["status"])[4:], ["NEEDS_AI", "NEEDS_AI", "NEEDS_AI"])


class TestSettlementNameGuard(unittest.TestCase):
    """Settlement pseudo-records must never be absorbed via core matching."""

    def test_road_named_after_settlement_not_core_matched(self):
        # From the log: small settlements keep a single "אלעזר"-style record;
        # a road "דרך אלעזר" must not confidently match it via core stripping.
        result = _run_match(
            ["דרך אלעזר"],
            [{"LAMAS_id": "10", "LAMAS_name": "אלעזר", "normalized_name": "אלעזר"}],
            city="אלעזר",
        )
        self.assertEqual(result.iloc[0]["status"], "NEEDS_AI")

    def test_exact_settlement_name_match_still_confident(self):
        # Documents the pre-existing boundary: a full-name exact match keeps
        # working exactly as before the core scoring was introduced.
        result = _run_match(
            ["אלעזר"],
            [{"LAMAS_id": "10", "LAMAS_name": "אלעזר", "normalized_name": "אלעזר"}],
            city="אלעזר",
        )
        self.assertEqual(result.iloc[0]["status"], "CONFIDENT")

    def test_guard_is_applied_per_city(self):
        # In a different city there is no settlement-name conflict at all.
        result = _run_match(
            ["דרך אלעזר"],
            [{"LAMAS_id": "10", "LAMAS_name": "אלעזר", "normalized_name": "אלעזר"}],
            city="רחובות",
        )
        self.assertEqual(result.iloc[0]["status"], "CONFIDENT")


class TestInfrastructureStaysUnmatched(unittest.TestCase):
    def test_road_variants_stay_below_threshold(self):
        # From the log: אלקנה "כביש ביטחון" vs "כביש ביטחון אלקנה" — partial
        # infrastructure names must not become confident.
        result = _run_match(
            ["כביש ביטחון"],
            [
                {
                    "LAMAS_id": "10",
                    "LAMAS_name": "כביש ביטחון אלקנה",
                    "normalized_name": "כביש ביטחון אלקנה",
                }
            ],
        )
        self.assertEqual(result.iloc[0]["status"], "NEEDS_AI")
        self.assertLess(result.iloc[0]["best_score"], 90.0)


if __name__ == "__main__":
    unittest.main()
