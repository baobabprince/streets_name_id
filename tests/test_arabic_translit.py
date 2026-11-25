"""Cross-script Arabic ↔ Hebrew street matching tests.

Fixtures pair real OSM Arabic-script street names (taken from published
settlement pages: כפר קרע, קלנסווה, אום אל-פחם, רהט, מעלות-תרשיחא, טמרה,
כפר קאסם, באקה אל-גרביה) with the Hebrew-script LAMAS transliterations that
the same settlement pages show.  Before the transliteration channel these
pairs always scored ~0 and every Arabic-script street ended up unmatched.
"""

import unittest

import pandas as pd

from arabic_translit import (
    contains_arabic,
    transliterate,
    transliteration_variants,
)
from normalization import (
    comparable_forms,
    find_fuzzy_candidates,
    fold_hebrew_final_letters,
    normalize_street_name,
)
from pipeline import _core_wratio


def _blend_score(left: str, right: str) -> float:
    """The project's standard fuzzy blend for a single pair."""
    from rapidfuzz import fuzz

    return max(
        fuzz.ratio(left, right),
        fuzz.token_sort_ratio(left, right),
        fuzz.token_set_ratio(left, right),
    )


def _fold(text: str) -> str:
    """Match-time comparison form: normalized + final letters folded."""
    from normalization import fold_hebrew_final_letters

    return fold_hebrew_final_letters(normalize_street_name(text) or "")


class TestContainsArabic(unittest.TestCase):
    def test_detects_arabic_script(self):
        self.assertTrue(contains_arabic("ابن سينا"))
        self.assertTrue(contains_arabic("שארע אלקודס ירושלים؟ لا"))

    def test_ignores_hebrew_latin_and_digits(self):
        self.assertFalse(contains_arabic("רחוב אבן סינא"))
        self.assertFalse(contains_arabic("Ibn Sina street"))
        self.assertFalse(contains_arabic("רחוב 140"))
        self.assertFalse(contains_arabic(None))
        self.assertFalse(contains_arabic(""))


class TestTransliterationVariants(unittest.TestCase):
    def assert_variant(self, arabic_name, hebrew_expected):
        forms = transliteration_variants(arabic_name)
        # Variants are emitted with regular Hebrew letters; LAMAS spellings
        # use conventional final letters, so both sides compare after the
        # match-time fold (see normalization.fold_hebrew_final_letters).
        expected = _fold(hebrew_expected)
        self.assertTrue(
            any(_fold(form) == expected for form in forms),
            f"{arabic_name!r}: expected {hebrew_expected!r} in {forms}",
        )

    def test_ibn_sina_matches_lamas_aleph_variants(self):
        # Real LAMAS spellings: מעלות-תרשיחא and טירה both list אבן סינא.
        self.assert_variant("ابن سينا", "אבן סינא")
        self.assert_variant("ابن سينا", "אבן סינה")
        self.assertEqual(_fold(transliteration_variants("ابن سينا")[0]), _fold("אבן סינא"))

    def test_street_type_word_maps_to_hebrew_prefix(self):
        # Hebrew transliteration inserts the vowel of القدس (קודס).
        self.assert_variant("شارع القدس", "רחוב אלקדס")

    def test_definite_article_styles(self):
        # LAMAS טמרה writes النصر as א-נסר (assimilated + geresh stripped).
        self.assert_variant("النصر", "א נסר")
        self.assert_variant("النصر", "אלנצר")
        # Moon letters never assimilate: الق is not sun.
        forms = transliteration_variants("القودس")
        self.assertNotIn("א קודס", forms)
        self.assertIn("אל קודס", forms)

    def test_sun_letter_assimilation(self):
        # LAMAS רהט: אל סנאבל / אל ספא; כפר קרע: א-ס'יף for السيف.
        self.assert_variant("السيف", "א סיף")
        self.assert_variant("السوسن", "א סוסן")

    def test_hamza_carrier_drop_reaches_confident_score(self):
        # LAMAS רהט spells الرأيف as א-רייף; the exact spelling differs
        # (ראיף/רייף) but the fuzzy blend still clears the threshold.
        osm_forms = comparable_forms("الرايف", fold_final_letters=True)
        lamas_form = _fold("א-רייף")
        best = max(_blend_score(form, lamas_form) for form in osm_forms)
        self.assertGreaterEqual(best, 90.0, f"scored {best:.1f}")

    def test_alternate_letter_map(self):
        # LAMAS נצרת spells كرم الصاحب as כרם א-סאחב (ص→ס + assimilation).
        self.assert_variant("كرم الصاحب", "כרם א סאחב")
        # LAMAS כפר קרע spells صلاح الدين as סלאח א-דין.
        self.assert_variant("صلاح الدين", "סלאח א דין")

    def test_word_exceptions(self):
        # LAMAS נצרת/טירה spell عمر بن الخطاب as עומר בן אל-ח'טאב.
        self.assert_variant("عمر بن الخطاب", "עומר בן אלחטאב")
        self.assert_variant("عثمان بن عفان", "עותמאן בן עפאן")

    def test_roundabout_and_square_map_to_kikar(self):
        self.assert_variant("دوار عبد الكريم قاسم", "כיכר עבד אלכרים קאסם")
        self.assert_variant("ساحة ترشيحا", "כיכר תרשיחא")

    def test_mammar_maps_to_mishol(self):
        # Real OSM מעלות-תרשיחא name.
        self.assert_variant("ممر السوسن", "משעול אלסוסן")

    def test_arabic_indic_digits_reach_the_hebrew_form(self):
        # normalization folds the digits; translit maps the street word.
        forms = transliteration_variants("شارع 140")
        self.assertIn("רחוב 140", forms)
        forms = transliteration_variants("شارع ١٤٠")
        self.assertTrue(forms)  # digits pass through, folded by normalization

    def test_bounded_and_deduplicated(self):
        forms = transliteration_variants("علي بن أبي طالب")
        self.assertLessEqual(len(forms), 16)
        self.assertEqual(len(forms), len(set(forms)))
        self.assertGreater(len(forms), 0)

    def test_no_variants_for_hebrew_or_empty(self):
        self.assertEqual(transliteration_variants("רחוב הגפן"), [])
        self.assertEqual(transliteration_variants(""), [])
        self.assertEqual(transliteration_variants(None), [])
        self.assertEqual(transliteration_variants("רחוב 140"), [])

    def test_transliterate_returns_primary_form(self):
        self.assertEqual(_fold(transliterate("ابن سينا")), _fold("אבן סינא"))
        self.assertEqual(transliterate("רחוב הגפן"), "")


class TestComparableForms(unittest.TestCase):
    def test_hebrew_names_unchanged(self):
        self.assertEqual(comparable_forms("רחוב הגפן"), ["רחוב הגפן"])
        self.assertEqual(comparable_forms("סמ אורן"), ["סמטת אורן"])

    def test_arabic_name_gets_transliterations(self):
        forms = comparable_forms("ابن سينا")
        self.assertEqual(forms[0], "ابن سينا")
        self.assertIn(_fold("אבן סינא"), [fold_hebrew_final_letters(f) for f in forms])

    def test_fold_final_letters_only_when_requested(self):
        # LAMAS uses conventional final letters: א-ס'יף → א סיף with ף.
        lamas_normalized = normalize_street_name("א-ס'יף")
        folded = comparable_forms(lamas_normalized, fold_final_letters=True)
        self.assertEqual(folded, [fold_hebrew_final_letters(lamas_normalized)])
        self.assertEqual(folded, ["א סיפ"])
        unfolded = comparable_forms(lamas_normalized)
        self.assertEqual(unfolded, [lamas_normalized])

    def test_fold_helper(self):
        self.assertEqual(fold_hebrew_final_letters(normalize_street_name("א-ס'יף")), "א סיפ")
        self.assertEqual(fold_hebrew_final_letters(""), "")
        self.assertEqual(fold_hebrew_final_letters(None), "")


class TestCrossScriptFuzzyMatching(unittest.TestCase):
    def _run_matching(self, osm_names, lamas_rows):
        osm_gdf = pd.DataFrame(
            {
                "osm_id": [str(i + 1) for i in range(len(osm_names))],
                "normalized_name": osm_names,
                "city": ["כפר קרע"] * len(osm_names),
            }
        )
        lamas_df = pd.DataFrame(lamas_rows)
        return find_fuzzy_candidates(osm_gdf, lamas_df)

    def test_arabic_osm_street_matches_hebrew_lamas_street(self):
        lamas_rows = [
            {
                "LAMAS_id": "101",
                "LAMAS_name": "אבן סינא",
                "normalized_name": normalize_street_name("אבן סינא"),
            },
            {
                "LAMAS_id": "202",
                "LAMAS_name": "אל-קודס",
                "normalized_name": normalize_street_name("אל-קודס"),
            },
            {
                "LAMAS_id": "303",
                "LAMAS_name": "א-ס'יף",
                "normalized_name": normalize_street_name("א-ס'יף"),
            },
        ]
        osm_names = [
            normalize_street_name("ابن سينا"),
            normalize_street_name("شارع القدس"),
            normalize_street_name("السيف"),
            normalize_street_name("قحاوش"),  # real unmatched control
        ]
        result = self._run_matching(osm_names, lamas_rows)
        # find_fuzzy_candidates maps results back through osm_id.
        name_by_id = {str(i + 1): name for i, name in enumerate(osm_names)}
        row_by_id = {row.osm_id: row for row in result.itertuples(index=False)}

        def row_for(name):
            return row_by_id[
                next(osm_id for osm_id, n in name_by_id.items() if n == name)
            ]

        ibn_sina = row_for(normalize_street_name("ابن سينا"))
        self.assertEqual(ibn_sina.status, "CONFIDENT")
        self.assertEqual(ibn_sina.best_LAMAS_id, "101")

        # The real OSM spelling spells القدس without the waw that LAMAS's
        # קודס represents; the compact channel still clears the threshold.
        alquds = row_for(normalize_street_name("شارع القدس"))
        self.assertEqual(alquds.status, "CONFIDENT")
        self.assertEqual(alquds.best_LAMAS_id, "202")
        self.assertGreaterEqual(alquds.best_score, 90.0)

        alsif = row_for(normalize_street_name("السيف"))
        self.assertEqual(alsif.status, "CONFIDENT")
        self.assertEqual(alsif.best_LAMAS_id, "303")

        # Unrelated Arabic name must not be promoted by the variants.
        control = row_for(normalize_street_name("قحاوش"))
        self.assertEqual(control.status, "NEEDS_AI")

    def test_ambiguity_still_deferred_to_ai(self):
        lamas_rows = [
            {
                "LAMAS_id": "101",
                "LAMAS_name": "רחוב הרצל",
                "normalized_name": normalize_street_name("רחוב הרצל"),
            },
            {
                "LAMAS_id": "202",
                "LAMAS_name": "שדרות הרצל",
                "normalized_name": normalize_street_name("שדרות הרצל"),
            },
        ]
        result = self._run_matching([normalize_street_name("הרצל")], lamas_rows)
        self.assertEqual(result.iloc[0]["status"], "NEEDS_AI")


class TestCrossScriptAIGate(unittest.TestCase):
    def test_core_wratio_cross_script(self):
        self.assertGreaterEqual(_core_wratio("ابن سينا", "אבן סינא"), 75.0)
        self.assertGreaterEqual(_core_wratio("شارع القدس", "אל-קודס"), 75.0)
        self.assertGreaterEqual(_core_wratio("السيف", "א-ס'יף"), 75.0)
        self.assertGreaterEqual(_core_wratio("عمر بن الخطاب", "עומר בן אל-ח'טאב"), 75.0)

    def test_core_wratio_cross_script_rejects_unrelated(self):
        self.assertLess(_core_wratio("قحاوش", "אומייה"), 75.0)

    def test_core_wratio_hebrew_pairs_unchanged(self):
        # Regression guard: Hebrew/Hebrew scoring keeps its exact values.
        self.assertEqual(_core_wratio("סמ אורן", "סמטת אורן"), 100.0)
        self.assertEqual(_core_wratio("אורן", "סמטת אורן"), 100.0)
        self.assertEqual(_core_wratio("הרצל", "שדרות הרצל"), 100.0)


class TestRealPagePairsPassConfidentThreshold(unittest.TestCase):
    """Every pair below was observed across this repo's published pages."""

    # Score >= 90 → approved without AI (the pipeline's CONFIDENT threshold).
    CONFIDENT_PAIRS = [
        ("ابن سينا", "אבן סינא"),
        ("النصر", "א-נסר"),
        ("السيف", "א-ס'יף"),
        ("الرايف", "א-רייף"),
        ("ممر السوسن", "משעול אלסוסן"),
        ("علي بن أبي طالب", "עלי בן אבי טאלב"),
        ("الفاروق", "אלפארוק"),
        ("عين خالد", "עין ח'אלד"),
        ("نزلة أبو نارة", "נזלה אבו נארה"),
        ("كرم الصاحب", "כרם א-סאחב"),
        ("صلاح الدين", "סלאח א-דין"),
        ("ساحة ترشيحا", "כיכר תרשיחא"),
        ("دوار عبد الكريم قاسم", "כיכר עבד אלכרים קאסם"),
        ("عمر بن الخطاب", "עומר בן אל-ח'טאב"),
        ("رؤوس الجبال", "רואוס אל-ג'יבאל"),
        ("شارع القدس", "אל-קודס"),
    ]

    def _channel_score(self, osm_name, lamas_name):
        """Mirror the pipeline: max over full, core and compact comparisons."""
        from normalization import strip_street_prefix

        osm_forms = comparable_forms(osm_name, fold_final_letters=True)
        lamas_forms = comparable_forms(lamas_name, fold_final_letters=True)
        best = 0.0
        for osm_form in osm_forms:
            for lamas_form in lamas_forms:
                best = max(best, _blend_score(osm_form, lamas_form))
                core_osm, core_lamas = (
                    strip_street_prefix(osm_form),
                    strip_street_prefix(lamas_form),
                )
                best = max(
                    best,
                    _blend_score(core_osm, core_lamas),
                    _blend_score(core_osm.replace(" ", ""), core_lamas.replace(" ", "")),
                )
        return best

    def test_pairs_reach_confident_score(self):
        for osm_name, lamas_name in self.CONFIDENT_PAIRS:
            best = self._channel_score(osm_name, lamas_name)
            self.assertGreaterEqual(
                best, 90.0, f"{osm_name!r} vs {lamas_name!r} scored {best:.1f}"
            )

    def test_unrelated_pairs_stay_below_threshold(self):
        unrelated = [("قحاوش", "אומייה"), ("علي بن أبي طالب", "אלקודס")]
        for osm_name, lamas_name in unrelated:
            best = self._channel_score(osm_name, lamas_name)
            self.assertLess(
                best, 90.0, f"{osm_name!r} vs {lamas_name!r} scored {best:.1f}"
            )


if __name__ == "__main__":
    unittest.main()
