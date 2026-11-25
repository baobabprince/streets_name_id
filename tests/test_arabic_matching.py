import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

import pandas as pd

from arabic_matching import (
    arabic_hebrew_skeleton,
    arabic_long_vowels_compatible,
    extract_embedded_street_number,
    has_arabic,
    hebrew_arabic_article_form,
    is_arab_style_lamas_city,
    latin_arabic_to_hebrew_skeleton,
    strip_arabic_street_prefix,
    transliterate_arabic_to_hebrew,
)
from laya_ai_resolver import LayaAIResolver
from normalization import (
    find_fuzzy_candidates,
    normalize_street_name,
    strip_street_prefix,
)
from pipeline import _core_wratio, _finalize_mappings


def _make_osm(names: list[str], city: str = "סח'נין") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "osm_id": [f"way/{i}" for i in range(1, len(names) + 1)],
            "osm_name": names,
            "normalized_name": [normalize_street_name(n) for n in names],
            "city": [city] * len(names),
        }
    )


def _make_lamas(rows: list[tuple[str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "LAMAS_id": [lid for lid, _ in rows],
            "LAMAS_name": [name for _, name in rows],
            "normalized_name": [normalize_street_name(name) for _, name in rows],
        }
    )


class TestArabicMatchingPrimitives(unittest.TestCase):
    def test_strip_arabic_street_prefix(self):
        self.assertEqual(strip_arabic_street_prefix("شارع صلاح الدين"), "صلاح الدين")
        self.assertEqual(strip_arabic_street_prefix("طريق السلام"), "السلام")
        self.assertEqual(strip_arabic_street_prefix("دوار ابن رشد"), "ابن رشد")
        self.assertEqual(strip_arabic_street_prefix("ممر السوسن"), "السوسن")
        self.assertEqual(strip_arabic_street_prefix("ساحة ترشيحا"), "ترشيحا")
        self.assertEqual(strip_arabic_street_prefix("شارع رقم 1108"), "1108")
        # Single-word name is never stripped down to empty
        self.assertEqual(strip_arabic_street_prefix("شارع"), "شارع")
        # Unified strip_street_prefix handles Arabic prefixes too
        self.assertEqual(strip_street_prefix("شارع 1108"), "1108")

    def test_transliterate_arabic_to_hebrew_handles_idafa_and_diacritics(self):
        self.assertEqual(transliterate_arabic_to_hebrew("شارع خلة بلوط"), "חלת בלוט")
        self.assertEqual(transliterate_arabic_to_hebrew("شارع خلة مشعل"), "חלת משעל")
        self.assertEqual(transliterate_arabic_to_hebrew("حائط البُراق"), "חאיט אלבראק")
        self.assertEqual(transliterate_arabic_to_hebrew("شارع ١١٠٨"), "1108")
        self.assertEqual(transliterate_arabic_to_hebrew("شارع صلاح الدين"), "סלאח אלדינ")

    def test_semitic_skeleton_unifies_variants(self):
        self.assertEqual(
            arabic_hebrew_skeleton(transliterate_arabic_to_hebrew("شارع صلاح الدين")),
            arabic_hebrew_skeleton("צלאח אלדין"),
        )
        self.assertEqual(
            arabic_hebrew_skeleton(transliterate_arabic_to_hebrew("عثمان بن عفان")),
            arabic_hebrew_skeleton("עותמאן בן עפאן"),
        )
        self.assertEqual(
            arabic_hebrew_skeleton(transliterate_arabic_to_hebrew("الشيخ أمين طريف")),
            arabic_hebrew_skeleton("א שייח אמין טריף"),
        )

    def test_arabic_long_vowels_compatible_blocks_short_vowel_swap(self):
        # نومن (נומנ) must NOT match ניימן (ניימנ)
        self.assertFalse(arabic_long_vowels_compatible("נומנ", "ניימן"))
        # Arabic omits short vowels: رشد -> רושד, المتنبي -> אלמותנבי remain valid
        self.assertTrue(arabic_long_vowels_compatible("רשד", "רושד"))
        self.assertTrue(arabic_long_vowels_compatible("אלמתנבי", "אלמותנבי"))

    def test_extract_embedded_street_number(self):
        self.assertEqual(extract_embedded_street_number("المدينة القديمة - 6171"), "6171")
        self.assertEqual(extract_embedded_street_number("6152 (سوق)"), "6152")
        self.assertEqual(extract_embedded_street_number("אל עומאל אל ערב (5017)"), "5017")
        # Ignore highway routes, 2-digit numbers, and multi-number strings
        self.assertIsNone(extract_embedded_street_number("כביש 443"))
        self.assertIsNone(extract_embedded_street_number("Route 65"))
        self.assertIsNone(extract_embedded_street_number("גדוד 21"))
        self.assertIsNone(extract_embedded_street_number("רחוב 100 ו 200"))

    def test_latin_arabic_to_hebrew_skeleton(self):
        self.assertEqual(
            latin_arabic_to_hebrew_skeleton("Abu Bakr al-Siddiq"),
            arabic_hebrew_skeleton("אבו בכר א סדיק"),
        )
        self.assertEqual(
            latin_arabic_to_hebrew_skeleton("al-Khamis"),
            arabic_hebrew_skeleton("אל חמיס"),
        )
        # Non-Arabic English labels return empty
        self.assertEqual(latin_arabic_to_hebrew_skeleton("Under Pass"), "")
        self.assertEqual(latin_arabic_to_hebrew_skeleton("Main Street"), "")

    def test_is_arab_style_lamas_city_separates_arab_and_mixed_cities(self):
        arab_city = ["אלגליל", "אלחרוב", "אל-סלאם", "אבו בכר", "חטין"]
        mixed_city = ["הרצל", "זבוטינסקי", "ביאליק", "בן גוריון", "אלתקווא", "התקוה", "דרך חברון"]
        self.assertTrue(is_arab_style_lamas_city(arab_city))
        self.assertFalse(is_arab_style_lamas_city(mixed_city))


class TestArabicFuzzyMatching(unittest.TestCase):
    def test_arabic_script_matches_hebrew_lamas_streets(self):
        osm = _make_osm(
            [
                "شارع صلاح الدين",
                "دوار ابن رشد",
                "شارع خلة بلوط",
                "الشيخ أمين طريف",
                "ظاهر عمر الزيداني",
                "شارع 1108",
            ],
            city="סח'נין",
        )
        lamas = _make_lamas(
            [
                ("101", "צלאח אלדין"),
                ("102", "אבן רושד"),
                ("103", "ח'לת בלוט"),
                ("104", "א-שייח' אמין טריף"),
                ("105", "זאהר עומר א-זידאני"),
                ("106", "רח 1108"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"]), ["CONFIDENT"] * 6)
        self.assertEqual(list(res["best_LAMAS_id"]), ["101", "102", "103", "104", "105", "106"])

    def test_arabic_precision_guardrails_reject_false_positives(self):
        osm = _make_osm(
            [
                "نومن",
                "شارع بيسان",
                "شارع النورس",
                "شارع الشابي",
                "دوار الباز",
            ],
            city="רהט",
        )
        lamas = _make_lamas(
            [
                ("201", "ניימן"),
                ("202", "אלביאן"),
                ("203", "אלנרג'ס"),
                ("204", "אלאיובי"),
                ("205", "אלח'באז"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        for status in res["status"]:
            self.assertNotEqual(status, "CONFIDENT")

    def test_arabic_tie_breaking_prefers_street_over_neighborhood_and_exact_translit(self):
        osm = _make_osm(
            [
                "شارع وادي الصفا",
                "ساحة ترشيحا",
                "السهل",
                "شارع السعدية",
            ],
            city="סח'נין",
        )
        lamas = _make_lamas(
            [
                ("102", "דרך וואדי אלספא"),
                ("6761", "שכ ואדי אל ספא"),
                ("6021", "כיכר תרשיחא"),
                ("6500", "תרשיחא"),
                ("4731", "אל סהל"),
                ("6022", 'ככר צה"ל'),
                ("4022", "אל סעדיה"),
                ("4074", "סעידה"),
                ("4125", "סעדא"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"]), ["CONFIDENT"] * 4)
        self.assertEqual(list(res["best_LAMAS_id"]), ["102", "6021", "4731", "4022"])

    def test_embedded_street_number_matching_and_guards(self):
        osm = _make_osm(
            [
                "المدينة القديمة - 6171",
                "6152 (سوق)",
                "אל עומאל אל ערב (5017)",
                "כביש 443",
                "גדוד 21",
            ],
            city="נצרת",
        )
        lamas = _make_lamas(
            [
                ("321", "רח 6171"),
                ("668", "רח 6152"),
                ("404", "רח 5017"),
                ("443", "רח 443"),
                ("21", "רח 21"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"]), ["CONFIDENT", "CONFIDENT", "CONFIDENT", "NEEDS_AI", "NEEDS_AI"])
        self.assertEqual(list(res["best_LAMAS_id"])[:3], ["321", "668", "404"])

    def test_latin_arabic_matching_and_english_guard(self):
        osm = _make_osm(
            [
                "Abu Bakr al-Siddiq",
                "Khalid ibn al-Walid",
                "al-Khamis",
                "Under Pass",
            ],
            city="טירה",
        )
        lamas = _make_lamas(
            [
                ("104", "אבו בכר א-ס'דיק"),
                ("296", "ח'אלד בן אל-ווליד"),
                ("156", "אל-ח'מיס"),
                ("999", "אנדרומדה"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"]), ["CONFIDENT", "CONFIDENT", "CONFIDENT", "NEEDS_AI"])
        self.assertEqual(list(res["best_LAMAS_id"])[:3], ["104", "296", "156"])

    def test_hebrew_arabic_article_matching_in_arab_city(self):
        osm = _make_osm(
            [
                "הגליל",
                "החרוב",
                "ח׳לה",
                "אלניל",
                "א רוודה",
                "ח'לת אל שלח",
                "אלגליל",
            ],
            city="סח'נין",
        )
        lamas = _make_lamas(
            [
                ("131", "אלגליל"),
                ("106", "אלחרוב"),
                ("141", "אלח'לה"),
                ("172", "א-ניל"),
                ("219", "א-רוודא"),
                ("6741", "ח'לת שלח"),
                ("199", "הכרמל"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(list(res["status"])[:6], ["CONFIDENT"] * 6)
        self.assertEqual(list(res["best_LAMAS_id"])[:6], ["131", "106", "141", "172", "219", "6741"])

    def test_mixed_city_blocks_plain_hebrew_to_arabic_false_positive(self):
        # In a mixed city (majority Hebrew streets), "דרך תקוה" must NOT match "אלתקווא" / "אל תקווא",
        # whereas explicit Arabic-article OSM names ("א-טוואל", "אל חרובה") still match cleanly.
        osm = _make_osm(
            [
                "דרך תקוה",
                "א-טוואל",
                "אל חרובה",
                "סלחייה",
            ],
            city="ירושלים",
        )
        lamas = _make_lamas(
            [
                ("101", "הרצל"),
                ("102", "ביאליק"),
                ("103", "ז'בוטינסקי"),
                ("104", "בן גוריון"),
                ("105", "דרך חברון"),
                ("106", "יפו"),
                ("107", "המלך ג'ורג'"),
                ("108", "עמק רפאים"),
                ("5143", "אלתקווא"),
                ("5143", "אל תקווא"),
                ("5268", "אל טואל"),
                ("4710", "ח'רובה"),
                ("4012", "אל סלחייה"),
            ]
        )
        res = find_fuzzy_candidates(osm, lamas)
        self.assertEqual(res.loc[0, "status"], "NEEDS_AI")
        self.assertEqual(list(res["status"])[1:], ["CONFIDENT", "CONFIDENT", "CONFIDENT"])
        self.assertEqual(list(res["best_LAMAS_id"])[1:], ["5268", "4710", "4012"])


class TestArabicPipelineAndLayaIntegration(unittest.TestCase):
    def test_core_wratio_supports_cross_script_without_inflating_unrelated_hebrew(self):
        self.assertGreaterEqual(_core_wratio("شارع صلاح الدين", "צלאח אלדין"), 95.0)
        self.assertGreaterEqual(_core_wratio("شارع خلة بلوط", "ח'לת בלוט"), 85.0)
        self.assertGreaterEqual(_core_wratio("Abu Bakr al-Siddiq", "אבו בכר א-ס'דיק"), 95.0)
        self.assertGreaterEqual(_core_wratio("הגליל", "אלגליל"), 95.0)
        # Unrelated Hebrew pair starting with אל must not be inflated
        self.assertLess(_core_wratio("אלקטרה מול הנוף", "בנין המשטרה"), 30.0)
        # Unrelated Arabic pair must stay low
        self.assertLess(_core_wratio("نومن", "ניימן"), 50.0)

    def test_finalize_mappings_approves_verified_arabic_laya_match(self):
        candidates = pd.DataFrame(
            {
                "osm_id": ["ar-1"],
                "normalized_name": ["شارع خله بلوط"],
                "status": ["NEEDS_AI"],
                "best_score": [85.0],
                "best_LAMAS_id": ["6760"],
                "best_LAMAS_name": ["ח'לת בלוט"],
            }
        )
        ai_decisions = pd.DataFrame(
            {
                "osm_id": ["ar-1"],
                "ai_suggested_LAMAS_id": ["6760"],
                "ai_confidence": [0.92],
                "ai_reasoning": ["Laya selected street_1"],
                "ai_method": ["local_laya"],
            }
        )
        merged, approved = _finalize_mappings(
            candidates,
            ai_decisions,
            lamas_name_by_id={"6760": "ח'לת בלוט"},
            city_name="סח'נין",
        )
        self.assertEqual(merged.loc[0, "mapping_status"], "APPROVED_AI")
        self.assertEqual(list(approved["osm_id"]), ["ar-1"])

    def test_laya_resolver_adds_hebrew_transliteration_hint_for_arabic_streets(self):
        agent = Mock()
        agent.predict.return_value = {
            "answers": {
                "street_match": {
                    "choice": "street_1",
                    "probabilities": {"street_1": 0.91},
                }
            }
        }
        laya_module = ModuleType("laya")
        laya_module.Agent = Mock(return_value=agent)
        with patch.dict(sys.modules, {"laya": laya_module}):
            resolver = LayaAIResolver()

        resolver.resolve_street("סח'נין", "شارع صلاح الدين", {"101": ["צלאח אלדין"]})
        ar_state, _ = agent.predict.call_args.args
        self.assertEqual(ar_state["שם הרחוב ב-OSM"], "شارع صلاح الدين")
        self.assertEqual(ar_state["תעתיק עברי"], "סלאח אלדינ")

        agent.predict.reset_mock()
        resolver.resolve_street("חיפה", "רחוב הרצל", {"101": ["הרצל"]})
        he_state, _ = agent.predict.call_args.args
        self.assertNotIn("תעתיק עברי", he_state)


if __name__ == "__main__":
    unittest.main()
