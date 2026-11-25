import json
import os
import tempfile
import unittest

from settlement_names import (
    canonical_key,
    clean_display_name,
    extract_qualifiers,
    fuzzy_form,
    has_geresh_conflict,
    has_qualifier_conflict,
    ktiv_haser_key,
    load_aliases,
    load_osm_id_overrides,
    lookup_by_settlement_key,
    name_variants,
    qualifier_match_bonus,
    settlement_identity_key,
)


class TestSettlementNames(unittest.TestCase):
    def test_canonical_key_unifies_lamas_and_osm_spellings(self):
        pairs = [
            ("תל אביב - יפו", "תל אביב-יפו"),
            ("אבו קרינאת )יישוב(", "אבו קרינאת"),
            ("קרית גת", "קריית גת"),
            ("סח'נין", "סחנין"),
            ("עיריית חיפה", "חיפה"),
            ("ג'דיידה-מכר", "גדיידה מכר"),
            ("באקה אל-גרביה", "באקה אל גרביה"),
            ("أبو ربيعة", "ابو ربيعه"),
        ]
        for lamas, osm in pairs:
            with self.subTest(lamas=lamas, osm=osm):
                self.assertEqual(canonical_key(lamas), canonical_key(osm))

    def test_display_name_keeps_geresh_for_nominatim_queries(self):
        self.assertEqual(clean_display_name("אבו ג'ווייעד )שבט("), "אבו ג'ווייעד")
        self.assertEqual(canonical_key("אבו ג'ווייעד"), canonical_key("אבו גווייעד"))
        self.assertEqual(fuzzy_form("קרית שמונה"), "קריית שמונה")

    def test_aliases_are_loaded_and_variants_are_bounded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "aliases.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {"aliases": {"עיר בדיקה": ["בדיקה אחרת", "Test Town", "עיר בדיקה"]}},
                    handle,
                    ensure_ascii=False,
                )
            aliases = load_aliases(path)
            self.assertIn(canonical_key("עיר בדיקה"), aliases)
            variants = name_variants("עיר בדיקה", aliases)
            self.assertEqual(variants[0], "עיר בדיקה")
            self.assertIn("בדיקה אחרת", variants)
            self.assertEqual(len(variants), len(set(canonical_key(v) for v in variants)))

    def test_curated_aliases_cover_arabic_spelling_variants(self):
        variants = name_variants("אבו רובייעה )שבט(", load_aliases())
        self.assertIn("أبو ربيعة", variants)
        self.assertIn("أبو ربية", variants)

    def test_osm_id_overrides_skip_metadata_and_read_only_verified_pins(self):
        live = load_osm_id_overrides()
        self.assertNotIn("_meta", live)
        for key, value in live.items():
            self.assertTrue(key)
            tokens = value if isinstance(value, list) else [value]
            self.assertTrue(tokens)
            for token in tokens:
                self.assertRegex(token, r"^[nwr]\d+$")

        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "aliases.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {"osm_id_overrides": {
                        "_meta": "explanation",
                        "יישוב חריג": "n123456",
                        "מועצה מאוחדת": ["w111", "w222"],
                    }},
                    handle, ensure_ascii=False,
                )
            self.assertEqual(
                load_osm_id_overrides(path),
                {
                    canonical_key("יישוב חריג"): "n123456",
                    canonical_key("מועצה מאוחדת"): ["w111", "w222"],
                },
            )

    def test_identity_key_separates_gat_jatt_and_twin_settlements(self):
        self.assertNotEqual(
            settlement_identity_key("גת )קיבוץ("),
            settlement_identity_key("ג'ת"),
        )
        self.assertEqual(
            settlement_identity_key("ג'ת"),
            settlement_identity_key("ג׳ת"),
        )
        self.assertNotEqual(
            settlement_identity_key("גת"),
            settlement_identity_key("ג'ת"),
        )
        self.assertNotEqual(
            settlement_identity_key("טמרה"),
            settlement_identity_key("טמרה )יזרעאל("),
        )
        self.assertNotEqual(
            settlement_identity_key("טייבה"),
            settlement_identity_key("טייבה )בעמק("),
        )
        self.assertNotEqual(
            settlement_identity_key("גבעת חיים )איחוד("),
            settlement_identity_key("גבעת חיים )מאוחד("),
        )
        self.assertNotEqual(
            settlement_identity_key("בן שמן )מושב("),
            settlement_identity_key("בן שמן )כפר נוער("),
        )
        # Plain names keep identical key to canonical_key
        self.assertEqual(
            settlement_identity_key("תל אביב - יפו"),
            canonical_key("תל אביב - יפו"),
        )

    def test_geresh_and_qualifier_conflicts_protect_twins(self):
        self.assertTrue(has_geresh_conflict("גת )קיבוץ(", ["ג'ת", "جت", "Jatt"]))
        self.assertTrue(has_geresh_conflict("ג'ת", ["גת", "Gat"]))
        self.assertFalse(has_geresh_conflict("ג'ת", ["ג’ת", "جت", "Jatt"]))
        self.assertFalse(has_geresh_conflict("גת )קיבוץ(", ["גת", "Gat"]))
        # Longer Arabic names with omitted geresh in OSM remain allowed
        self.assertFalse(has_geresh_conflict("ג'דיידה-מכר", ["גדיידה מכר"]))

        self.assertTrue(has_qualifier_conflict("גבעת חיים )מאוחד(", ["גבעת חיים (איחוד)"]))
        self.assertFalse(has_qualifier_conflict("גבעת חיים )מאוחד(", ["גבעת חיים (מאוחד)"]))
        self.assertEqual(qualifier_match_bonus("גבעת חיים )מאוחד(", ["גבעת חיים (מאוחד)"]), 1)
        self.assertTrue(has_qualifier_conflict("מרחביה )קיבוץ(", ["מושב מרחביה", "Moshav Merhavia"]))
        self.assertTrue(has_qualifier_conflict("בן שמן )מושב(", ["כפר הנוער בן שמן"]))

    def test_ktiv_haser_and_parenthetical_variants(self):
        self.assertEqual(ktiv_haser_key("עופרה"), ktiv_haser_key("עפרה"))
        self.assertNotEqual(ktiv_haser_key("עופרה"), ktiv_haser_key("עופר"))
        self.assertEqual(ktiv_haser_key("סוואעד חמירה"), ktiv_haser_key("סואעד חמירה"))
        self.assertEqual(extract_qualifiers("כפר רוזנואלד )זרעית("), ["זרעית"])
        self.assertIn("זרעית", name_variants("כפר רוזנואלד )זרעית(", aliases={}))
        self.assertIn("קיבוץ מרחביה", name_variants("מרחביה )קיבוץ(", aliases={}))

        # lookup_by_settlement_key never leaks ג'ת's config into גת )קיבוץ(
        centers = {settlement_identity_key("ג'ת"): (35.03, 32.40)}
        self.assertIsNone(lookup_by_settlement_key(centers, "גת )קיבוץ("))
        self.assertEqual(lookup_by_settlement_key(centers, "ג'ת"), (35.03, 32.40))

    def test_missing_alias_file_is_not_an_error(self):
        self.assertEqual(load_aliases("/does/not/exist.json"), {})


if __name__ == "__main__":
    unittest.main()
