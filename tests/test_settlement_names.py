import json
import os
import tempfile
import unittest

from settlement_names import (
    canonical_key,
    clean_display_name,
    fuzzy_form,
    load_aliases,
    load_osm_id_overrides,
    name_variants,
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
        self.assertEqual(load_osm_id_overrides(), {})
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "aliases.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(
                    {"osm_id_overrides": {
                        "_meta": "explanation", "יישוב חריג": "n123456",
                    }},
                    handle, ensure_ascii=False,
                )
            self.assertEqual(
                load_osm_id_overrides(path),
                {canonical_key("יישוב חריג"): "n123456"},
            )

    def test_missing_alias_file_is_not_an_error(self):
        self.assertEqual(load_aliases("/does/not/exist.json"), {})


if __name__ == "__main__":
    unittest.main()
