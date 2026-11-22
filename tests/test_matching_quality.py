import unittest

import pandas as pd

from normalization import DEFAULT_CONFIDENT_THRESHOLD, find_fuzzy_candidates


class TestMatchingQuality(unittest.TestCase):
    def test_default_threshold_is_90(self):
        self.assertEqual(DEFAULT_CONFIDENT_THRESHOLD, 90.0)

    def test_fuzzy_splits_strictly_at_90_and_sends_everything_below_to_ai(self):
        osm_gdf = pd.DataFrame(
            {
                "osm_id": ["exact", "mid", "low"],
                "normalized_name": [
                    "שדרות רוטשילד",
                    "רוטשילד מזרח",
                    "שם שונה לגמרי",
                ],
            }
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "202"],
                "LAMAS_name": ["שדרות רוטשילד", "הנביאים"],
                "normalized_name": ["שדרות רוטשילד", "הנביאים"],
            }
        )

        candidates = find_fuzzy_candidates(osm_gdf, lamas_df).set_index("osm_id")

        self.assertEqual(candidates.loc["exact", "status"], "CONFIDENT")
        self.assertEqual(candidates.loc["exact", "best_LAMAS_id"], "101")
        self.assertEqual(candidates.loc["exact", "best_score"], 100.0)

        # Both medium (<90%) and very low (<50%) scores go to AI — nothing dropped.
        self.assertEqual(candidates.loc["mid", "status"], "NEEDS_AI")
        self.assertLess(candidates.loc["mid", "best_score"], 90.0)
        self.assertEqual(candidates.loc["low", "status"], "NEEDS_AI")
        self.assertLess(candidates.loc["low", "best_score"], 50.0)

    def test_fuzzy_matches_through_synonym_aliases(self):
        osm_gdf = pd.DataFrame(
            {"osm_id": ["osm-1"], "normalized_name": ["השקד"]}
        )
        lamas_df = pd.DataFrame(
            {
                "LAMAS_id": ["101", "101"],
                "LAMAS_name": ["שקד", "השקד"],
                "normalized_name": ["שקד", "השקד"],
            }
        )

        candidates = find_fuzzy_candidates(osm_gdf, lamas_df)

        self.assertEqual(candidates.iloc[0]["status"], "CONFIDENT")
        self.assertEqual(candidates.iloc[0]["best_LAMAS_id"], "101")
        self.assertEqual(candidates.iloc[0]["best_score"], 100.0)


if __name__ == "__main__":
    unittest.main()
