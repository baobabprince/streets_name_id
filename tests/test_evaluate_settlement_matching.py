import csv
import os
import tempfile
import unittest

import evaluate_settlement_matching as audit


def report_rows():
    return [
        {
            "settlement": "חיפה", "status": "matched", "source": "pbf",
            "name_score": "100.0", "center_distance_m": "1200",
            "osm_id": "1", "osm_type": "relation", "display_name": "חיפה, Israel",
            "reason": "", "validation_message": "Local OSM boundary match",
        },
        {
            "settlement": "אבו כביר", "status": "matched", "source": "nominatim",
            "name_score": "100.0", "center_distance_m": "",
            "osm_id": "2", "osm_type": "way",
            "display_name": "אבו כביר דרום, רהט, ישראל",
            "reason": "",
            "validation_message": "Exact settlement name match; ignored 2 non-settlement OSM candidates (neighbourhood)",
        },
        {
            "settlement": "הוזייל", "status": "unresolved", "source": "",
            "name_score": "", "center_distance_m": "",
            "osm_id": "", "osm_type": "", "display_name": "",
            "reason": "Only non-settlement OSM features matched (neighbourhood)",
            "validation_message": "",
        },
        {
            "settlement": "ברכה", "status": "matched", "source": "nominatim",
            "name_score": "93.3", "center_distance_m": "",
            "osm_id": "3", "osm_type": "", "display_name": "הר ברכה, שטח C",
            "reason": "", "validation_message": "Exact settlement name match",
        },
        {
            "settlement": "יישוב כפול א", "status": "matched", "source": "pbf",
            "name_score": "100.0", "center_distance_m": "500",
            "osm_id": "9", "osm_type": "node", "display_name": "יישוב כפול א, Israel",
            "reason": "", "validation_message": "Local OSM place match",
        },
        {
            "settlement": "יישוב כפול ב", "status": "matched", "source": "pbf",
            "name_score": "100.0", "center_distance_m": "600",
            "osm_id": "9", "osm_type": "node", "display_name": "יישוב כפול ב, Israel",
            "reason": "", "validation_message": "Local OSM place match",
        },
    ]


class TestEvaluateSettlementMatching(unittest.TestCase):
    def test_flags_nominatim_fuzzy_and_suburb_rejections(self):
        rows = report_rows()
        duplicates = audit._duplicate_osm_ids(rows)
        self.assertEqual(duplicates, {"node:9"})

        issues = audit.classify_row(rows[1], duplicates)
        self.assertIn("nominatim_fallback", issues)
        self.assertIn("suburb_candidates_ignored", issues)
        self.assertIn("missing_center_distance", issues)
        self.assertIn("display_name_mismatch", issues)

        issues = audit.classify_row(rows[2], duplicates)
        self.assertEqual(issues, ["unresolved_non_settlement"])

        issues = audit.classify_row(rows[3], duplicates)
        self.assertIn("fuzzy_name", issues)
        self.assertIn("empty_osm_type", issues)

        self.assertIn("duplicate_osm_id", audit.classify_row(rows[4], duplicates))

    def test_clean_offline_match_is_ok(self):
        enriched = audit.build_audit(report_rows())[0]
        self.assertEqual(enriched["priority"], "OK")
        self.assertEqual(enriched["issues"], "")
        self.assertEqual(enriched["suggested_action"], "ok")

    def test_cli_writes_audit_csv_markdown_and_retry_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report = os.path.join(temp_dir, "report.csv")
            out_dir = os.path.join(temp_dir, "out")
            with open(report, "w", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(report_rows()[0]))
                writer.writeheader()
                writer.writerows(report_rows())

            code = audit.main([
                "--report", report, "--out-dir", out_dir, "--top", "3",
            ])
            self.assertEqual(code, 0)
            audit_csv = os.path.join(out_dir, "settlement_match_audit.csv")
            audit_md = os.path.join(out_dir, "settlement_match_audit.md")
            retry = os.path.join(out_dir, "settlement_match_retry.txt")
            self.assertTrue(os.path.exists(audit_csv))
            self.assertTrue(os.path.exists(audit_md))
            self.assertTrue(os.path.exists(retry))
            with open(audit_csv, encoding="utf-8-sig", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 6)
            self.assertIn("issues", rows[0])
            self.assertIn("priority", rows[0])
            with open(retry, encoding="utf-8") as handle:
                names = [
                    line.strip() for line in handle
                    if line.strip() and not line.startswith("#")
                ]
            self.assertIn("הוזייל", names)
            self.assertIn("ברכה", names)
            self.assertNotIn("חיפה", names)
            with open(audit_md, encoding="utf-8") as handle:
                md = handle.read()
            self.assertIn("Settlement matching audit", md)
            self.assertIn("unresolved_non_settlement", md)


if __name__ == "__main__":
    unittest.main()
