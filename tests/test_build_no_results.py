import json
import os
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

import batch_process_settlements
import build_no_results


class TestBuildNoResults(unittest.TestCase):
    def test_separates_twin_settlements_and_counts_street_level_ids_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            html_dir = os.path.join(tmp, "HTML")
            os.makedirs(html_dir)
            report_path = os.path.join(tmp, "settlement_match_report.csv")
            store_path = os.path.join(tmp, "settlement_matches.json")
            lamas_csv = os.path.join(tmp, "lamas_data.csv")
            out_path = os.path.join(tmp, "no_results.json")

            # Stale no_results.json from an older run must NOT leak into the new output.
            with open(out_path, "w", encoding="utf-8") as fh:
                json.dump(
                    [
                        {
                            "name": "טמרה",
                            "category": "unresolved",
                            "reason": "לא נמצא יישוב מתאים ב-OSM",
                            "detail": "stale",
                            "lamas": 1239,
                        }
                    ],
                    fh,
                    ensure_ascii=False,
                )

            pd.DataFrame(
                [
                    {"settlement": "גת )קיבוץ(", "status": "matched", "reason": ""},
                    {"settlement": "ג'ת", "status": "matched", "reason": ""},
                    {"settlement": "טמרה", "status": "matched", "reason": ""},
                    {"settlement": "יישוב חסר", "status": "unresolved", "reason": "No OSM match"},
                ]
            ).to_csv(report_path, index=False, encoding="utf-8-sig")

            pd.DataFrame(
                [
                    # גת )קיבוץ( has ONLY its own-name pseudo-record (0 streets)
                    {"city": "גת )קיבוץ(", "LAMAS_id": "9000", "LAMAS_name": "גת )קיבוץ("},
                    {"city": "גת )קיבוץ(", "LAMAS_id": "9000", "LAMAS_name": "גת"},
                    # ג'ת has own-name 9000 + 2 real streets (multiple aliases each)
                    {"city": "ג'ת", "LAMAS_id": "9000", "LAMAS_name": "ג'ת"},
                    {"city": "ג'ת", "LAMAS_id": "101", "LAMAS_name": "אל-קודס"},
                    {"city": "ג'ת", "LAMAS_id": "101", "LAMAS_name": "אלקודס"},
                    {"city": "ג'ת", "LAMAS_id": "102", "LAMAS_name": "אל-סלאם"},
                    # טמרה has an HTML page so it must not appear in no_results.json
                    {"city": "טמרה", "LAMAS_id": "101", "LAMAS_name": "אל-גליל"},
                    # יישוב חסר has 1 real street + own-name record
                    {"city": "יישוב חסר", "LAMAS_id": "9000", "LAMAS_name": "יישוב חסר"},
                    {"city": "יישוב חסר", "LAMAS_id": "201", "LAMAS_name": "השלום"},
                ]
            ).to_csv(lamas_csv, index=False)

            with patch.object(
                build_no_results,
                "scan_settlements",
                return_value=[{"name": "טמרה"}, {"name": "ג'ת"}],
            ):
                items = build_no_results.main(
                    out=out_path,
                    html_dir=html_dir,
                    report_path=report_path,
                    store_path=store_path,
                    lamas_csv_path=lamas_csv,
                    no_data_path=os.path.join(tmp, "missing_nodata.txt"),
                    retry_pending_path=os.path.join(tmp, "missing_retry.txt"),
                    check_pbf=False,
                )

            by_name = {item["name"]: item for item in items}
            self.assertNotIn("טמרה", by_name)
            self.assertNotIn("ג'ת", by_name)
            self.assertEqual(by_name["גת )קיבוץ("]["category"], "pending")
            self.assertEqual(by_name["גת )קיבוץ("]["lamas"], 0)
            self.assertEqual(by_name["יישוב חסר"]["category"], "unresolved")
            self.assertEqual(by_name["יישוב חסר"]["lamas"], 1)

    def test_pbf_empty_settlements_categorized_as_no_named_streets_not_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            report_path = os.path.join(tmp, "settlement_match_report.csv")
            store_path = os.path.join(tmp, "settlement_matches.json")
            lamas_csv = os.path.join(tmp, "lamas_data.csv")
            out_path = os.path.join(tmp, "no_results.json")

            with open(store_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "entries": {
                            "זרזיר": {
                                "settlement": "זרזיר",
                                "status": "matched",
                                "match": {"settlement_name": "זרזיר", "osm_id": "1933845"},
                            }
                        }
                    },
                    fh,
                    ensure_ascii=False,
                )
            pd.DataFrame(
                [{"settlement": "זרזיר", "status": "matched", "reason": ""}]
            ).to_csv(report_path, index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {"city": "זרזיר", "LAMAS_id": "9000", "LAMAS_name": "זרזיר"},
                    {"city": "זרזיר", "LAMAS_id": "101", "LAMAS_name": "אל-סלאם"},
                ]
            ).to_csv(lamas_csv, index=False)

            with (
                patch.object(build_no_results, "scan_settlements", return_value=[]),
                patch.object(build_no_results, "_pbf_has_no_named_streets", return_value=True),
            ):
                items = build_no_results.main(
                    out=out_path,
                    html_dir=tmp,
                    report_path=report_path,
                    store_path=store_path,
                    lamas_csv_path=lamas_csv,
                    no_data_path=os.path.join(tmp, "missing_nodata.txt"),
                    retry_pending_path=os.path.join(tmp, "missing_retry.txt"),
                    check_pbf=True,
                )
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["category"], "no_named_streets")
            self.assertEqual(items[0]["lamas"], 1)

    def test_diagnostics_handles_list_valued_highway_tags_without_array_error(self):
        from diagnostics import _non_addressable_osm_names

        df = pd.DataFrame(
            {
                "osm_id": ["1", "2", "3"],
                "normalized_name": ["שביל היער", "רחוב הרצל", "סמטת הגן"],
                "highway": [["path", "footway"], ["residential", "service"], "service"],
            }
        )
        non_addr = _non_addressable_osm_names(df)
        self.assertEqual(non_addr, {"שביל היער", "סמטת הגן"})

    def test_pbf_no_named_streets_skips_overpass_and_returns_empty_frame(self):
        import geopandas as gpd
        import OSM_streets
        import data_loader

        empty_pbf = gpd.GeoDataFrame(
            columns=["osm_id", "osm_name", "highway", "geometry", "city"]
        )
        empty_pbf.attrs["pbf_no_named_streets"] = True

        with (
            patch("osm_pbf_loader.fetch_pbf_street_data", return_value=empty_pbf),
            patch.object(OSM_streets.ox, "graph_from_polygon") as overpass_poly,
        ):
            res = OSM_streets.fetch_osm_street_data({"settlement_name": "זרזיר"})
        overpass_poly.assert_not_called()
        self.assertIsNotNone(res)
        self.assertTrue(res.empty)
        self.assertTrue(res.attrs.get("pbf_no_named_streets"))

        with (
            patch.object(data_loader, "fetch_osm_street_data", return_value=empty_pbf),
            patch.object(data_loader, "is_fresh", return_value=False),
        ):
            loaded = data_loader.load_or_fetch_osm({"settlement_name": "זרזיר"}, force_refresh=True)
        self.assertTrue(loaded.empty)

    def test_refresh_dashboard_rebuilds_no_results_before_index(self):
        calls = []
        with patch("build_no_results.main", side_effect=lambda: calls.append("no_results")), patch(
            "generate_index.generate_index_html", side_effect=lambda: calls.append("index")
        ):
            batch_process_settlements._refresh_dashboard()
        self.assertEqual(calls, ["no_results", "index"])


if __name__ == "__main__":
    unittest.main()
