"""Creation/source timestamps: recording, propagation and display."""
import json
import os
import struct
import tempfile
import unittest
import zlib
from unittest import mock

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

import data_loader
import lamas_streets
import provenance
from generate_html import create_html_from_gdf


def _key(number, wire):
    return bytes([number << 3 | wire]) if number < 16 else _varint(number << 3 | wire)


def _varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _bytes_field(number, payload):
    return _key(number, 2) + _varint(len(payload)) + payload


def _write_pbf(path, seconds, compressed=True):
    header_block = _bytes_field(4, b"OsmSchema-V0.6")
    if seconds is not None:
        header_block += _key(32, 0) + _varint(seconds)
    blob = (
        _key(2, 0) + _varint(len(header_block)) + _bytes_field(3, zlib.compress(header_block))
        if compressed
        else _bytes_field(1, header_block)
    )
    blob_header = _bytes_field(1, b"OSMHeader") + _key(3, 0) + _varint(len(blob))
    with open(path, "wb") as handle:
        handle.write(struct.pack(">I", len(blob_header)) + blob_header + blob)


class PbfHeaderTests(unittest.TestCase):
    def test_reads_replication_timestamp_from_compressed_and_raw_headers(self):
        with tempfile.TemporaryDirectory() as tmp:
            for compressed in (True, False):
                path = os.path.join(tmp, f"x{compressed}.osm.pbf")
                _write_pbf(path, 1763800000, compressed=compressed)
                self.assertEqual(
                    provenance.pbf_replication_timestamp(path), "2025-11-22T08:26:40+00:00"
                )

    def test_missing_timestamp_or_invalid_file_is_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            no_stamp = os.path.join(tmp, "a.osm.pbf")
            _write_pbf(no_stamp, None)
            junk = os.path.join(tmp, "b.osm.pbf")
            with open(junk, "wb") as handle:
                handle.write(b"not a pbf at all")
            self.assertIsNone(provenance.pbf_replication_timestamp(no_stamp))
            self.assertIsNone(provenance.pbf_replication_timestamp(junk))
            self.assertIsNone(provenance.pbf_replication_timestamp(os.path.join(tmp, "missing")))


class DisplayTests(unittest.TestCase):
    def test_aware_times_are_shown_in_israel_time_and_naive_as_recorded(self):
        self.assertEqual(provenance.format_local("2026-11-23T15:34:00+00:00"), "23.11.2026, 17:34")
        self.assertEqual(provenance.format_local("2026-07-01T09:00:00+00:00"), "01.07.2026, 12:00")
        self.assertEqual(provenance.format_local("2026-11-22T10:40:49.184962"), "22.11.2026, 10:40")
        self.assertEqual(provenance.zone_label("2026-11-22T10:40:49"), "")
        self.assertIsNone(provenance.format_local("not a date"))
        self.assertIsNone(provenance.format_local(None))

    def test_build_time_honours_source_date_epoch(self):
        with mock.patch.dict(os.environ, {"SOURCE_DATE_EPOCH": "1700000000"}):
            self.assertEqual(provenance.build_time(), "2023-11-14T22:13:20+00:00")

    def test_unrecorded_values_are_labelled_not_estimated(self):
        footer = provenance.page_sources_html(None)
        self.assertEqual(footer.count("לא תועד"), 3)
        self.assertIn("מועד יצירת הדף לא תועד", provenance.page_stamp_html({}))

    def test_osm_snapshot_is_preferred_over_extraction_time(self):
        when, label = provenance.osm_data_time(
            {"osm_snapshot_at": "2026-11-19T20:21:00+00:00", "extracted_at": "2026-11-20T10:00:00+00:00"}
        )
        self.assertEqual(when, "2026-11-19T20:21:00+00:00")
        self.assertIn("PBF", label)
        self.assertEqual(provenance.osm_data_time({"retrieved_at": "x"})[1], "נשלפו מ־OSM")


class SidecarTests(unittest.TestCase):
    def test_sidecar_round_trip_and_mtime_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cache.pkl")
            with open(path, "w") as handle:
                handle.write("x")
            os.utime(path, (1700000000, 1700000000))
            self.assertEqual(
                provenance.cached_file_provenance(path),
                {"cache_written_at": "2023-11-14T22:13:20+00:00"},
            )
            provenance.write_sidecar(path, {"retrieved_at": "2026-01-01T00:00:00+00:00", "empty": None})
            self.assertEqual(
                provenance.cached_file_provenance(path), {"retrieved_at": "2026-01-01T00:00:00+00:00"}
            )
            provenance.clear_sidecar(path)
            self.assertNotIn("retrieved_at", provenance.cached_file_provenance(path))


class LoaderPropagationTests(unittest.TestCase):
    def setUp(self):
        data_loader._LAMAS_MEMORY_CACHE.clear()
        data_loader._OSM_MEMORY_CACHE.clear()

    def _osm_frame(self, attrs):
        gdf = gpd.GeoDataFrame(
            {"osm_id": ["1"], "osm_name": ["הרצל"], "geometry": [LineString([(34.8, 32.0), (34.81, 32.0)])]},
            crs="EPSG:4326",
        )
        if attrs:
            gdf.attrs["provenance"] = attrs
        return gdf

    def test_fresh_osm_fetch_records_origin_for_later_cache_hits(self):
        origin = {"source": "pbf", "osm_snapshot_at": "2026-11-19T20:21:00+00:00"}
        with tempfile.TemporaryDirectory() as tmp:
            template = os.path.join(tmp, "osm_data_{place}.pkl")
            with mock.patch.object(data_loader, "OSM_CACHE_TEMPLATE", template), mock.patch.object(
                data_loader, "fetch_osm_street_data", return_value=self._osm_frame(origin)
            ):
                fresh = data_loader.load_or_fetch_osm("עיר", force_refresh=True)
                data_loader._OSM_MEMORY_CACHE.clear()
                cached = data_loader.load_or_fetch_osm("עיר")
                self.assertEqual(data_loader.osm_provenance("עיר"), origin)
            self.assertEqual(fresh.attrs["provenance"], origin)
            self.assertEqual(cached.attrs["provenance"], origin)

    def test_refetch_without_origin_clears_stale_sidecar(self):
        with tempfile.TemporaryDirectory() as tmp:
            template = os.path.join(tmp, "osm_data_{place}.pkl")
            with mock.patch.object(data_loader, "OSM_CACHE_TEMPLATE", template):
                with mock.patch.object(
                    data_loader, "fetch_osm_street_data",
                    return_value=self._osm_frame({"source": "overpass", "retrieved_at": "2020-01-01T00:00:00+00:00"}),
                ):
                    data_loader.load_or_fetch_osm("עיר", force_refresh=True)
                with mock.patch.object(data_loader, "fetch_osm_street_data", return_value=self._osm_frame(None)):
                    data_loader.load_or_fetch_osm("עיר", force_refresh=True)
                self.assertEqual(list(data_loader.osm_provenance("עיר")), ["cache_written_at"])

    def test_lamas_retrieval_time_survives_cache(self):
        frame = pd.DataFrame({"LAMAS_id": ["101"], "LAMAS_name": ["רחוב"], "city": ["חיפה"]})
        frame.attrs["provenance"] = {"retrieved_at": "2026-11-20T08:00:00+00:00"}
        with tempfile.TemporaryDirectory() as tmp:
            cache = os.path.join(tmp, "LAMAS_data.pkl")
            with mock.patch.object(data_loader, "LAMAS_CACHE", cache), mock.patch.object(
                data_loader, "fetch_all_LAMAS_data", return_value=frame
            ):
                fresh = data_loader.load_or_fetch_LAMAS(force_refresh=True)
                data_loader._LAMAS_MEMORY_CACHE.clear()
                cached = data_loader.load_or_fetch_LAMAS()
        for df in (fresh, cached):
            self.assertEqual(df.attrs["provenance"]["retrieved_at"], "2026-11-20T08:00:00+00:00")
            self.assertEqual(df.attrs["provenance"]["resource_id"], lamas_streets.RESOURCE_ID)

    def test_lamas_api_download_is_stamped_and_csv_cache_keeps_it(self):
        payload = {"result": {"total": 1, "records": [
            {"_id": 1, "city_code": "4000", "city_name": "חיפה", "street_code": "101",
             "street_name": "הרצל", "official_code": "101"}]}}
        with tempfile.TemporaryDirectory() as tmp:
            csv = os.path.join(tmp, "lamas_data.csv")
            with mock.patch.object(lamas_streets, "CACHE_FILE", csv), mock.patch.object(
                lamas_streets, "_get_api_json", return_value=payload
            ), mock.patch.object(lamas_streets, "utc_now", return_value="2026-11-21T06:00:00+00:00"):
                fetched = lamas_streets.fetch_all_LAMAS_data(force_refresh=True)
                reloaded = lamas_streets._load_csv_cache()
        self.assertEqual(fetched.attrs["provenance"]["retrieved_at"], "2026-11-21T06:00:00+00:00")
        self.assertEqual(reloaded.attrs["provenance"]["retrieved_at"], "2026-11-21T06:00:00+00:00")


class PageRenderingTests(unittest.TestCase):
    def test_page_shows_creation_and_source_times_and_embeds_them(self):
        gdf = gpd.GeoDataFrame(
            {
                "osm_name": ["הרצל"], "LAMAS_name": ["הרצל"], "LAMAS_id": ["101"],
                "match_type": ["exact"], "final_LAMAS_id": ["101"],
                "geometry": [LineString([(34.80, 32.00), (34.81, 32.01)])],
            },
            crs="EPSG:4326",
        )
        sources = {
            "osm": {"source": "pbf", "osm_snapshot_at": "2026-11-19T20:21:00+00:00"},
            "lamas": {"source": "data.gov.il", "retrieved_at": "2026-11-20T08:00:00+00:00"},
        }
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                with mock.patch("provenance.utc_now", return_value="2026-11-23T13:05:00+00:00"):
                    create_html_from_gdf(gdf, "יישוב בדיקה", None, provenance=sources)
                html = open(os.path.join("HTML", os.listdir("HTML")[0]), encoding="utf-8").read()
            finally:
                os.chdir(cwd)
        self.assertIn('הדף נוצר <time datetime="2026-11-23T13:05:00+00:00">23.11.2026, 15:05</time>', html)
        self.assertIn("נתוני OSM: <time", html)
        self.assertIn("19.11.2026, 22:21", html)
        self.assertIn("20.11.2026, 10:00", html)
        self.assertNotIn("@@", html)
        meta = json.loads(html.split('<script id="page-meta" type="application/json">')[1].split("</script>")[0])
        self.assertEqual(meta["provenance"]["generated_at"], "2026-11-23T13:05:00+00:00")
        self.assertEqual(meta["provenance"]["osm"]["osm_snapshot_at"], "2026-11-19T20:21:00+00:00")


if __name__ == "__main__":
    unittest.main()
