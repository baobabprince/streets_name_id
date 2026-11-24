"""
Settlement results page ("חדר השוואה") for one settlement.

The pipeline calls :func:`create_html_from_gdf` with the diagnostic
GeoDataFrame. Each OSM street name becomes one compact record (status, names,
final ID, score, AI fields); its geometry is stored in WGS84 for the Leaflet
map. The page layout and UI live in ``settlement_template.html`` and
``assets/room.{js,css}`` (see ``settlement_page.py``).
"""

import hashlib
import json
import os
import re
import sys

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

from data_loader import safe_place_name as _safe_place_name
from provenance import page_provenance
from settlement_page import render_page

GEOMETRY_NOTE = "מיקום הרחובות מנתוני OSM, בדיוק של כמטר."


def display_place_name(value: str) -> str:
    """Strip the Nominatim suffix that leaks into page titles."""
    return re.sub(r",\s*Israel\s*$", "", _clean(value), flags=re.I).strip()


def _clean(value):
    """Return a stripped string, or '' for missing values."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "<na>", "nat"}:
        return ""
    return text


def _score_value(value):
    try:
        return None if pd.isna(value) else float(value)
    except (TypeError, ValueError):
        return None


def _record_key(osm_name, lamas_name, lamas_id, status, ai_id):
    raw = "\u0001".join([osm_name, lamas_name, lamas_id, status, ai_id])
    # Readable prefix + stable hash keeps the DOM attribute short.
    return f"{status[:4]}-{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]}"


def _street_status(row) -> str:
    """Display status of a named OSM street in a diagnostic row."""
    is_matched = bool(_clean(row.get("final_LAMAS_id")))
    mapping_status = _clean(row.get("mapping_status"))
    best_score = _score_value(row.get("best_score"))

    if mapping_status == "GEOGRAPHIC_REVIEW":
        return "geo-review"
    if mapping_status in ("APPROVED_AI", "AI_PROPOSED"):
        return "ai"
    if is_matched:
        if best_score is not None and best_score < 100:
            return "partial"
        return "perfect"
    return "unmatched"


def _geometry_lines(geom):
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, LineString):
        return [geom]
    if isinstance(geom, MultiLineString):
        return list(geom.geoms)
    return []


def _govmap_center(gdf):
    """Centre of the rendered extent in ITM (EPSG:2039) for GovMap links."""
    try:
        minx, miny, maxx, maxy = gdf.total_bounds
        source_crs = gdf.crs or "EPSG:3857"
        if getattr(source_crs, "is_geographic", False):
            source_crs = "EPSG:4326"
        from pyproj import Transformer

        transformer = Transformer.from_crs(source_crs, "EPSG:2039", always_xy=True)
        x, y = transformer.transform((minx + maxx) / 2, (miny + maxy) / 2)
        if x != x or y != y:  # NaN
            return None
        return {"x": int(round(x)), "y": int(round(y))}
    except Exception:
        return None


def _page_meta(gdf, place_name, diagnostics, counts, match_pct, provenance=None):
    """Small JSON summary embedded in the page (and reused by the dashboard)."""
    diag = {}
    if diagnostics:
        for key in (
            "total_osm_streets",
            "total_lamas_streets",
            "confident_matches",
            "ai_proposed_matches",
            "geographic_review_matches",
            "total_matched",
            "unmatched_osm_streets",
            "unmatched_lamas_count",
        ):
            value = diagnostics.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                diag[key] = value
    payload = {
        "place": place_name,
        "match_pct": round(match_pct, 1),
        "total_segments": int(len(gdf)),
        "counts": counts,
        "diagnostics": diag,
    }
    center = _govmap_center(gdf)
    if center:
        payload["govmap"] = center
    if provenance:
        payload["provenance"] = provenance
    return payload


def _has_geometry(gdf) -> bool:
    return (
        isinstance(gdf, gpd.GeoDataFrame)
        and gdf.geometry.name in gdf.columns
        and bool(gdf.geometry.notna().any())
    )


def _with_crs(gdf):
    """Return ``gdf`` with a CRS; unlabelled frames are inferred from bounds."""
    if getattr(gdf, "crs", None) is not None:
        return gdf
    minx, miny, maxx, maxy = gdf.total_bounds
    geographic = all(abs(v) <= 180 for v in (minx, miny, maxx, maxy))
    return gdf.set_crs(epsg=4326 if geographic else 3857)


def build_records(gdf):
    """Compact per-street records, their WGS84 lines and status counts."""
    counts = {"perfect": 0, "partial": 0, "ai": 0, "geo-review": 0, "unmatched": 0, "unnamed": 0}
    records, geometry, record_index = [], [], {}
    has_geometry = _has_geometry(gdf)
    if has_geometry:
        gdf = _with_crs(gdf).to_crs(epsg=4326)

    for _, row in gdf.iterrows():
        osm_name = _clean(row.get("osm_name"))
        lamas_name = _clean(row.get("best_LAMAS_name"))
        final_lamas_id = _clean(row.get("final_LAMAS_id"))
        mapping_status = _clean(row.get("mapping_status"))
        best_score = _score_value(row.get("best_score"))
        is_ai = mapping_status in ("APPROVED_AI", "AI_PROPOSED")
        ai_suggestion = _clean(row.get("ai_suggested_LAMAS_id")) if is_ai else ""
        ai_confidence = _score_value(row.get("ai_confidence")) if is_ai else None
        status = _street_status(row) if osm_name else "unnamed"

        key = _record_key(osm_name, lamas_name, final_lamas_id, status, ai_suggestion)
        rec_id = record_index.get(key)
        if rec_id is None:
            rec_id = len(records)
            record_index[key] = rec_id
            distance = _score_value(row.get("geographic_distance_m"))
            records.append(
                [
                    status,
                    osm_name,
                    lamas_name,
                    final_lamas_id,
                    "" if best_score is None else round(best_score, 1),
                    ai_suggestion,
                    "" if ai_confidence is None else round(ai_confidence, 3),
                    _clean(row.get("geographic_validation")),
                    "" if distance is None else round(distance, 1),
                ] + ([_clean(row.get("mazal_id")), _clean(row.get("mazal_name"))]
                     if _clean(row.get("mazal_id")) else [])
            )
            geometry.append([])
            counts[status] = counts.get(status, 0) + 1

        if has_geometry:
            for line in _geometry_lines(row.get("geometry")):
                geometry[rec_id].append([(lat, lon) for lon, lat, *_ in line.coords])
    return records, geometry, counts


def create_html_from_gdf(
    gdf: gpd.GeoDataFrame,
    place_name: str,
    diagnostics: dict = None,
    provenance: dict | None = None,
):
    """Render one settlement page to ``HTML/<name>_roads.html``.

    ``provenance`` describes the OSM/LAMAS inputs (see ``provenance.py``); the
    page creation time is stamped here, when the file is written.
    """
    place_name = display_place_name(place_name)
    if gdf.empty:
        print("Input GeoDataFrame is empty. No HTML will be generated.")
        return

    records, geometry, counts = build_records(gdf)
    diagnostics = diagnostics or {}
    total_osm = diagnostics.get("total_osm_streets") or 0
    total_matched = diagnostics.get("total_matched") or 0
    match_pct = round(100.0 * total_matched / total_osm, 1) if total_osm else 0.0

    page_provenance_block = page_provenance(provenance)
    has_geometry = _has_geometry(gdf)
    meta = _page_meta(
        _with_crs(gdf) if has_geometry else gdf.iloc[0:0],
        place_name,
        diagnostics,
        counts,
        match_pct,
        page_provenance_block,
    )
    meta["total_segments"] = int(len(gdf))
    html_content = render_page(
        place=place_name,
        meta=meta,
        records=records,
        geometry=geometry,
        missing_official=list(diagnostics.get("unmatched_lamas_street_names") or []),
        missing_official_total=diagnostics.get("unmatched_lamas_count"),
        geometry_note=GEOMETRY_NOTE if has_geometry else "",
        provenance=page_provenance_block,
    )

    safe_name = _safe_place_name(place_name)
    output_dir = "HTML"
    os.makedirs(output_dir, exist_ok=True)
    output_filename = f"{output_dir}/{safe_name}_roads.html"
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Successfully generated HTML file: {output_filename}")
    except IOError as e:
        print(f"Error writing HTML file: {e}")
    return output_filename


def _previous_page_context(path: str) -> tuple[dict, dict | None, list]:
    """Diagnostics, source provenance and unmatched LAMAS names of an existing page.

    ``generate_html.py <name>`` only has the diagnostic CSV; reusing what the
    last pipeline run recorded keeps totals and source dates instead of
    showing them as unknown.
    """
    if not os.path.exists(path):
        return {}, None, []
    raw = open(path, encoding="utf-8").read()

    def block(name):
        match = re.search(rf'<script id="{name}" type="application/json">(.*?)</script>', raw, re.S)
        try:
            return json.loads(match.group(1)) if match else {}
        except ValueError:
            return {}

    meta, room = block("page-meta"), block("room-data")
    diagnostics = dict(meta.get("diagnostics") or {})
    sources = {k: v for k, v in (meta.get("provenance") or {}).items() if k in ("osm", "lamas")}
    missing = list(room.get("missingOfficial") or [])
    if missing:
        diagnostics["unmatched_lamas_street_names"] = missing
    return diagnostics, sources or None, missing


def main():
    if len(sys.argv) < 2:
        print('Usage: python generate_html.py "<City Name>"')
        sys.exit(1)
    place_name = sys.argv[1]
    print(f"--- Generating settlement page for {place_name} ---")
    try:
        safe_name = _safe_place_name(place_name)
        report_path = f"data/diagnostic_report_{safe_name}.csv"
        if not os.path.exists(report_path):
            print(f"Diagnostic report not found at {report_path}")
            print(f'Please run the pipeline first: python pipeline.py "{place_name}"')
            return
        print(f"Loading diagnostic data from {report_path}")
        try:
            df = pd.read_csv(report_path, encoding="utf-8")
            from shapely import wkt

            if "geometry" in df.columns and df["geometry"].notna().any():
                df["geometry"] = df["geometry"].apply(
                    lambda value: wkt.loads(value) if isinstance(value, str) else None
                )
                gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            else:
                gdf = df
        except Exception as e:
            print(f"Error reading geometry from CSV: {e}")
            gdf = pd.read_csv(report_path, encoding="utf-8")
        if not _has_geometry(gdf):
            osm_cache = f"data/osm_data_{safe_name}.pkl"
            if os.path.exists(osm_cache) and "osm_id" in gdf.columns:
                osm_gdf = pd.read_pickle(osm_cache)
                gdf = osm_gdf.merge(gdf, on="osm_id", how="inner", suffixes=("", "_diag"))
            else:
                print("No geometry available; the page will have a table without a map.")
        diagnostics, sources, _ = _previous_page_context(f"HTML/{safe_name}_roads.html")
        if not diagnostics:
            print("No previous page found; totals will be computed from the report only.")
        create_html_from_gdf(gdf, place_name, diagnostics or None, provenance=sources)
    except Exception as e:
        print(f"Error generating HTML: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
