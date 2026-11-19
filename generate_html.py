"""
Interactive SVG map + street inspector for one settlement.

The generated page is intentionally data-driven: every OSM way segment is a
``<path class="street-path">`` carrying its match metadata in ``data-*``
attributes, and a JSON block (``#page-meta``) exposes the settlement summary.
The heavy UI (list, filters, labels, insights, export) lives in
``assets/map.js`` so that hundreds of settlement pages stay small.
"""

import colorsys
import hashlib
import json
import os
import re
import sys
from html import escape
from urllib.parse import quote

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString

from data_loader import safe_place_name as _safe_place_name

# Match-status palette — must stay in sync with assets/map.js
COLOR_PERFECT = "#35d0a1"
COLOR_AI = "#38bdf8"
COLOR_GEO_REVIEW = "#a78bfa"
COLOR_UNMATCHED = "#fb923c"
COLOR_UNNAMED = "#7c8aa3"

MAX_LISTED_NAMES = 500


def score_to_color(score):
    """Amber → green gradient for approved, non-exact matches."""
    if pd.isna(score):
        score = 0
    score = max(0, min(float(score), 99))
    hue = (40 + (score / 99.0) * 118) / 360.0
    lightness = 0.60 - (score / 99.0) * 0.06
    r, g, b = colorsys.hls_to_rgb(hue, lightness, 0.70)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def _clean(value):
    """Return a stripped string, or '' for missing values."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "<na>", "nat"}:
        return ""
    return text


def _attr(value):
    """Escape a value for use inside a double-quoted HTML attribute."""
    return escape(_clean(value), quote=True)


def _score_value(value):
    try:
        return None if pd.isna(value) else float(value)
    except (TypeError, ValueError):
        return None


def _record_key(osm_name, lamas_name, lamas_id, status, ai_id):
    raw = "\u0001".join([osm_name, lamas_name, lamas_id, status, ai_id])
    # Readable prefix + stable hash keeps the DOM attribute short.
    return f"{status[:4]}-{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]}"


def _street_visual(row):
    """Resolve status, colour and stroke styling for a diagnostic row."""
    is_matched = bool(_clean(row.get("final_LAMAS_id")))
    mapping_status = _clean(row.get("mapping_status"))
    best_score = _score_value(row.get("best_score"))

    if mapping_status == "GEOGRAPHIC_REVIEW":
        return "geo-review", COLOR_GEO_REVIEW, 4.6, "8,4"
    if mapping_status == "AI_PROPOSED":
        return "ai", COLOR_AI, 4.6, "none"
    if is_matched:
        if best_score is not None and best_score < 100:
            return "partial", score_to_color(best_score), 4.6, "none"
        return "perfect", COLOR_PERFECT, 4.6, "none"
    return "unmatched", COLOR_UNMATCHED, 4.0, "none"


def _has_approved_match(row) -> bool:
    return bool(_clean(row.get("final_LAMAS_id")))


def _build_diagnostics_html(diagnostics: dict, place_name: str) -> str:
    def _items(names):
        shown = list(names)[:MAX_LISTED_NAMES]
        html = "".join(_govmap_link(name, place_name) for name in shown)
        if len(names) > len(shown):
            html += (
                f'<li class="more-note">+{len(names) - len(shown)} רחובות נוספים '
                f'ביומן הדיאגנוסטיקה</li>'
            )
        return html

    unmatched_lamas_html = ""
    if diagnostics.get("unmatched_lamas_street_names"):
        unmatched_lamas_html = f"""
        <div class="unmatched-list">
            <h3>רחובות למ״ס ללא התאמה ({diagnostics.get('unmatched_lamas_count', '?')} · {diagnostics.get('unmatched_lamas_percentage', '?')})</h3>
            <ul>{_items(diagnostics['unmatched_lamas_street_names'])}</ul>
        </div>
        """

    unmatched_osm_html = ""
    if diagnostics.get("unmatched_osm_street_names"):
        unmatched_osm_html = f"""
        <div class="unmatched-list">
            <h3>רחובות OSM ללא התאמה מאושרת ({diagnostics.get('unmatched_osm_streets', '?')})</h3>
            <ul>{_items(diagnostics['unmatched_osm_street_names'])}</ul>
        </div>
        """

    return f"""
    <div class="diagnostics">
        <h2>סיכום דיאגנוסטיקה מלא</h2>
        <div class="diagnostics-grid">
            <div class="diagnostics-card">
                <div class="label">סה״כ רחובות OSM</div>
                <div class="value metric">{diagnostics.get('total_osm_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">סה״כ רחובות למ״ס</div>
                <div class="value metric">{diagnostics.get('total_lamas_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">התאמות ודאיות</div>
                <div class="value metric">{diagnostics.get('confident_matches', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">הצעות AI (לא מאושרות)</div>
                <div class="value metric">{diagnostics.get('ai_proposed_matches', diagnostics.get('ai_resolved_matches', 'N/A'))}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">סה״כ התאמות מאושרות</div>
                <div class="value metric">{diagnostics.get('total_matched', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">רחובות ללא התאמה מאושרת</div>
                <div class="value metric">{diagnostics.get('unmatched_osm_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">נדרשת בדיקה גאוגרפית</div>
                <div class="value metric">{diagnostics.get('geographic_review_matches', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">רחובות למ״ס שלא מופו</div>
                <div class="value metric">{diagnostics.get('unmatched_lamas_count', 'N/A')}</div>
            </div>
        </div>
        {unmatched_osm_html}
        {unmatched_lamas_html}
    </div>
    """


def _govmap_link(street_name: str, place_name: str) -> str:
    query = quote(f"{street_name}, {place_name}", safe="")
    href = escape(f"https://www.govmap.gov.il/?q={query}&z=13", quote=True)
    return (
        f'<li><a href="{href}" target="_blank" rel="noopener noreferrer">'
        f'{escape(str(street_name))}</a></li>'
    )


def _geometry_lines(geom):
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, LineString):
        return [geom]
    if isinstance(geom, MultiLineString):
        return list(geom.geoms)
    return []


def _page_meta(gdf, place_name, diagnostics, counts, match_pct):
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
    return json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c")


def _records_json(records) -> str:
    return json.dumps(records, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")


def create_html_from_gdf(gdf: gpd.GeoDataFrame, place_name: str, diagnostics: dict = None):
    if gdf.empty:
        print("Input GeoDataFrame is empty. No HTML will be generated.")
        return

    if gdf.crs and gdf.crs.is_geographic:
        gdf = gdf.to_crs(epsg=3857)

    minx, miny, maxx, maxy = gdf.total_bounds
    width = maxx - minx
    height = maxy - miny
    svg_paths = []
    counts = {
        "perfect": 0,
        "partial": 0,
        "ai": 0,
        "geo-review": 0,
        "unmatched": 0,
        "unnamed": 0,
    }
    records = []           # compact per-street metadata shared by all segments
    record_index = {}

    for _, row in gdf.iterrows():
        geom = row.get("geometry")
        osm_name = _clean(row.get("osm_name"))
        lamas_name = _clean(row.get("best_LAMAS_name"))
        final_lamas_id = _clean(row.get("final_LAMAS_id"))
        mapping_status = _clean(row.get("mapping_status"))
        best_score = _score_value(row.get("best_score"))
        ai_suggestion = _clean(row.get("ai_suggested_LAMAS_id"))
        ai_confidence = _score_value(row.get("ai_confidence"))

        if not osm_name:
            status = "unnamed"
            stroke_color = COLOR_UNNAMED
            stroke_width = 2.2
            stroke_dasharray = "5,5"
        else:
            status, stroke_color, stroke_width, stroke_dasharray = _street_visual(row)

        key = _record_key(osm_name, lamas_name, final_lamas_id, status, ai_suggestion)
        rec_id = record_index.get(key)
        if rec_id is None:
            rec_id = len(records)
            record_index[key] = rec_id
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
                    "" if _score_value(row.get("geographic_distance_m")) is None
                    else round(_score_value(row.get("geographic_distance_m")), 1),
                ]
            )
            counts[status] = counts.get(status, 0) + 1

        lines = _geometry_lines(geom)
        if not lines:
            continue

        tooltip_lines = [osm_name or "רחוב ללא שם"]
        if _has_approved_match(row):
            if lamas_name and lamas_name != osm_name:
                tooltip_lines[0] += f" -> {lamas_name}"
            if best_score is not None:
                tooltip_lines.append(f"ציון: {best_score:.1f}")
            if final_lamas_id:
                tooltip_lines.append(f"מזהה: {final_lamas_id}")
                tooltip_lines.append("התאמה מאושרת")
        elif ai_suggestion:
            tooltip_lines.append(f"הצעת AI (לא מאושרת): {ai_suggestion}")
            if ai_confidence is not None:
                tooltip_lines.append(f"ביטחון המודל: {float(ai_confidence):.2f}")
            if mapping_status == "GEOGRAPHIC_REVIEW":
                tooltip_lines.append("נדרשת בדיקה גאוגרפית")
        elif mapping_status == "GEOGRAPHIC_REVIEW":
            tooltip_lines.append("נדרשת בדיקה גאוגרפית לפני אישור")
        elif osm_name:
            tooltip_lines.append("סטטוס: ללא התאמה מאושרת")

        geographic_validation = _clean(row.get("geographic_validation"))
        if geographic_validation:
            tooltip_lines.append(f"אימות גאוגרפי: {geographic_validation}")
        geographic_distance = _score_value(row.get("geographic_distance_m"))
        if geographic_distance is not None:
            tooltip_lines.append(
                f"מרחק מעוגן התאמה מדויקת: {float(geographic_distance):.0f} מ׳"
            )

        tooltip = "\n".join(tooltip_lines)
        safe_tooltip = re.sub(r'[<>&"]', "", tooltip)

        # Tooltips are only embedded for rows that must carry an explicit
        # "not approved" notice (AI proposals / geographic review). Everything
        # else is rendered by assets/map.js from the compact record table.
        tooltip_attr = (
            f'data-tooltip="{safe_tooltip}" '
            if status in ("ai", "geo-review")
            else ""
        )

        for line_geometry in lines:
            path_data = "M " + " L ".join(
                f"{x - minx:.1f},{maxy - y:.1f}" for x, y in line_geometry.coords
            )
            svg_paths.append(
                f'<path d="{path_data}" '
                f'stroke="{stroke_color}" '
                f'stroke-width="{stroke_width}" '
                f'stroke-dasharray="{stroke_dasharray}" '
                f'vector-effect="non-scaling-stroke" '
                f'stroke-linecap="round" '
                f'stroke-linejoin="round" '
                f'fill="none" '
                f'class="street-path" '
                f'data-status="{status}" '
                f'data-rec="{rec_id}" '
                f'{tooltip_attr}>'
                f'</path>'
            )

    match_pct = 0.0
    if diagnostics:
        total_osm = diagnostics.get("total_osm_streets") or 0
        total_matched = diagnostics.get("total_matched") or 0
        match_pct = round(100.0 * total_matched / total_osm, 1) if total_osm else 0
        coverage_html = (
            f'<div class="coverage"><div class="coverage-bar" style="width:{match_pct}%"></div></div>'
            f'<div class="coverage-label">כיסוי התאמות מאושרות: {match_pct}% '
            f'({total_matched}/{total_osm})</div>'
        )
        diag_html = _build_diagnostics_html(diagnostics, place_name)
    else:
        coverage_html = ""
        diag_html = ""

    template_path = os.path.join(os.path.dirname(__file__), "map_template.html")
    with open(template_path, encoding="utf-8") as tf:
        template = tf.read()

    html_content = (
        template
        .replace("@@PLACE_NAME@@", escape(str(place_name)))
        .replace("@@WIDTH@@", f"{width:.1f}")
        .replace("@@HEIGHT@@", f"{height:.1f}")
        .replace("@@SVG_PATHS@@", "\n".join(svg_paths))
        .replace("@@DIAGNOSTICS@@", diag_html)
        .replace("@@PAGE_META@@", _page_meta(gdf, place_name, diagnostics, counts, match_pct))
        .replace("@@RECORDS@@", _records_json(records))
        .replace("@@MATCH_PCT@@", str(match_pct))
        .replace("@@COVERAGE@@", coverage_html)
        .replace("@@TOTAL_MATCHED@@", str(diagnostics.get("total_matched", "?") if diagnostics else "?"))
        .replace("@@TOTAL_OSM@@", str(diagnostics.get("total_osm_streets", "?") if diagnostics else "?"))
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


def main():
    if len(sys.argv) < 2:
        print('Usage: python generate_html.py "<City Name>"')
        sys.exit(1)
    place_name = sys.argv[1]
    print(f"--- Generating Interactive HTML Map for {place_name} ---")
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
                df["geometry"] = df["geometry"].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")
            else:
                gdf = df
        except Exception as e:
            print(f"Error reading geometry from CSV: {e}")
            gdf = pd.read_csv(report_path, encoding="utf-8")
        if "geometry" not in gdf.columns or not isinstance(gdf, gpd.GeoDataFrame):
            osm_cache = f"data/osm_data_{safe_name}.pkl"
            if os.path.exists(osm_cache):
                osm_gdf = pd.read_pickle(osm_cache)
                gdf = osm_gdf.merge(gdf, on="osm_id", how="inner", suffixes=("", "_diag"))
            else:
                print("No geometry available. Cannot generate map.")
                return
        create_html_from_gdf(gdf, place_name)
    except Exception as e:
        print(f"Error generating HTML: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
