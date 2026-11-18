
import os
import sys
import geopandas as gpd
from shapely.geometry import LineString, MultiLineString
import pandas as pd
import re
import colorsys
from html import escape
from urllib.parse import quote

from data_loader import safe_place_name as _safe_place_name


def score_to_color(score):
    if pd.isna(score):
        score = 0
    score = max(0, min(float(score), 99))
    lightness = 0.7 - (score / 99.0) * 0.3
    r, g, b = colorsys.hls_to_rgb(0, lightness, 0.9)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"


def _build_diagnostics_html(diagnostics: dict, place_name: str) -> str:
    unmatched_lamas_html = ""
    if diagnostics["unmatched_lamas_street_names"]:
        street_list_items = "".join(
            _govmap_link(name, place_name)
            for name in diagnostics["unmatched_lamas_street_names"]
        )
        unmatched_lamas_html = f"""
        <div class="unmatched-list">
            <h3>רחובות למ"ס ללא התאמה ({diagnostics['unmatched_lamas_count']} - {diagnostics['unmatched_lamas_percentage']})</h3>
            <ul>{street_list_items}</ul>
        </div>
        """

    unmatched_osm_html = ""
    if diagnostics.get("unmatched_osm_street_names"):
        street_list_items = "".join(f"<li>{name}</li>" for name in diagnostics["unmatched_osm_street_names"])
        unmatched_osm_html = f"""
        <div class="unmatched-list">
            <h3>רחובות OSM ללא התאמה מאושרת ({diagnostics['unmatched_osm_streets']})</h3>
            <ul>{street_list_items}</ul>
        </div>
        """

    return f"""
    <div class="diagnostics">
        <h2>סיכום דיאגנוסטיקה</h2>
        <div class="diagnostics-grid">
            <div class="diagnostics-card">
                <div class="label">סה"כ רחובות OSM</div>
                <div class="value">{diagnostics.get('total_osm_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">סה"כ רחובות למ"ס</div>
                <div class="value">{diagnostics.get('total_lamas_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">התאמות ודאיות</div>
                <div class="value">{diagnostics.get('confident_matches', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">הצעות AI (לא מאושרות)</div>
                <div class="value">{diagnostics.get('ai_proposed_matches', diagnostics.get('ai_resolved_matches', 'N/A'))}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">סה"כ התאמות מאושרות</div>
                <div class="value">{diagnostics.get('total_matched', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">רחובות ללא התאמה מאושרת</div>
                <div class="value">{diagnostics.get('unmatched_osm_streets', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">נדרשת בדיקה גאוגרפית</div>
                <div class="value">{diagnostics.get('geographic_review_matches', 'N/A')}</div>
            </div>
        </div>
        {unmatched_osm_html}
        {unmatched_lamas_html}
    </div>
    """


def _govmap_link(street_name: str, place_name: str) -> str:
    query = quote(f"{street_name}, {place_name}", safe="")
    href = escape(f"https://www.govmap.gov.il/?q={query}&z=16", quote=True)
    return (
        f'<li><a href="{href}" target="_blank" rel="noopener noreferrer">'
        f'{escape(str(street_name))}</a></li>'
    )


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

    for idx, row in gdf.iterrows():
        geom = row['geometry']
        osm_name_raw = row.get('osm_name', None)
        osm_name = str(osm_name_raw).strip() if pd.notna(osm_name_raw) else ""

        is_matched = (
            pd.notna(row.get("final_LAMAS_id"))
            and str(row.get("final_LAMAS_id")).lower() != "none"
        )
        best_score = row.get('best_score', None)
        mapping_status = row.get('mapping_status', '')
        lamas_name = row.get('best_LAMAS_name', '')
        final_lamas_id = row.get('final_LAMAS_id', '')
        ai_suggestion = row.get('ai_suggested_LAMAS_id', None)

        stroke_width = 4
        stroke_dasharray = "none"

        if not osm_name:
            stroke_color = "#CCCCCC"
            stroke_dasharray = "5,5"
            stroke_width = 2
            data_status = "unnamed"
        else:
            if mapping_status == "GEOGRAPHIC_REVIEW":
                stroke_color = "#c084fc"
                stroke_width = 5
                stroke_dasharray = "8,4"
                data_status = "geo-review"
            elif mapping_status == "AI_PROPOSED":
                stroke_color = "#00BFFF"
                stroke_width = 5
                data_status = "ai"
            elif is_matched:
                if pd.notna(best_score):
                    if best_score >= 100:
                        stroke_color = "#f8fafc"
                        stroke_width = 5
                        data_status = "perfect"
                    else:
                        stroke_color = score_to_color(best_score)
                        stroke_width = 5
                        data_status = "partial"
                else:
                    stroke_color = "#f8fafc"
                    stroke_width = 5
                    data_status = "perfect"
            else:
                stroke_color = "#FF8C00"
                stroke_width = 4
                data_status = "unmatched"

        if geom is None or geom.is_empty:
            continue
        if isinstance(geom, LineString):
            line_geometries = [geom]
        elif isinstance(geom, MultiLineString):
            line_geometries = geom.geoms
        else:
            continue

        tooltip_lines = []
        if osm_name:
            tooltip_lines.append(f"{osm_name}")
        else:
            tooltip_lines.append("רחוב ללא שם")
        if is_matched:
            if (
                isinstance(lamas_name, str)
                and lamas_name.strip()
                and lamas_name.strip() != osm_name
            ):
                tooltip_lines[0] += f" -> {lamas_name}"
            if pd.notna(best_score):
                tooltip_lines.append(f"ציון: {best_score:.1f}")
            if final_lamas_id:
                tooltip_lines.append(f"מזהה: {final_lamas_id}")
                tooltip_lines.append("התאמה מאושרת")
        elif pd.notna(ai_suggestion):
            tooltip_lines.append(f"הצעת AI (לא מאושרת): {ai_suggestion}")
            confidence = row.get("ai_confidence")
            if pd.notna(confidence):
                tooltip_lines.append(f"ביטחון המודל: {float(confidence):.2f}")
            if mapping_status == "GEOGRAPHIC_REVIEW":
                tooltip_lines.append("נדרשת בדיקה גאוגרפית")
        elif mapping_status == "GEOGRAPHIC_REVIEW":
            tooltip_lines.append("נדרשת בדיקה גאוגרפית לפני אישור")
        elif osm_name:
            tooltip_lines.append("סטטוס: ללא התאמה מאושרת")

        geographic_validation = row.get("geographic_validation")
        if pd.notna(geographic_validation):
            tooltip_lines.append(f"אימות גאוגרפי: {geographic_validation}")
        geographic_distance = row.get("geographic_distance_m")
        if pd.notna(geographic_distance):
            tooltip_lines.append(
                f"מרחק מעוגן התאמה מדויקת: {float(geographic_distance):.0f} מ׳"
            )

        tooltip = "&#10;".join(tooltip_lines)
        safe_tooltip = re.sub(r'[<>&"]', '', tooltip)
        safe_name_attr = re.sub(r'[<>&"]', '', osm_name).lower()

        for line_geometry in line_geometries:
            path_data = "M " + " L ".join(
                f"{x - minx},{maxy - y}" for x, y in line_geometry.coords
            )
            path_element = (
                f'<path d="{path_data}" '
                f'stroke="{stroke_color}" '
                f'stroke-width="{stroke_width}" '
                f'stroke-dasharray="{stroke_dasharray}" '
                f'vector-effect="non-scaling-stroke" '
                f'stroke-linecap="round" '
                f'stroke-linejoin="round" '
                f'fill="none" '
                f'class="street-path" '
                f'data-tooltip="{safe_tooltip}" '
                f'data-status="{data_status}" '
                f'data-name="{safe_name_attr}">'
                f'</path>'
            )
            svg_paths.append(path_element)

    coverage_html = ""
    match_pct = 0
    if diagnostics:
        total_osm = diagnostics.get("total_osm_streets") or 0
        total_matched = diagnostics.get("total_matched") or 0
        match_pct = round(100.0 * total_matched / total_osm, 1) if total_osm else 0
        coverage_html = f"""
        <div class="coverage"><div class="coverage-bar" style="width:{match_pct}%"></div></div>
        <div class="coverage-label">כיסוי התאמות מאושרות: {match_pct}% ({total_matched}/{total_osm})</div>
        """

    template_path = os.path.join(os.path.dirname(__file__), "map_template.html")
    with open(template_path, encoding="utf-8") as tf:
        template = tf.read()
    html_content = (
        template
        .replace("@@PLACE_NAME@@", str(place_name))
        .replace("@@WIDTH@@", str(width))
        .replace("@@HEIGHT@@", str(height))
        .replace("@@SVG_PATHS@@", "\n".join(svg_paths))
        .replace("@@DIAGNOSTICS@@", _build_diagnostics_html(diagnostics, place_name) if diagnostics else "")
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
        print("Usage: python generate_html.py \"<City Name>\"")
        sys.exit(1)
    place_name = sys.argv[1]
    print(f"--- Generating Interactive HTML Map for {place_name} ---")
    try:
        safe_name = _safe_place_name(place_name)
        report_path = f"data/diagnostic_report_{safe_name}.csv"
        if not os.path.exists(report_path):
            print(f"Diagnostic report not found at {report_path}")
            print(f"Please run the pipeline first: python pipeline.py \"{place_name}\"")
            return
        print(f"Loading diagnostic data from {report_path}")
        try:
            df = pd.read_csv(report_path, encoding='utf-8')
            from shapely import wkt
            if 'geometry' in df.columns and df['geometry'].notna().any():
                df['geometry'] = df['geometry'].apply(wkt.loads)
                gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")
            else:
                gdf = df
        except Exception as e:
            print(f"Error reading geometry from CSV: {e}")
            gdf = pd.read_csv(report_path, encoding='utf-8')
        if 'geometry' not in gdf.columns or not isinstance(gdf, gpd.GeoDataFrame):
            osm_cache = f"data/osm_data_{safe_name}.pkl"
            if os.path.exists(osm_cache):
                osm_gdf = pd.read_pickle(osm_cache)
                gdf = osm_gdf.merge(gdf, on='osm_id', how='inner', suffixes=('', '_diag'))
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
