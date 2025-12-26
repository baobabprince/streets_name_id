
import os
import sys
import geopandas as gpd
from shapely.geometry import LineString
import pandas as pd
import re
import colorsys

# --- Import from existing project files ---
from pipeline import _safe_place_name


def score_to_color(score):
    """
    Convert a confidence score (0-99) to a gradient of red.
    - A lower score results in a lighter red.
    - A higher score results in a darker red.
    
    Returns RGB hex color string.
    """
    if pd.isna(score):
        score = 0  # Default score for missing values
    
    score = max(0, min(float(score), 99)) # Clamp score between 0 and 99
    
    # Map score (0-99) to lightness (0.7 -> 0.4)
    # Lower score = higher lightness (brighter color)
    lightness = 0.7 - (score / 99.0) * 0.3

    # Hue for red is 0. Saturation is high for a vivid color.
    r, g, b = colorsys.hls_to_rgb(0, lightness, 0.9)
    
    return f"#{int(r*255):02x}{int(b*255):02x}{int(b*255):02x}"


def _build_diagnostics_html(diagnostics: dict) -> str:
    """Builds the HTML for the diagnostics section."""

    unmatched_lamas_html = ""
    if diagnostics["unmatched_lamas_street_names"]:
        street_list_items = "".join(f"<li>{name}</li>" for name in diagnostics["unmatched_lamas_street_names"])
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
            <h3>רחובות OSM ללא התאמה ({diagnostics['unmatched_osm_streets']})</h3>
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
                <div class="label">התאמות בעזרת AI</div>
                <div class="value">{diagnostics.get('ai_resolved_matches', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">סה"כ התאמות</div>
                <div class="value">{diagnostics.get('total_matched', 'N/A')}</div>
            </div>
            <div class="diagnostics-card">
                <div class="label">רחובות OSM ללא התאמה</div>
                <div class="value">{diagnostics.get('unmatched_osm_streets', 'N/A')}</div>
            </div>
        </div>
        {unmatched_osm_html}
        {unmatched_lamas_html}
    </div>
    """


def create_html_from_gdf(gdf: gpd.GeoDataFrame, place_name: str, diagnostics: dict = None):
    """
    Generates an interactive HTML visualization of a GeoDataFrame of roads.
    Streets are color-coded by confidence score and display tooltips on hover.

    Args:
        gdf: A GeoDataFrame containing road geometries and matching data.
        place_name: The name of the place, used for the output filename.
    """
    if gdf.empty:
        print("Input GeoDataFrame is empty. No HTML will be generated.")
        return

    # Reproject to a suitable projected CRS for visualization if needed
    if gdf.crs and gdf.crs.is_geographic:
        gdf = gdf.to_crs(epsg=3857)  # Web Mercator

    # Get the total bounds of the geometries
    minx, miny, maxx, maxy = gdf.total_bounds
    width = maxx - minx
    height = maxy - miny
    
    # Start building SVG paths
    svg_paths = []
    
    for idx, row in gdf.iterrows():
        geom = row['geometry']
        osm_name_raw = row.get('osm_name', None)
        osm_name = str(osm_name_raw).strip() if pd.notna(osm_name_raw) else ""
        
        # Get matching information
        is_matched = pd.notna(row.get('final_LAMAS_id')) and str(row.get('final_LAMAS_id')).lower() != 'none'
        best_score = row.get('best_score', None)
        status = row.get('status', 'UNKNOWN')
        lamas_name = row.get('best_LAMAS_name', '')
        final_lamas_id = row.get('final_LAMAS_id', '')
        
        # Determine color based on new rules
        stroke_width = 4  # Default stroke width
        stroke_dasharray = "none"  # Default: solid line

        if not osm_name:  # Street has no name
            stroke_color = "#CCCCCC"  # Light gray
            stroke_dasharray = "5,5"  # Dashed line for unnamed streets
            stroke_width = 2  # Thinner for unnamed
        else:  # Street has a name
            if is_matched:
                if status == 'NEEDS_AI':  # AI-resolved match
                    stroke_color = "#00BFFF"  # Deep sky blue for AI matches
                    stroke_width = 5
                elif pd.notna(best_score):
                    if best_score >= 100:
                        stroke_color = "#000000"  # Black for perfect match
                        stroke_width = 5
                    else:
                        stroke_color = score_to_color(best_score)  # Red gradient
                        stroke_width = 5
                else:
                    stroke_color = "#000000"  # Matched but no score -> Black
                    stroke_width = 5
            else:
                # Has a name, but is not matched
                stroke_color = "#FF8C00"  # Dark orange - more visible than gray
                stroke_width = 4
        
        if isinstance(geom, LineString):
            # Convert LineString to SVG path data
            path_data = "M " + " L ".join(f"{x - minx},{maxy - y}" for x, y in geom.coords)
            
            # Build tooltip content
            tooltip_lines = []
            if osm_name:
                tooltip_lines.append(f"{osm_name}")
            else:
                tooltip_lines.append("רחוב ללא שם")

            if is_matched:
                if lamas_name and lamas_name.strip() and lamas_name.strip() != osm_name:
                    tooltip_lines[0] += f" -> {lamas_name}" # Append matched name
                if pd.notna(best_score):
                    tooltip_lines.append(f"ציון: {best_score:.1f}")
                if final_lamas_id:
                    tooltip_lines.append(f"מזהה: {final_lamas_id}")
            elif osm_name: # Has a name but not matched
                tooltip_lines.append("סטטוס: לא נמצאה התאמה")
            
            tooltip = "&#10;".join(tooltip_lines)  # &#10; is newline in HTML
            safe_tooltip = re.sub(r'[<>&"]', '', tooltip)
            
            # Get diagnostics JSON
            diagnostics_json = row.get('diagnostics', '')
            if pd.isna(diagnostics_json):
                diagnostics_json = ''
            
            # Create path element with data attributes for interactivity
            path_element = (
                f'<path d="{path_data}" '
                f'stroke="{stroke_color}" '
                f'stroke-width="{stroke_width}" '
                f'stroke-dasharray="{stroke_dasharray}" '
                f'fill="none" '
                f'class="street-path" '
                f'data-tooltip="{safe_tooltip}" '
                f'data-diagnostics=\'{diagnostics_json}\'>'
                f'</path>'
            )
            
            svg_paths.append(path_element)
    
    # Build complete HTML document
    html_content = f"""<!DOCTYPE html>
<html lang="he">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Street Map - {place_name}</title>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #f0f2f5;
            direction: rtl;
            display: flex;
            flex-direction: column;
            height: 100vh;
            box-sizing: border-box;
        }}
        
        .main-layout {{
            display: flex;
            flex: 1;
            gap: 20px;
            min-height: 0;
        }}

        .container {{
            flex: 3;
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
        }}

        .debug-panel {{
            flex: 1;
            background: white;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            border-right: 4px solid #00BFFF;
        }}

        /* Responsive Adjustments */
        @media (max-width: 768px) {{
            body {{
                height: auto;
                min-height: 100vh;
                padding: 10px;
            }}
            
            .main-layout {{
                flex-direction: column;
                flex: none;
            }}

            .container, .debug-panel {{
                flex: none;
                width: 100%;
                min-height: 400px;
                height: auto;
            }}

            .debug-panel {{
                border-right: none;
                border-top: 4px solid #00BFFF;
            }}

            h1 {{ font-size: 1.5rem; }}
        }}
        
        h1, h2, h3 {{
            color: #1a1a1a;
            margin-top: 0;
        }}

        .debug-panel h2 {{
            border-bottom: 2px solid #eee;
            padding-bottom: 10px;
            margin-bottom: 15px;
            font-size: 1.4em;
        }}
        
        .legend {{
            margin-bottom: 20px;
            padding: 12px;
            background: #f8f9fa;
            border-radius: 8px;
            border: 1px solid #e9ecef;
        }}
        
        .legend-items {{
            display: flex;
            justify-content: center;
            gap: 20px;
            flex-wrap: wrap;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 0.9em;
        }}
        
        .legend-color {{
            width: 24px;
            height: 4px;
            border-radius: 2px;
        }}
        
        svg {{
            width: 100%;
            height: auto;
            border: 1px solid #dee2e6;
            background: #fdfdfd;
            border-radius: 4px;
        }}
        
        .street-path {{
            cursor: pointer;
            transition: stroke-width 0.2s, filter 0.2s;
        }}
        
        .street-path:hover {{
            stroke-width: 12 !important;
            filter: drop-shadow(0 0 4px rgba(0,0,0,0.4));
        }}
        
        .street-path.pinned {{
            stroke-width: 10 !important;
            filter: drop-shadow(0 0 6px rgba(0,191,255,0.6));
            stroke: #00BFFF !important;
        }}
        
        #tooltip {{
            position: fixed;
            background: rgba(33, 37, 41, 0.95);
            color: white;
            padding: 8px 12px;
            border-radius: 4px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.15s;
            z-index: 1000;
            white-space: pre-line;
            font-size: 14px;
            max-width: 300px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.2);
        }}
        
        #tooltip.visible {{
            opacity: 1;
        }}

        /* Debug UI Components */
        .debug-section {{
            margin-bottom: 20px;
            font-size: 0.95em;
        }}
        .debug-section h4 {{
            margin-bottom: 8px;
            color: #495057;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }}
        .debug-card {{
            background: #f8f9fa;
            padding: 12px;
            border-radius: 6px;
            border: 1px solid #e9ecef;
        }}
        .trace-item {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 4px;
        }}
        .trace-label {{ color: #6c757d; }}
        .trace-value {{ font-weight: 600; color: #212529; }}

        .score-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        .score-table th, .score-table td {{
            text-align: right;
            padding: 8px;
            border-bottom: 1px solid #dee2e6;
        }}
        .score-table th {{ color: #6c757d; font-weight: normal; }}
        .score-row.selected {{ background: #e7f5ff; font-weight: bold; }}
        .final-score {{ color: #0056b3; font-weight: bold; }}

        .ai-reasoning {{
            font-style: italic;
            color: #495057;
            background: #fff3cd;
            padding: 10px;
            border-radius: 4px;
            border-right: 3px solid #ffc107;
            margin-top: 10px;
        }}

        .empty-state {{
            color: #adb5bd;
            text-align: center;
            margin-top: 50px;
        }}

        .diagnostics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }}
        .diagnostics-card {{
            background: #fff;
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #e9ecef;
            text-align: center;
        }}
        .diagnostics-card .label {{ font-size: 0.8em; color: #6c757d; }}
        .diagnostics-card .value {{ font-size: 1.3em; font-weight: bold; color: #0d6efd; }}
    </style>
</head>
<body>
    <div class="main-layout">
        <div class="container">
            <h1>מפת רחובות - {place_name}</h1>
            
            <div class="legend">
                <div class="legend-items">
                    <div class="legend-item">
                        <span class="legend-color" style="background: #000000;"></span>
                        <span>התאמה מלאה</span>
                    </div>
                    <div class="legend-item">
                        <span class="legend-color" style="background: #c06666;"></span>
                        <span>התאמה חלקית</span>
                    </div>
                    <div class="legend-item">
                        <span class="legend-color" style="background: #00BFFF;"></span>
                        <span>התאמה ע"י AI</span>
                    </div>
                    <div class="legend-item">
                        <span class="legend-color" style="background: #FF8C00;"></span>
                        <span>ללא התאמה</span>
                    </div>
                    <div class="legend-item">
                        <span class="legend-color" style="background: #CCCCCC;"></span>
                        <span>ללא שם</span>
                    </div>
                </div>
            </div>
            
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" id="street-map">
                <rect x="0" y="0" width="{width}" height="{height}" fill="#fdfdfd" />
                {chr(10).join(svg_paths)}
            </svg>
            
            <div style="margin-top: 15px; font-size: 0.9em; color: #6c757d; text-align: center;">
                העבר עכבר למידע מהיר. <strong>לחץ על רחוב</strong> לפרטים מלאים ודיאגנוסטיקה.
            </div>

            {_build_diagnostics_html(diagnostics) if diagnostics else ""}
        </div>

        <div class="debug-panel" id="debug-panel">
            <h2>פרטי התאמה (Debug)</h2>
            <div id="debug-content">
                <div class="empty-state">לחץ על רחוב במפה כדי לראות את הלוגיקה מאחורי ההתאמה</div>
            </div>
        </div>
    </div>
    
    <div id="tooltip"></div>
    
    <script>
        const tooltip = document.getElementById('tooltip');
        const debugContent = document.getElementById('debug-content');
        const paths = document.querySelectorAll('.street-path');
        let pinnedPath = null;
        
        function updateDebugPanel(path) {{
            const rawDiag = path.getAttribute('data-diagnostics');
            const tooltipText = path.getAttribute('data-tooltip');
            
            if (!rawDiag || rawDiag === 'null' || rawDiag === '') {{
                debugContent.innerHTML = `<div class="debug-card"><strong>${{tooltipText}}</strong><p>אין מידע דיאגנוסטי זמין עבור רחוב זה.</p></div>`;
                return;
            }}

            try {{
                const diag = JSON.parse(rawDiag);
                let html = '';

                // 1. Normalization Trace
                if (diag.normalization) {{
                    html += `
                    <div class="debug-section">
                        <h4>נרמול שמות (Normalization)</h4>
                        <div class="debug-card">
                            <div class="trace-item"><span class="trace-label">מקור (OSM):</span> <span class="trace-value">${{diag.normalization.osm_original || 'N/A'}}</span></div>
                            <div class="trace-item"><span class="trace-label">מנורמל:</span> <span class="trace-value">${{diag.normalization.osm_normalized || 'N/A'}}</span></div>
                        </div>
                    </div>`;
                }}

                // 2. Scoring Breakdown
                if (diag.scoring_breakdown && diag.scoring_breakdown.length > 0) {{
                    html += `
                    <div class="debug-section">
                        <h4>מועמדים וציונים (Fuzzy Scoring)</h4>
                        <table class="score-table">
                            <thead>
                                <tr>
                                    <th>שם למ"ס</th>
                                    <th>ציון</th>
                                    <th>Ratio</th>
                                    <th>Sort</th>
                                    <th>Set</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{diag.scoring_breakdown.map((m, i) => `
                                    <tr class="score-row ${{i === 0 ? 'selected' : ''}}">
                                        <td>${{m.lamas_name}} (ID ${{m.lamas_id}})</td>
                                        <td class="final-score">${{m.final_score}}</td>
                                        <td>${{m.fuzz_ratio}}</td>
                                        <td>${{m.token_sort_ratio}}</td>
                                        <td>${{m.token_set_ratio}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>`;
                }}

                // 3. AI Resolution
                if (diag.ai_resolution) {{
                    html += `
                    <div class="debug-section">
                        <h4>החלטת AI (${{diag.ai_resolution.method || 'Unknown'}})</h4>
                        <div class="debug-card">
                            <div class="trace-item"><span class="trace-label">ID שנבחר:</span> <span class="trace-value">${{diag.ai_resolution.response}}</span></div>
                            <div class="ai-reasoning">
                                <strong>הסבר:</strong> ${{diag.ai_resolution.reasoning || 'לא סופק הסבר'}}
                            </div>
                            <details style="margin-top: 10px; font-size: 0.8em;">
                                <summary>צפה ב-Prompt</summary>
                                <pre style="white-space: pre-wrap; background: #eee; padding: 5px;">${{diag.ai_resolution.prompt}}</pre>
                            </details>
                        </div>
                    </div>`;
                }}

                debugContent.innerHTML = html;
            }} catch (e) {{
                debugContent.innerHTML = `<div class="ai-reasoning">שגיאה בפענוח נתוני דיאגנוסטיקה: ${{e.message}}</div>`;
            }}
        }}

        paths.forEach(path => {{
            path.addEventListener('click', (e) => {{
                e.stopPropagation();
                
                if (pinnedPath === path) {{
                    path.classList.remove('pinned');
                    pinnedPath = null;
                    debugContent.innerHTML = '<div class="empty-state">לחץ על רחוב במפה כדי לראות את הלוגיקה מאחורי ההתאמה</div>';
                }} else {{
                    if (pinnedPath) pinnedPath.classList.remove('pinned');
                    path.classList.add('pinned');
                    pinnedPath = path;
                    updateDebugPanel(path);
                }}
            }});
            
            path.addEventListener('mouseenter', (e) => {{
                const tooltipText = path.getAttribute('data-tooltip');
                tooltip.textContent = tooltipText;
                tooltip.classList.add('visible');
            }});
            
            path.addEventListener('mousemove', (e) => {{
                tooltip.style.left = (e.clientX + 15) + 'px';
                tooltip.style.top = (e.clientY + 15) + 'px';
            }});
            
            path.addEventListener('mouseleave', () => {{
                tooltip.classList.remove('visible');
            }});
        }});
        
        document.addEventListener('click', () => {{
            if (pinnedPath) {{
                pinnedPath.classList.remove('pinned');
                pinnedPath = null;
                debugContent.innerHTML = '<div class="empty-state">לחץ על רחוב במפה כדי לראות את הלוגיקה מאחורי ההתאמה</div>';
            }}
        }});
    </script>
</body>
</html>"""

    # Write to file
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
    """
    Main function to run the HTML generation.
    Expects a city name as a command-line argument.
    """
    if len(sys.argv) < 2:
        print("Usage: python generate_html.py \"<City Name>\"")
        sys.exit(1)
        
    place_name = sys.argv[1]
    print(f"--- Generating Interactive HTML Map for {place_name} ---")

    try:
        safe_name = _safe_place_name(place_name)
        
        # Load the diagnostic report which contains all necessary data
        report_path = f"data/diagnostic_report_{safe_name}.csv"
        if not os.path.exists(report_path):
            print(f"Diagnostic report not found at {report_path}")
            print(f"Please run the pipeline first: python pipeline.py \"{place_name}\"")
            return
        
        print(f"Loading diagnostic data from {report_path}")
        # Use geopandas to read the CSV with geometry
        try:
            # geopandas can read WKT geometries directly from a CSV
            df = pd.read_csv(report_path, encoding='utf-8')
            from shapely import wkt
            # Ensure the geometry column exists and is not empty before converting
            if 'geometry' in df.columns and df['geometry'].notna().any():
                 df['geometry'] = df['geometry'].apply(wkt.loads)
                 gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")
            else:
                # If no geometry, create a DataFrame that will be merged later
                gdf = df

        except Exception as e:
            print(f"Error reading geometry from CSV, will fall back to OSM merge: {e}")
            gdf = pd.read_csv(report_path, encoding='utf-8')

        if 'geometry' not in gdf.columns or gdf.geometry.isnull().all():
             # Fallback for CSVs without embedded geometry: Load OSM data and merge
            print("Geometry not found in report, loading from OSM pickle.")
            
            # Robustly find the OSM pickle file
            import glob
            osm_pattern = f"data/osm_data_{safe_name}*.pkl"
            matching_files = glob.glob(osm_pattern)
            
            if not matching_files:
                print(f"OSM data not found at {osm_pattern} and geometry not in report. Aborting.")
                return

            osm_path = matching_files[0]
            print(f"Loading OSM data from {osm_path}")
            osm_gdf = pd.read_pickle(osm_path)
            diagnostic_df = pd.read_csv(report_path, encoding='utf-8')

            # Ensure osm_id is of a consistent type for merging
            osm_gdf['osm_id'] = osm_gdf['osm_id'].astype(str)
            diagnostic_df['osm_id'] = diagnostic_df['osm_id'].astype(str)

            gdf = osm_gdf.merge(
                diagnostic_df.drop(columns=['geometry'], errors='ignore'),
                on='osm_id',
                how='left'
            )

        if gdf is None or gdf.empty:
            print(f"Could not load or construct data for '{place_name}'. Aborting.")
            return

        # Generate the HTML
        print("Generating HTML...")
        create_html_from_gdf(gdf, place_name)
        print("HTML Generation Complete.")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
