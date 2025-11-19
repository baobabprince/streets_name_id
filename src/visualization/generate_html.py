
import os
import sys
import geopandas as gpd
from shapely.geometry import LineString
import pandas as pd
import re
import colorsys

# Add the src directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# --- Import from existing project files ---
from src.utils.caching import _safe_place_name


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
            
            # Create path element with data attributes for interactivity
            path_element = (
                f'<path d="{path_data}" '
                f'stroke="{stroke_color}" '
                f'stroke-width="{stroke_width}" '
                f'stroke-dasharray="{stroke_dasharray}" '
                f'fill="none" '
                f'class="street-path" '
                f'data-tooltip="{safe_tooltip}">'
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
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
            direction: rtl;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            text-align: center;
            color: #333;
            margin-bottom: 10px;
        }}
        
        .legend {{
            margin: 20px 0;
            padding: 15px;
            background: #f9f9f9;
            border-radius: 5px;
            text-align: center;
        }}
        
        .legend-title {{
            font-weight: bold;
            margin-bottom: 10px;
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
        }}
        
        .legend-color {{
            width: 30px;
            height: 4px;
        }}
        
        svg {{
            width: 100%;
            height: auto;
            border: 1px solid #ddd;
            background: #f0f0f0;
        }}
        
        .street-path {{
            cursor: pointer;
            transition: stroke-width 0.2s, filter 0.2s;
        }}
        
        .street-path:hover {{
            stroke-width: 10 !important;
            filter: drop-shadow(0 0 3px rgba(0,0,0,0.5));
        }}
        
        .street-path.pinned {{
            stroke-width: 8 !important;
            filter: drop-shadow(0 0 5px rgba(255,215,0,0.8));
        }}
        
        #tooltip {{
            position: fixed;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 12px 18px;
            border-radius: 6px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s;
            z-index: 1000;
            white-space: pre-line;
            font-size: 15px;
            max-width: 350px;
            direction: rtl;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }}
        
        #tooltip.visible {{
            opacity: 1;
        }}
        
        .stats {{
            margin-top: 20px;
            padding: 15px;
            background: #e8f4f8;
            border-radius: 5px;
            text-align: center;
        }}
        .diagnostics {{
            margin-top: 20px;
            padding: 15px;
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
        }}
        .diagnostics h2 {{
            text-align: center;
            color: #333;
            margin-top: 0;
        }}
        .diagnostics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}
        .diagnostics-card {{
            background: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
        }}
        .diagnostics-card .label {{
            font-weight: bold;
            color: #555;
        }}
        .diagnostics-card .value {{
            font-size: 1.5em;
            color: #0056b3;
        }}
        .unmatched-list {{
            margin-top: 20px;
            padding: 15px;
            background: #fff8f8;
            border: 1px solid #f2dede;
            border-radius: 5px;
        }}
        .unmatched-list h3 {{
            margin-top: 0;
            color: #a94442;
        }}
        .unmatched-list ul {{
            columns: 3;
            padding-right: 20px;
            list-style-type: none;
        }}
        .unmatched-list li {{
            margin-bottom: 5px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>מפת רחובות - {place_name}</h1>
        
        <div class="legend">
            <div class="legend-title">מקרא צבעים</div>
            <div class="legend-items">
                <div class="legend-item">
                    <span class="legend-color" style="background: #000000;"></span>
                    <span>התאמה מלאה (100)</span>
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: linear-gradient(to left, #c06666, #ffcccc);"></span>
                    <span>התאמה חלקית (0-99)</span>
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #00BFFF;"></span>
                    <span>התאמה ע"י AI</span>
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #FF8C00;"></span>
                    <span>שם קיים, ללא התאמה</span>
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: repeating-linear-gradient(to right, #CCCCCC 0px, #CCCCCC 5px, transparent 5px, transparent 10px);"></span>
                    <span>ללא שם</span>
                </div>
            </div>
        </div>
        
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" id="street-map">
            <rect x="0" y="0" width="{width}" height="{height}" fill="#f0f0f0" />
            {chr(10).join(svg_paths)}
        </svg>
        
        <div class="stats">
            <strong>הוראות:</strong> העבר את העכבר מעל רחוב כדי לראות את שמו ופרטי ההתאמה. <strong>לחץ על רחוב</strong> כדי לנעוץ את המידע (שימושי בערים גדולות).
        </div>

        {_build_diagnostics_html(diagnostics) if diagnostics else ""}

    </div>
    
    <div id="tooltip"></div>
    
    <script>
        const tooltip = document.getElementById('tooltip');
        const paths = document.querySelectorAll('.street-path');
        let pinnedPath = null;
        
        paths.forEach(path => {{
            // Click to pin/unpin tooltip
            path.addEventListener('click', (e) => {{
                e.stopPropagation();
                
                if (pinnedPath === path) {{
                    // Unpin current
                    path.classList.remove('pinned');
                    tooltip.classList.remove('visible');
                    pinnedPath = null;
                }} else {{
                    // Unpin previous if exists
                    if (pinnedPath) {{
                        pinnedPath.classList.remove('pinned');
                    }}
                    
                    // Pin new
                    path.classList.add('pinned');
                    pinnedPath = path;
                    const tooltipText = path.getAttribute('data-tooltip');
                    tooltip.textContent = tooltipText;
                    tooltip.classList.add('visible');
                    tooltip.style.left = (e.clientX + 15) + 'px';
                    tooltip.style.top = (e.clientY + 15) + 'px';
                }}
            }});
            
            // Hover to show tooltip (only if not pinned)
            path.addEventListener('mouseenter', (e) => {{
                if (pinnedPath !== path) {{
                    const tooltipText = path.getAttribute('data-tooltip');
                    tooltip.textContent = tooltipText;
                    tooltip.classList.add('visible');
                }}
            }});
            
            path.addEventListener('mousemove', (e) => {{
                if (pinnedPath !== path) {{
                    tooltip.style.left = (e.clientX + 15) + 'px';
                    tooltip.style.top = (e.clientY + 15) + 'px';
                }}
            }});
            
            path.addEventListener('mouseleave', () => {{
                if (pinnedPath !== path) {{
                    tooltip.classList.remove('visible');
                }}
            }});
        }});
        
        // Click anywhere else to unpin
        document.addEventListener('click', () => {{
            if (pinnedPath) {{
                pinnedPath.classList.remove('pinned');
                tooltip.classList.remove('visible');
                pinnedPath = null;
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
            osm_path = f"data/osm_data_{safe_name}.pkl"
            if not os.path.exists(osm_path):
                print(f"OSM data not found at {osm_path} and geometry not in report. Aborting.")
                return

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
