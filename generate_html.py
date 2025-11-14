
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
    
    return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"


def create_html_from_gdf(gdf: gpd.GeoDataFrame, place_name: str):
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

        if not osm_name:  # Street has no name
            stroke_color = "#808080"  # Gray
        else:  # Street has a name
            if is_matched:
                if pd.notna(best_score):
                    if best_score >= 100:
                        stroke_color = "#000000"  # Black for perfect match
                    else:
                        stroke_color = score_to_color(best_score)  # Red gradient
                else:
                    stroke_color = "#000000"  # Matched but no score -> Black
            else:
                # Has a name, but is not matched
                stroke_color = "#AAAAAA"  # A different gray
        
        if isinstance(geom, LineString):
            # Convert LineString to SVG path data
            path_data = "M " + " L ".join(f"{x - minx},{maxy - y}" for x, y in geom.coords)
            
            # Build tooltip content
            tooltip_lines = []
            matched_osm_name = row.get('matched_osm_name', osm_name)

            # Line 1: OSM Name -> LAMAS Name (if different)
            if matched_osm_name:
                tooltip_lines.append(f"OSM: {matched_osm_name}")
            else:
                tooltip_lines.append("רחוב ללא שם")

            if is_matched:
                if lamas_name and lamas_name.strip():
                     tooltip_lines.append(f"למ\"ס: {lamas_name}")

                # Line 2: Score and Source
                source = "AI" if status == 'NEEDS_AI' else "Fuzzy"
                if pd.notna(best_score):
                    tooltip_lines.append(f"ציון: {best_score:.1f} ({source})")

                # Line 3: Final LAMAS ID
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
            transition: stroke-width 0.2s;
        }}
        
        .street-path:hover {{
            stroke-width: 6 !important;
            filter: brightness(1.2);
        }}
        
        #tooltip {{
            position: fixed;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 10px 15px;
            border-radius: 5px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s;
            z-index: 1000;
            white-space: pre-line;
            font-size: 14px;
            max-width: 300px;
            direction: rtl;
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
                    <span class="legend-color" style="background: #AAAAAA;"></span>
                    <span>שם קיים, ללא התאמה</span>
                </div>
                <div class="legend-item">
                    <span class="legend-color" style="background: #808080;"></span>
                    <span>ללא שם</span>
                </div>
            </div>
        </div>
        
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" id="street-map">
            <rect x="0" y="0" width="{width}" height="{height}" fill="#f0f0f0" />
            {chr(10).join(svg_paths)}
        </svg>
        
        <div class="stats">
            <strong>הוראות:</strong> העבר את העכבר מעל רחוב כדי לראות את שמו ופרטי ההתאמה
        </div>
    </div>
    
    <div id="tooltip"></div>
    
    <script>
        const tooltip = document.getElementById('tooltip');
        const paths = document.querySelectorAll('.street-path');
        
        paths.forEach(path => {{
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
