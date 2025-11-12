import os
import geopandas as gpd
import pandas as pd
import re
import json

from pipeline import _safe_place_name

def create_html_from_gdf(gdf: gpd.GeoDataFrame, place_name: str):
    """
    Generates an HTML representation of a GeoDataFrame of roads using Leaflet.js.

    Args:
        gdf: A GeoDataFrame containing road geometries ('geometry'), names ('osm_name'),
             match scores ('best_score'), and LAMAS IDs ('final_LAMAS_id').
        place_name: The name of the place, used for the output filename.
    """
    if gdf.empty:
        print("Input GeoDataFrame is empty. No HTML will be generated.")
        return

    # Reproject to WGS84 (EPSG:4326) for Leaflet
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Get the total bounds of the geometries to center the map
    minx, miny, maxx, maxy = gdf.total_bounds
    center_lat = (miny + maxy) / 2
    center_lon = (minx + maxx) / 2

    # --- Prepare GeoJSON data ---
    features = []
    for _, row in gdf.iterrows():
        geom = row['geometry']
        
        # Ensure geometry is valid and not empty
        if geom is None or geom.is_empty:
            continue

        # Convert geometry to GeoJSON format
        geojson_geom = json.loads(gpd.GeoSeries([geom]).to_json())['features'][0]['geometry']
        
        # Get properties
        name = row.get('osm_name', 'N/A')
        score = row.get('best_score', 0)
        lamas_id = row.get('final_LAMAS_id', 'None')
        
        # Sanitize name for display
        safe_name = re.sub(r'[<&">]', '', str(name)) if pd.notna(name) else 'No Name'

        # Determine color based on matching score
        if pd.isna(name) or name == '':
            color = 'gray'
        elif pd.notna(lamas_id) and str(lamas_id).lower() != 'none':
            if score >= 95:
                color = 'black'
            else:
                # Gradient from yellow (low score) to red (high score)
                # score is 0-100, hue is 0-60 (red to yellow)
                hue = 60 - (score / 100) * 60
                color = f'hsl({hue}, 100%, 50%)'
        else:
            color = 'red' # Not matched

        features.append({
            "type": "Feature",
            "geometry": geojson_geom,
            "properties": {
                "name": safe_name,
                "score": score,
                "lamas_id": lamas_id,
                "color": color
            }
        })

    geojson_data = {"type": "FeatureCollection", "features": features}

    # --- HTML Template ---
    html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Roads of {place_name}</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        #map {{ height: 100vh; }}
        .leaflet-popup-content-wrapper {{
            background-color: #f8f9fa;
            border-radius: 5px;
        }}
        .leaflet-popup-content {{
            font-family: Arial, sans-serif;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        var map = L.map('map').setView([{center_lat}, {center_lon}], 13);

        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }}).addTo(map);

        var geojsonData = {json.dumps(geojson_data, indent=2)};

        L.geoJSON(geojsonData, {{
            style: function(feature) {{
                return {{
                    color: feature.properties.color,
                    weight: 4,
                    opacity: 0.8
                }};
            }},
            onEachFeature: function(feature, layer) {{
                if (feature.properties) {{
                    var popupContent = `<b>Street Name:</b> {feature.properties.name}<br>` +
                                       `<b>Score:</b> {feature.properties.score || 'N/A'}<br>` +
                                       `<b>LAMAS ID:</b> {feature.properties.lamas_id || 'N/A'}`;
                    layer.bindPopup(popupContent);
                }}
            }}
        }}).addTo(map);
        
        // Fit map to bounds of the GeoJSON data
        var bounds = L.geoJSON(geojsonData).getBounds();
        if (bounds.isValid()) {{
            map.fitBounds(bounds);
        }}
    </script>
</body>
</html>
    """

    # --- Write to file ---
    safe_name = _safe_place_name(place_name)
    output_filename = f"HTML/{safe_name}_roads.html"
    os.makedirs("HTML", exist_ok=True)
    
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(html_template)
        print(f"Successfully generated HTML file: {output_filename}")
    except IOError as e:
        print(f"Error writing HTML file: {e}")

def main():
    """
    Main function to run the HTML generation for testing.
    Expects a city name as a command-line argument.
    """
    if len(sys.argv) < 2:
        print("Usage: python generate_html.py \"<City Name>\"")
        sys.exit(1)
        
    place_name = sys.argv[1]
    print(f"--- Generating HTML for {place_name} ---")

    try:
        # This is for standalone testing, so we need to load the processed data
        from pipeline import load_or_fetch_osm, run_pipeline
        
        # We need the final processed GDF, so we might need to run the pipeline
        # For now, let's assume the final output from the pipeline is available
        # This part might need adjustment based on where the final GDF is stored
        
        # A better approach for testing would be to load a cached final GDF
        # For now, we'll just demonstrate by loading the raw OSM data
        osm_gdf = load_or_fetch_osm(place_name)
        
        if osm_gdf is None or osm_gdf.empty: 
            print(f"Could not fetch or load data for '{place_name}'. Aborting.")
            return

        # In a real scenario, you'd pass the final GDF from the pipeline here
        # For this test, we'll add dummy columns for demonstration
        if 'best_score' not in osm_gdf.columns:
            osm_gdf['best_score'] = 75 # dummy score
        if 'final_LAMAS_id' not in osm_gdf.columns:
            osm_gdf['final_LAMAS_id'] = '12345' # dummy ID

        create_html_from_gdf(osm_gdf, place_name)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()