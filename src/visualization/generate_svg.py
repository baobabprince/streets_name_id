
import os
import sys
import geopandas as gpd
from shapely.geometry import LineString
import pandas as pd
import re

# --- Import from existing project files ---
# This assumes pipeline.py and its dependencies are in the same directory
from pipeline import load_or_fetch_osm, _safe_place_name

def create_svg_from_gdf(gdf: gpd.GeoDataFrame, place_name: str):
    """
    Generates an SVG representation of a GeoDataFrame of roads.

    Args:
        gdf: A GeoDataFrame containing road geometries ('geometry') and names ('osm_name').
        place_name: The name of the place, used for the output filename.
    """
    if gdf.empty:
        print("Input GeoDataFrame is empty. No SVG will be generated.")
        return

    # Reproject to a suitable projected CRS for visualization if needed
    if gdf.crs and gdf.crs.is_geographic:
        gdf = gdf.to_crs(epsg=3857) # Web Mercator is common for visualization

    # Get the total bounds of the geometries
    minx, miny, maxx, maxy = gdf.total_bounds
    width = maxx - minx
    height = maxy - miny
    
    # --- Start SVG ---
    # Flip the y-axis for correct SVG coordinate system
    svg_header = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" width="800">'
    
    svg_elements = [svg_header]
    
    # --- Add a background ---
    svg_elements.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="#f0f0f0" />')

    # --- Iterate over geometries ---
    for _, row in gdf.iterrows():
        geom = row['geometry']
        name = row['osm_name']

        # Determine color
        is_matched = pd.notna(row.get('final_LAMAS_id')) and str(row.get('final_LAMAS_id')).lower() != 'none'
        #is_matched = pd.notna(LAMAS_id) and str(LAMAS_id).lower() != 'none'
        stroke_color = "black" if is_matched else "red"
        stroke_width = "2" if is_matched else "3"
        
        if isinstance(geom, LineString):
            # Convert LineString to SVG path data
            path_data = "M " + " L ".join(f"{x - minx},{maxy - y}" for x, y in geom.coords)
            
            # Update path element with new width variable
            path_element = f'<path d="{path_data}" stroke="{stroke_color}" stroke-width="{stroke_width}" fill="none">'
            
            # Add hover-over title if name exists
            if not pd.isna(name):
                safe_name = re.sub(r'[<&">]', '', name)
                # Include LAMAS ID in title for matched roads
                LAMAS_info = f" | LAMAS ID: {row['final_LAMAS_id']}" if is_matched else ""
                path_element += f'<title>{safe_name}{LAMAS_info}</title>'
            
            path_element += '</path>'
            svg_elements.append(path_element)
    # --- End SVG ---
    svg_elements.append('</svg>')
    
    # --- Write to file ---
    svg_content = "\n".join(svg_elements)
    safe_name = _safe_place_name(place_name)
    output_filename = f"SVG/{safe_name}_roads.svg"
    
    try:
        with open(output_filename, "w", encoding="utf-8") as f:
            f.write(svg_content)
        print(f"Successfully generated SVG file: {output_filename}")
    except IOError as e:
        print(f"Error writing SVG file: {e}")

def main():
    """
    Main function to run the SVG generation pipeline.
    Expects a city name as a command-line argument.
    """
    if len(sys.argv) < 2:
        print("Usage: python generate_svg.py \"<City Name>\"")
        sys.exit(1)
        
    place_name = sys.argv[1]
    print(f"--- Generating SVG for {place_name} ---")

    try:
        # 1. Fetch the data using the function from pipeline.py
        osm_gdf = load_or_fetch_osm(place_name)
        
        if osm_gdf is None or osm_gdf.empty: 
            print(f"Could not fetch or load data for '{place_name}'. Aborting.")
            return

        # 2. Generate the SVG
        create_svg_from_gdf(osm_gdf, place_name)

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
