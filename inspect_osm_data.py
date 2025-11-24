import pandas as pd
import pickle
import geopandas as gpd

file_path = "data/osm_data_חיפה__Israel.pkl"

print(f"Loading {file_path}...")
with open(file_path, 'rb') as f:
    gdf = pickle.load(f)

print(f"Total rows: {len(gdf)}")
print(f"CRS: {gdf.crs}")

# Check Bounding Box
bounds = gdf.total_bounds # [minx, miny, maxx, maxy]
print(f"Bounds: {bounds}")
print(f"Center: ({(bounds[1]+bounds[3])/2:.4f}, {(bounds[0]+bounds[2])/2:.4f})")

# Check City column
if 'city' in gdf.columns:
    print("\nCity distribution:")
    print(gdf['city'].value_counts().head(10))
else:
    print("\nNo 'city' column found.")

# Check unique names
if 'name' in gdf.columns:
    unique_names = gdf['name'].nunique()
    print(f"\nUnique names: {unique_names}")
    print("Sample names:")
    print(gdf['name'].sample(20).tolist())
elif 'osm_name' in gdf.columns:
    unique_names = gdf['osm_name'].nunique()
    print(f"\nUnique names: {unique_names}")
    print("Sample names:")
    print(gdf['osm_name'].sample(20).tolist())
