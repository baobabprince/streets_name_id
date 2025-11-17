import osmnx as ox
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

def fetch_osm_street_data(place):
    """
    מוריד את גרף הרחובות של האזור הנתון וממיר אותו
    ל-GeoDataFrame של קטעי רחובות (Ways/Edges).
    """
    print(f"מתחיל בשליפת גרף הרחובות של {place} מ-OSM...")
    
    # 1. הורדת גרף הרחובות
    # 'drive' מציין שאנו רוצים את רשת הדרכים הניתנת לנסיעה
    try:
        G = ox.graph_from_place(place, network_type="all", simplify=False)
    except Exception as e:
        print(f"שגיאה בשליפת גרף מ-OSM: {e}")
        print("נסה לשנות את place_name לתיבת גבולות (bbox) אם השגיאה נמשכת.")
        return None

    # 2. המרת קטעי הדרך (Edges) ל-GeoDataFrame
    osm_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
    
    # 3. ניקוי ועיבוד ראשוני של ה-GeoDataFrame:
    # שמירת עמודות רלוונטיות, במיוחד שמות הרחובות (name)
    
    # oxmnx משתמש ב-u, v, key כמזהה של כל קטע. ניצור osm_id פשוט לשם נוחות.
    osm_gdf = osm_gdf.reset_index()
    osm_gdf['osm_id'] = osm_gdf.apply(lambda row: f"{row['u']}-{row['v']}-{row['key']}", axis=1)
    
    # נבחר את השדות החיוניים להמשך העבודה
    # We keep all columns that start with 'name' to capture all language variations
    name_columns = [col for col in osm_gdf.columns if col.startswith('name')]
    essential_columns = ['osm_id', 'highway', 'geometry'] + name_columns
    osm_gdf = osm_gdf[essential_columns].copy()
    
    # Re-order to have a consistent 'osm_name' as the primary, if it exists
    if 'name' in osm_gdf.columns:
        osm_gdf.rename(columns={'name': 'osm_name'}, inplace=True)
        # Handle cases where 'osm_name' is a list
        osm_gdf['osm_name'] = osm_gdf['osm_name'].apply(lambda x: x[0] if isinstance(x, list) else x)
    else:
        # If no primary 'name' tag, create the column and fill it with the first available name
        # This ensures the rest of the pipeline has a consistent column to reference
        def get_first_available_name(row):
            for col in name_columns:
                if pd.notna(row[col]):
                    return row[col]
            return None
        osm_gdf['osm_name'] = osm_gdf.apply(get_first_available_name, axis=1)

    # Create the 'all_osm_names' field
    # This field will contain a list of all unique, non-null names for that street
    def combine_names(row):
        names = set()
        for col in name_columns:
            name_val = row.get(col)
            if pd.notna(name_val):
                if isinstance(name_val, list):
                    names.update(name for name in name_val if pd.notna(name))
                else:
                    names.add(name_val)
        return list(names) if names else None

    osm_gdf['all_osm_names'] = osm_gdf.apply(combine_names, axis=1)

    # 4. כיווץ ה-GeoDataFrame לדרכים בלבד (נאבד חלק מהתכונות של הגרף, אך זה מתאים למיפוי)
    print(f"נשלפו {len(osm_gdf)} קטעי דרך מ-OSM.")
    return osm_gdf

def check_place_exists_in_osm(place_name):
    """
    Checks if a given place name is likely to be found in OSM Nominatim.
    Returns True if found, False otherwise.
    """
    try:
        # Geocode the location to see if it's found
        gdf = ox.geocode_to_gdf(place_name)
        if gdf.empty:
            print(f"Warning: Place '{place_name}' not found in OSM. Skipping.")
            return False
        return True
    except Exception as e:
        print(f"Warning: Could not verify '{place_name}' in OSM due to an error: {e}. Skipping.")
        return False

