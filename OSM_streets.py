import logging

import osmnx as ox
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

# ניתן להשתמש בשם יישוב או בתיבת גבולות (bounding box).
# ברירת מחדל מצומצמת כדי להימנע משליפת רשת הדרכים של מדינה שלמה.
place_name = "Tel Aviv-Yafo, Israel"
logger = logging.getLogger(__name__)

def is_hebrew(text):
    """
    בדיקה אם טקסט מכיל תווים עבריים.
    מחזיר True אם יש לפחות תו עברי אחד בטקסט.
    """
    if pd.isna(text) or text is None:
        return False
    text = str(text)
    # טווח תווים עבריים ב-Unicode: U+0590 to U+05FF
    return any('\u0590' <= char <= '\u05FF' for char in text)

def fetch_osm_street_data(place, use_pbf: bool = True):
    """
    מוריד את גרף הרחובות של האזור הנתון וממיר אותו
    ל-GeoDataFrame של קטעי רחובות (Ways/Edges).
    משתמש בקובץ PBF מקומי כברירת מחדל למהירות ואמינות מקסימלית.
    """
    logger.info("Fetching OSM street graph for %s...", place)

    # 0. נסיון שליפה מקובץ PBF מקומי תחילה (ללא תלות ברשת וללא חסימות Overpass)
    if use_pbf:
        try:
            from osm_pbf_loader import fetch_pbf_street_data
            pbf_gdf = fetch_pbf_street_data(place)
            if pbf_gdf is not None and not pbf_gdf.empty:
                logger.info(
                    "Successfully loaded %s road segments from local PBF for %s",
                    len(pbf_gdf),
                    place,
                )
                return pbf_gdf
            logger.info("Local PBF did not return streets for %s; attempting Overpass...", place)
        except Exception as pbf_err:
            logger.warning("Local PBF extraction encountered error (%s); falling back to Overpass", pbf_err)

    # הגדרת התגים הנוספים שאנו רוצים לשלוף, במיוחד 'name:he'
    if "name:he" not in ox.settings.useful_tags_way:
        ox.settings.useful_tags_way.append("name:he")

    # 1. Download and simplify the full street network for the requested place.
    try:
        if isinstance(place, dict) and place.get("osm_id") and place.get("osm_type"):
            osm_prefix = {"node": "N", "way": "W", "relation": "R"}.get(
                str(place["osm_type"]).lower()
            )
            if not osm_prefix:
                raise ValueError(f"Unsupported OSM element type: {place['osm_type']}")
            place_gdf = ox.geocode_to_gdf(
                f"{osm_prefix}{place['osm_id']}", by_osmid=True
            )
            polygon_geometries = place_gdf.loc[
                place_gdf.geometry.geom_type.isin(["Polygon", "MultiPolygon"]),
                "geometry",
            ]
            if not polygon_geometries.empty:
                G = ox.graph_from_polygon(
                    polygon_geometries.iloc[0],
                    network_type="all",
                    simplify=True,
                )
            else:
                G = ox.graph_from_point(
                    center_point=(float(place["lat"]), float(place["lon"])),
                    dist=1500,
                    network_type="all",
                    simplify=True,
                )
        else:
            G = ox.graph_from_place(place, network_type="all", simplify=True)
    except Exception as e:
        logger.exception("Failed to fetch OSM street graph for %s: %s", place, e)
        return None

    # 2. המרת קטעי הדרך (Edges) ל-GeoDataFrame
    osm_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
    
    # 3. ניקוי ועיבוד ראשוני של ה-GeoDataFrame:
    # שמירת עמודות רלוונטיות, במיוחד שמות הרחובות (name)
    
    # oxmnx משתמש ב-u, v, key כמזהה של כל קטע. ניצור osm_id פשוט לשם נוחות.
    osm_gdf = osm_gdf.reset_index()
    osm_gdf['osm_id'] = (
        osm_gdf['u'].astype(str)
        + "-"
        + osm_gdf['v'].astype(str)
        + "-"
        + osm_gdf['key'].astype(str)
    )
    
    # נבחר את השדות החיוניים להמשך העבודה
    if "name" not in osm_gdf.columns:
        osm_gdf["name"] = pd.NA
    if "highway" not in osm_gdf.columns:
        osm_gdf["highway"] = pd.NA
    cols_to_keep = ['osm_id', 'name', 'highway', 'geometry']
    if 'name:he' in osm_gdf.columns:
        cols_to_keep.append('name:he')

    osm_gdf = osm_gdf[cols_to_keep].copy()
    
    # נוודא ששם העמודה הוא 'osm_name' ונוסיף 'city' (שנצטרך לחשב בהמשך)
    osm_gdf = osm_gdf.rename(columns={'name': 'osm_name'})
    
    # ב-OSM, שם הרחוב (name) יכול להיות מחרוזת בודדת או רשימה של שמות.
    # נטפל בכך כדי לקבל מחרוזת אחידה
    osm_gdf['osm_name'] = osm_gdf['osm_name'].apply(
        lambda value: value[0] if isinstance(value, list) and value else value
    )
    if 'name:he' in osm_gdf.columns:
        osm_gdf['name:he'] = osm_gdf['name:he'].apply(
            lambda value: value[0] if isinstance(value, list) and value else value
        )
    
    # 4. עדיפות לשם העברי: אם השם הראשי אינו בעברית ויש name:he, נשתמש בו
    # שמירת השם המקורי לצורך דיאגנוסטיקה
    if 'name:he' in osm_gdf.columns:
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()
        
        has_hebrew_main = osm_gdf['osm_name'].map(is_hebrew)
        valid_hebrew_name = osm_gdf['name:he'].notna() & osm_gdf['name:he'].map(is_hebrew)
        use_hebrew_name = ~has_hebrew_main & valid_hebrew_name
        osm_gdf.loc[use_hebrew_name, 'osm_name'] = osm_gdf.loc[use_hebrew_name, 'name:he']
        logger.info("Preferred Hebrew names for %s road segments", use_hebrew_name.sum())
    else:
        # אם אין name:he, השם המקורי זהה לשם הראשי
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()

    # 4. כיווץ ה-GeoDataFrame לדרכים בלבד (נאבד חלק מהתכונות של הגרף, אך זה מתאים למיפוי)
    logger.info("Fetched %s OSM road segments for %s", len(osm_gdf), place)
    return osm_gdf

# טעינת נתוני OSM (יכול לקחת מספר דקות!)
# osm_gdf = fetch_osm_street_data(place_name)
# if osm_gdf is not None:
#     print("\n--- נתוני OSM ששולפו ---")
#     print(osm_gdf.head())
#     print(f"מערכת קואורדינטות (CRS): {osm_gdf.crs}")


if __name__ == "__main__":
    # ---
    # הדמיה (Mock Data) אם השליפה נכשלת או לצורך בדיקה מהירה:
    # ---
    if 'osm_gdf' not in locals():
        print("יצירת GeoDataFrame מדומה עבור OSM...")
        from shapely.geometry import LineString
        data_osm = {
            'osm_id': ['5001-A', '5002-B', '5003-C', '5004-D'],
            'osm_name': ['שדרות רוטשילד', 'אבא הלל', 'הרצל', 'הרב סילבר'],
            'highway': ['primary', 'residential', 'primary', 'residential'],
            'geometry': [
                LineString([(0, 0), (1, 0)]), 
                LineString([(1, 0), (1, 1)]), 
                LineString([(1, 1), (2, 1)]),
                LineString([(1, 0), (2, 0)]) # קטע חדש המשיק ל-5001-A ו-5002-B
            ]
        }
        osm_gdf = gpd.GeoDataFrame(data_osm, crs="EPSG:4326")
        # נוסיף עמודת עיר כרגע באופן ידני, נצטרך למפות אותה בהמשך
        osm_gdf['city'] = ['תל אביב-יפו', 'רמת גן', 'רמת גן', 'רמת גן']


    # ודא שהנתונים של הלמ"ס נשלפים (מהקוד הקודם)
    # LAMAS_data = fetch_all_LAMAS_data() # (יש להריץ את הפונקציה שהוגדרה קודם)
    if 'LAMAS_df' not in locals():
        print("שימוש בנתוני הלמ\"ס מדומה...")
        data_LAMAS = {
            '_id': [1001, 1002, 1003, 1004],
            'שם_ישוב': ['תל אביב-יפו', 'רמת גן', 'רמת גן', 'רמת גן'],
            'שם_רחוב': ['שד. רוטשילד', 'הרב סילבר', 'הרצל רח\'', 'אבא הלל סילבר'],
        }
        LAMAS_df = pd.DataFrame(data_LAMAS)
        LAMAS_df.rename(columns={'_id': 'LAMAS_id', 'שם_ישוב': 'city', 'שם_רחוב': 'LAMAS_name'}, inplace=True)