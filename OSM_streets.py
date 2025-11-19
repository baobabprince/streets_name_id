import osmnx as ox
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString

# הגדרת אזור העניין (ישראל ופלסטין)
# ניתן להשתמש בשם גיאוגרפי או בתיבת גבולות (bounding box)
# השם הגיאוגרפי "Israel, Palestine" משמש כקירוב נוח
place_name = "Israel, Palestine" 

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

def fetch_osm_street_data(place):
    """
    מוריד את גרף הרחובות של האזור הנתון וממיר אותו
    ל-GeoDataFrame של קטעי רחובות (Ways/Edges).
    """
    print(f"מתחיל בשליפת גרף הרחובות של {place} מ-OSM...")
    
    # הגדרת התגים הנוספים שאנו רוצים לשלוף, במיוחד 'name:he'
    ox.settings.useful_tags_way += ['name:he']

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
    cols_to_keep = ['osm_id', 'name', 'highway', 'geometry']
    if 'name:he' in osm_gdf.columns:
        cols_to_keep.append('name:he')

    osm_gdf = osm_gdf[cols_to_keep].copy()
    
    # נוודא ששם העמודה הוא 'osm_name' ונוסיף 'city' (שנצטרך לחשב בהמשך)
    osm_gdf.rename(columns={'name': 'osm_name'}, inplace=True)
    
    # ב-OSM, שם הרחוב (name) יכול להיות מחרוזת בודדת או רשימה של שמות.
    # נטפל בכך כדי לקבל מחרוזת אחידה
    osm_gdf['osm_name'] = osm_gdf['osm_name'].apply(lambda x: x[0] if isinstance(x, list) else x)
    if 'name:he' in osm_gdf.columns:
        osm_gdf['name:he'] = osm_gdf['name:he'].apply(lambda x: x[0] if isinstance(x, list) else x)
    
    # 4. עדיפות לשם העברי: אם השם הראשי אינו בעברית ויש name:he, נשתמש בו
    # שמירת השם המקורי לצורך דיאגנוסטיקה
    if 'name:he' in osm_gdf.columns:
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()
        
        # עבור כל שורה, אם השם הראשי אינו עברי אך name:he קיים ועברי, נחליף
        for idx, row in osm_gdf.iterrows():
            main_name = row['osm_name']
            hebrew_name = row['name:he']
            
            # אם השם הראשי אינו עברי ויש שם עברי תקין
            if not is_hebrew(main_name) and pd.notna(hebrew_name) and is_hebrew(hebrew_name):
                osm_gdf.at[idx, 'osm_name'] = hebrew_name
                print(f"  → החלפה: '{main_name}' → '{hebrew_name}' (עדיפות לעברית)")
    else:
        # אם אין name:he, השם המקורי זהה לשם הראשי
        osm_gdf['osm_name_original'] = osm_gdf['osm_name'].copy()

    # 4. כיווץ ה-GeoDataFrame לדרכים בלבד (נאבד חלק מהתכונות של הגרף, אך זה מתאים למיפוי)
    print(f"נשלפו {len(osm_gdf)} קטעי דרך מ-OSM.")
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