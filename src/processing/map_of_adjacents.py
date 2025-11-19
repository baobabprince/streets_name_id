# פונקציית build_adjacency_map כפי שהוגדרה קודם (שכפולה לצורך שלמות הקוד)
def build_adjacency_map(osm_gdf):
    """
    בונים מפה (מילון) של קשרים משיקים (Adjacent) בתוך רשת ה-OSM.
    """
    if osm_gdf.empty:
        return {}
        
    # המרת גיאומטריה לנקודות התחלה וסוף
    osm_gdf['start_point'] = osm_gdf.geometry.apply(lambda geom: geom.coords[0])
    osm_gdf['end_point'] = osm_gdf.geometry.apply(lambda geom: geom.coords[-1])
    
    adjacency_map = {}
    endpoint_index = {}

    # בניית אינדקס: מיפוי כל נקודת קצה לרשימת ה-osm_id-ים שעוברים בה
    for index, row in osm_gdf.iterrows():
        osm_id = row['osm_id']
        # הפיכת הנקודות למחרוזת או tuple כדי לשמש כמפתח (Key) במילון
        start_key = tuple(row['start_point'])
        end_key = tuple(row['end_point'])
        
        endpoint_index.setdefault(start_key, []).append(osm_id)
        endpoint_index.setdefault(end_key, []).append(osm_id)

    # מילוי מפת הקשרים: חיפוש רחובות החולקים נקודת קצה
    for index, row in osm_gdf.iterrows():
        osm_id = row['osm_id']
        adjacents = set()
        
        start_key = tuple(row['start_point'])
        end_key = tuple(row['end_point'])
        
        # איסוף משיקים מנקודת ההתחלה והסוף
        for connected_id in endpoint_index.get(start_key, []):
            if connected_id != osm_id:
                adjacents.add(connected_id)
                
        for connected_id in endpoint_index.get(end_key, []):
            if connected_id != osm_id:
                adjacents.add(connected_id)
                
        adjacency_map[osm_id] = list(adjacents)
        
    return adjacency_map

if __name__ == "__main__":
    try:
        map_of_adjacents = build_adjacency_map(osm_gdf)
        print(f"\nקשרי רחובות משיקים ב-OSM (דוגמה): {list(map_of_adjacents.items())[:2]}")
    except NameError:
        print("osm_gdf לא מוגדר - דילוג על הדגמת בניית מפת סמיכויות.")