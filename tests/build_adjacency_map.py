from shapely.geometry import LineString
import geopandas as gpd
import unittest

# נגדיר מחדש את הפונקציה לצורך בדיקה
def build_adjacency_map(osm_gdf):
    """
    בונים מפה (מילון) של קשרים משיקים (Adjacent) בתוך רשת ה-OSM.
    """
    if osm_gdf.empty:
        return {}
        
    # יש להעתיק את ה-GeoDataFrame כדי לא לשנות את המקור
    temp_gdf = osm_gdf.copy()
    
    # 1. יצירת נקודות התחלה וסוף
    temp_gdf['start_point'] = temp_gdf.geometry.apply(lambda geom: tuple(geom.coords[0]))
    temp_gdf['end_point'] = temp_gdf.geometry.apply(lambda geom: tuple(geom.coords[-1]))
    
    adjacency_map = {}
    endpoint_index = {}

    # 2. בניית אינדקס מהיר של נקודות קצה
    # מיפוי כל נקודת קצה לרשימת ה-osm_id-ים
    for index, row in temp_gdf.iterrows():
        osm_id = row['osm_id']
        start_key = row['start_point']
        end_key = row['end_point']
        
        endpoint_index.setdefault(start_key, []).append(osm_id)
        endpoint_index.setdefault(end_key, []).append(osm_id)

    # 3. מילוי מפת הקשרים
    for index, row in temp_gdf.iterrows():
        osm_id = row['osm_id']
        adjacents = set()
        
        # איסוף משיקים
        for connected_id in endpoint_index.get(row['start_point'], []):
            if connected_id != osm_id: adjacents.add(connected_id)
                
        for connected_id in endpoint_index.get(row['end_point'], []):
            if connected_id != osm_id: adjacents.add(connected_id)
                
        adjacency_map[osm_id] = sorted(list(adjacents)) # מיון לצורך בדיקה עקבית
        
    return adjacency_map


class TestAdjacencyMap(unittest.TestCase):
    
    def setUp(self):
        # בניית רשת בדיקה (כוכב ורחוב מבודד)
        data = {
            # רחובות A, B, C נפגשים בנקודה (0,0) - צומת
            'osm_id': ['A', 'B', 'C', 'D'],
            'geometry': [
                LineString([(1, 0), (0, 0)]),  # A
                LineString([(-1, 0), (0, 0)]), # B
                LineString([(0, 1), (0, 0)]),  # C
                LineString([(10, 10), (11, 10)]) # D - מבודד
            ]
        }
        self.osm_gdf_test = gpd.GeoDataFrame(data, crs="EPSG:4326")

    def test_star_connection(self):
        # A, B, C כולם מחוברים אחד לשני בנקודה (0,0)
        adj_map = build_adjacency_map(self.osm_gdf_test)
        
        # A מחובר ל-B ול-C
        self.assertCountEqual(adj_map['A'], ['B', 'C'])
        
        # B מחובר ל-A ול-C
        self.assertCountEqual(adj_map['B'], ['A', 'C'])
        
        # C מחובר ל-A ול-B
        self.assertCountEqual(adj_map['C'], ['A', 'B'])

    def test_isolated_street(self):
        # רחוב D לא מחובר לאף אחד
        adj_map = build_adjacency_map(self.osm_gdf_test)
        self.assertEqual(adj_map['D'], [])

# # הרצת הבדיקות
# unittest.main(argv=['first-arg-is-ignored'], exit=False)