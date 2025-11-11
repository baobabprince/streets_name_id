import unittest
import geopandas as gpd
from shapely.geometry import LineString, Point

# נניח ש-osm_gdf טעון לאחר קריאה מוצלחת ל-fetch_osm_street_data()

# יצירת דאטה מדומה לצורך הבדיקה אם לא בוצעה שליפה בפועל
if 'osm_gdf' not in locals() or osm_gdf.empty:
    print("יצירת GeoDataFrame מדומה של OSM לצורך בדיקה.")
    osm_data_mock = {
    'osm_id': ['A1', 'B2', 'C3', 'D4'],
    'osm_name': ['שדרות רוטשילד', None, 'רחוב הרצל', 'דרך מנחם בגין'],
    'city': ['תל אביב', 'חיפה', 'ירושלים', 'ירושלים'],
    'geometry': [
        LineString([(0, 0), (1, 0)]), 
        LineString([(1, 1), (2, 1)]), 
        LineString([(10, 10), (11, 10)]),
        LineString([(5, 5), (6, 6)]) # שינוי Point ל-LineString
    ]
}
    osm_gdf = gpd.GeoDataFrame(osm_data_mock, crs="EPSG:4326")


class TestOSMDataQuality(unittest.TestCase):
    
    def test_required_columns_exist(self):
        # 1. בדיקת מבנה: האם העמודות החיוניות קיימות?
        required_cols = {'osm_id', 'osm_name', 'geometry', 'city'}
        self.assertTrue(required_cols.issubset(osm_gdf.columns), 
                        "חסרות עמודות חיוניות בנתוני OSM.")

    def test_geometry_type(self):
        # 2. בדיקת סוג גיאומטריה: וידוא שהכול הוא LineString (קווי רחוב)
        # ה-Query אמור לשלוף רק קווים, אבל נבדוק למקרה שנקודות נכנסו בטעות
        expected_type = 'LineString'
        # נבדוק שפחות מ-1% מהגיאומטריות אינן קו
        non_line_count = osm_gdf[~osm_gdf.geometry.geom_type.isin(['LineString', 'MultiLineString'])].shape[0]
        non_line_percentage = non_line_count / len(osm_gdf)
        self.assertLess(non_line_percentage, 0.01, 
                        f"מעל 1% מהרשומות ב-OSM אינן LineString ({non_line_percentage*100:.2f}%)")

    def test_osm_name_missing_count(self):
        # 3. בדיקת שלמות השם: כמה רחובות חסרי שם ב-OSM?
        # זו רשומה קריטית שדורשת עבודת AI/טופולוגיה
        missing_name_percentage = osm_gdf['osm_name'].isnull().sum() / len(osm_gdf)
        # אם יש יותר מדי רחובות ללא שם (למשל, מעל 40%), כדאי לבדוק את שאילתת השליפה
        self.assertLess(missing_name_percentage, 0.40, 
                        f"מעל 40% מהרחובות ב-OSM חסרי שם ({missing_name_percentage*100:.2f}%), ייתכן שהשאילתה לא שלפה את כל הנתונים.")

# # הרצת הבדיקות
# unittest.main(argv=['first-arg-is-ignored'], exit=False)