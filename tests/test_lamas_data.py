import pandas as pd
import unittest
# נניח ש-lams_df טעון לאחר קריאה מוצלחת ל-fetch_all_lams_data()

# יצירת דאטה מדומה לצורך הבדיקה אם לא בוצעה שליפה בפועל
if 'lams_df' not in locals() or lams_df.empty:
    print("יצירת DataFrame מדומה של הלמ\"ס לצורך בדיקה.")
    lams_data_mock = {
    '_id': [1001, 1002, 1003, 1004, 1005],
    'שם_ישוב': ['תל אביב', 'חיפה', 'ירושלים', 'ירושלים', 'ירושלים'],
    'שם_רחוב': ['רוטשילד', 'הנביאים', 'שד. בן גוריון', 'אלי כהן', 'בן יהודה'], # שינוי None ל'אלי כהן'
}
    lams_df = pd.DataFrame(lams_data_mock)
    lams_df.rename(columns={'_id': 'lams_id', 'שם_ישוב': 'city', 'שם_רחוב': 'lams_name'}, inplace=True)


class TestLamsDataQuality(unittest.TestCase):
    
    def test_required_columns_exist(self):
        # 1. בדיקת מבנה: האם העמודות החיוניות קיימות?
        required_cols = {'lams_id', 'city', 'lams_name'}
        self.assertTrue(required_cols.issubset(lams_df.columns), 
                        "חסרות עמודות חיוניות בנתוני הלמ\"ס.")

    def test_id_is_unique(self):
        # 2. בדיקת ייחודיות: האם המזהה הייחודי אכן ייחודי?
        self.assertTrue(lams_df['lams_id'].is_unique, 
                        "המזהה הייחודי (lams_id) אינו ייחודי.")
        
    def test_street_name_completeness(self):
        # 3. בדיקת שלמות: יש לוודא שאין יותר מדי שמות רחובות חסרים
        # נסבול פחות מ-5% רשומות חסרות שם רחוב
        missing_name_percentage = lams_df['lams_name'].isnull().sum() / len(lams_df)
        self.assertLess(missing_name_percentage, 0.05, 
                        f"מעל 5% משמות הרחובות בלמ\"ס חסרים ({missing_name_percentage*100:.2f}%)")

# # הרצת הבדיקות
# unittest.main(argv=['first-arg-is-ignored'], exit=False)