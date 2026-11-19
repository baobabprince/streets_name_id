import pandas as pd
import unittest

# DataFrame מדומה לבדיקת איכות הנתונים.  ``official_code`` הוא סמל הרחוב
# הקנוני בטבלת data.gov.il; ``_id`` של CKAN לעולם אינו מזהה רחוב.
LAMAS_df = pd.DataFrame(
    {
        "official_code": [1001, 1002, 1003, 1004, 1005],
        "שם_ישוב": ["תל אביב", "חיפה", "ירושלים", "ירושלים", "ירושלים"],
        "שם_רחוב": ["רוטשילד", "הנביאים", "שד. בן גוריון", "אלי כהן", "בן יהודה"],
    }
).rename(
    columns={
        "official_code": "LAMAS_id",
        "שם_ישוב": "city",
        "שם_רחוב": "LAMAS_name",
    }
)


class TestLAMASDataQuality(unittest.TestCase):

    def test_required_columns_exist(self):
        # 1. בדיקת מבנה: האם העמודות החיוניות קיימות?
        required_cols = {"LAMAS_id", "city", "LAMAS_name"}
        self.assertTrue(
            required_cols.issubset(LAMAS_df.columns),
            "חסרות עמודות חיוניות בנתוני הלמ\"ס.",
        )

    def test_id_is_unique(self):
        # 2. בדיקת ייחודיות: במסגרת עיר אחת המזהה הקנוני אמור להיות ייחודי.
        self.assertTrue(
            LAMAS_df["LAMAS_id"].is_unique,
            "המזהה הייחודי (LAMAS_id) אינו ייחודי.",
        )

    def test_street_name_completeness(self):
        # 3. בדיקת שלמות: יש לוודא שאין יותר מדי שמות רחובות חסרים
        # נסבול פחות מ-5% רשומות חסרות שם רחוב
        missing_name_percentage = LAMAS_df["LAMAS_name"].isnull().sum() / len(LAMAS_df)
        self.assertLess(
            missing_name_percentage,
            0.05,
            f"מעל 5% משמות הרחובות בלמ\"ס חסרים ({missing_name_percentage*100:.2f}%)",
        )
