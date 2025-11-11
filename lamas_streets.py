import requests
import pandas as pd

LAMS_API_URL = "https://data.gov.il/api/3/action/datastore_search"
RESOURCE_ID = "9ad3862c-8391-4b2f-84a4-2d4c68625f4b"
LIMIT = 1000  # גודל דף מקסימלי, עדיף להתחיל גבוה

def fetch_all_lams_data():
    """
    שולף את כל נתוני הרחובות של הלמ"ס (דרך ה-API)
    ומחזיר אותם כ-DataFrame.
    """
    all_records = []
    offset = 0
    total = None
    
    print("מתחיל בשליפת נתוני הלמ\"ס המלאים...")

    while True:
        params = {
            'resource_id': RESOURCE_ID,
            'limit': LIMIT,
            'offset': offset
        }
        
        try:
            response = requests.get(LAMS_API_URL, params=params, timeout=30)
            response.raise_for_status() # מעלה שגיאה אם הסטטוס אינו 200
            data = response.json()
            
            result = data.get('result', {})
            records = result.get('records', [])
            
            if total is None:
                total = result.get('total', 0)
                print(f"סה\"כ רשומות לשליפה: {total}")
            
            if not records:
                break # יציאה מהלולאה אם אין עוד רשומות

            all_records.extend(records)
            offset += LIMIT
            print(f"נשלפו {len(all_records)} מתוך {total} רשומות...")

        except requests.exceptions.RequestException as e:
            print(f"שגיאה במהלך שליפת ה-API ב-offset {offset}: {e}")
            break # יציאה מהלולאה במקרה של שגיאה

    # המרת הנתונים ל-DataFrame
    if all_records:
        lams_df = pd.DataFrame(all_records)
        
        # נשמור רק את העמודות הרלוונטיות כפי שמוצג בצילום המסך שלך:
        # 'שם_רחוב', 'שם_ישוב', '_id' (מזהה הלמ"ס הייחודי)
        # ישנם שדות נוספים כמו 'סמל_רחוב' ו'סמל_ישוב' ששימושיים להתאמה
        
        # ניקוי ושמות עמודות בעברית (לצורך נוחות: נשתמש בעמודות המקוריות כרגע)
        # lams_df = lams_df[['_id', 'שם_ישוב', 'שם_רחוב', 'סמל_ישוב', 'סמל_רחוב']].copy()
        
        return lams_df
    else:
        print("לא נשלפו נתונים.")
        return pd.DataFrame()

    # lams_data = fetch_all_lams_data()
    # if not lams_data.empty:
    # print("\n--- דוגמה לנתוני הלמ\"ס ששולפו ---")
    # print(lams_data.head())
    # print(f"גודל הנתונים שנשלפו: {len(lams_data)} רשומות")