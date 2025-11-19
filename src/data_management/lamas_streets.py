import requests
import pandas as pd

LAMAS_API_URL = "https://data.gov.il/api/3/action/datastore_search"
RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
LIMIT = 1000  # גודל דף מקסימלי, עדיף להתחיל גבוה

def fetch_all_LAMAS_data():
    """
    שולף את כל נתוני הרחובות של הלמ"ס (דרך ה-API)
    ומחזיר אותם כ-DataFrame, כולל טיפול בשמות רחובות נרדפים.
    """
    all_records = []
    offset = 0
    total = None
    
    print("מתחיל בשליפת נתוני הלמ\"ס המלאים (כולל שמות נרדפים)...")

    while True:
        params = {
            'resource_id': RESOURCE_ID,
            'limit': LIMIT,
            'offset': offset
        }
        
        try:
            response = requests.get(LAMAS_API_URL, params=params, timeout=30)
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

    if all_records:
        LAMAS_raw_df = pd.DataFrame(all_records)
        
        # ניקוי רווחים משמות העמודות
        LAMAS_raw_df.columns = LAMAS_raw_df.columns.str.strip()

        # יצירת DataFrame חדש עם העמודות הרצויות
        # עבור שמות רחובות רשמיים ושמות נרדפים, ה-LAMAS_id יהיה ה-official_code
        LAMAS_df = pd.DataFrame({
            'LAMAS_id': LAMAS_raw_df['official_code'].astype(str).str.strip(),
            'LAMAS_name': LAMAS_raw_df['street_name'].str.strip(),
            'city': LAMAS_raw_df['city_name'].str.strip()
        })
        
        # הסרת כפילויות שנוצרות אם אותו שם רחוב מופיע מספר פעמים עבור אותו official_code באותה עיר
        LAMAS_df.drop_duplicates(subset=['LAMAS_id', 'LAMAS_name', 'city'], inplace=True)

        return LAMAS_df
    else:
        print("לא נשלפו נתונים.")
        return pd.DataFrame()

    # LAMAS_data = fetch_all_LAMAS_data()
    # if not LAMAS_data.empty:
    # print("\n--- דוגמה לנתוני הלמ\"ס ששולפו ---")
    # print(LAMAS_data.head())
    # print(f"גודל הנתונים שנשלפו: {len(LAMAS_data)} רשומות")