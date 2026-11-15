import datetime
import os
import requests
import pandas as pd

LAMAS_API_URL = "https://data.gov.il/api/3/action/datastore_search"
RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
LIMIT = 1000  # גודל דף מקסימלי, עדיף להתחיל גבוה
CACHE_FILE = os.path.join(os.path.dirname(__file__), "cache", "lamas_data.csv")
CACHE_MAX_AGE_DAYS = 182


def _normalize_lamas_data(df):
    df = df.copy()
    df.columns = df.columns.map(lambda column: str(column).strip())

    if {"LAMAS_id", "LAMAS_name", "city"}.issubset(df.columns):
        id_column, name_column, city_column = "LAMAS_id", "LAMAS_name", "city"
    elif {"official_code", "street_name", "city_name"}.issubset(df.columns):
        id_column, name_column, city_column = "official_code", "street_name", "city_name"
    else:
        raise ValueError(
            "LAMAS CSV must contain LAMAS_id/LAMAS_name/city or "
            "official_code/street_name/city_name columns"
        )

    normalized = pd.DataFrame({
        "LAMAS_id": df[id_column].astype("string").str.strip(),
        "LAMAS_name": df[name_column].astype("string").str.strip(),
        "city": df[city_column].astype("string").str.strip(),
    })
    normalized = normalized.drop_duplicates(subset=["LAMAS_id", "LAMAS_name", "city"])
    if normalized.empty:
        raise ValueError("LAMAS data contains no records")
    return normalized


def _load_csv_cache():
    df = pd.read_csv(CACHE_FILE, dtype=str)
    return _normalize_lamas_data(df)


def _csv_cache_is_fresh():
    if not os.path.exists(CACHE_FILE):
        return False
    age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
        os.path.getmtime(CACHE_FILE)
    )
    return age.days <= CACHE_MAX_AGE_DAYS


def fetch_all_LAMAS_data(force_refresh=False):
    """
    שולף את כל נתוני הרחובות של הלמ"ס (דרך ה-API)
    ומחזיר אותם כ-DataFrame, כולל טיפול בשמות רחובות נרדפים.
    """

    if not force_refresh and _csv_cache_is_fresh():
        print(f"נמצא קובץ מטמון מקומי: {CACHE_FILE}, טוען נתונים...")
        try:
            LAMAS_df = _load_csv_cache()
            print(f"נטען בהצלחה מהקובץ המקומי: {len(LAMAS_df)} רשומות")
            return LAMAS_df
        except (OSError, ValueError, UnicodeDecodeError, pd.errors.ParserError) as e:
            print(f"שגיאה בטעינת קובץ המטמון: {e}, מנסה להוריד מחדש...")

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
                if total is None or len(all_records) < total or not all_records:
                    raise ValueError(
                        f"LAMAS API stopped after {len(all_records)} of {total} records"
                    )
                break

            all_records.extend(records)
            offset += LIMIT
            print(f"נשלפו {len(all_records)} מתוך {total} רשומות...")
            if total and len(all_records) >= total:
                break

        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"שגיאה במהלך שליפת ה-API ב-offset {offset}: {e}")
            if os.path.exists(CACHE_FILE):
                try:
                    LAMAS_df = _load_csv_cache()
                    print(
                        f"ה-API נכשל; משתמש בעותק CSV מקומי "
                        f"({len(LAMAS_df)} רשומות)."
                    )
                    return LAMAS_df
                except (
                    OSError,
                    ValueError,
                    UnicodeDecodeError,
                    pd.errors.ParserError,
                ) as cache_error:
                    raise e from cache_error
            raise

    if all_records:
        LAMAS_raw_df = pd.DataFrame(all_records)
        LAMAS_df = _normalize_lamas_data(LAMAS_raw_df)

        # שמירה לקובץ מקומי
        try:
            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            LAMAS_df.to_csv(CACHE_FILE, index=False)
            print(f"הנתונים נשמרו לקובץ מקומי: {CACHE_FILE}")
        except Exception as e:
            print(f"שגיאה בשמירת קובץ המטמון: {e}")

        return LAMAS_df
    else:
        raise ValueError("LAMAS API returned no records")

    # LAMAS_data = fetch_all_LAMAS_data()
    # if not LAMAS_data.empty:
    # print("\n--- דוגמה לנתוני הלמ\"ס ששולפו ---")
    # print(LAMAS_data.head())
    # print(f"גודל הנתונים שנשלפו: {len(LAMAS_data)} רשומות")