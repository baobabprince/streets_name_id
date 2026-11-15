import datetime
import os
import requests
import pandas as pd

LAMAS_API_URL = "https://data.gov.il/api/3/action/datastore_search"
LAMAS_SQL_API_URL = "https://data.gov.il/api/3/action/datastore_search_sql"
RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
LIMIT = 32000
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

    print("מתחיל בשליפת נתוני הלמ\"ס המלאים (כולל שמות נרדפים)...")
    all_records = []
    offset = 0
    try:
        metadata_response = requests.get(
            LAMAS_API_URL,
            params={"resource_id": RESOURCE_ID, "limit": 1, "offset": 0},
            timeout=30,
        )
        metadata_response.raise_for_status()
        total = metadata_response.json().get("result", {}).get("total", 0)
        if not total:
            raise ValueError("LAMAS API returned no records")
        print(f"סה\"כ רשומות לשליפה: {total}")

        while offset < total:
            sql = (
                f'SELECT * FROM "{RESOURCE_ID}" ORDER BY _id '
                f"LIMIT {LIMIT} OFFSET {offset}"
            )
            response = requests.get(
                LAMAS_SQL_API_URL, params={"sql": sql}, timeout=120
            )
            response.raise_for_status()
            records = response.json().get("result", {}).get("records", [])
            expected_count = min(LIMIT, total - offset)
            if len(records) != expected_count:
                raise ValueError(
                    f"LAMAS API returned {len(records)} of "
                    f"{expected_count} records at offset {offset}"
                )

            all_records.extend(records)
            offset += len(records)
            print(f"נשלפו {len(all_records)} מתוך {total} רשומות...")
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