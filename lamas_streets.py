import datetime
import logging
import os
import time

import requests
import pandas as pd

from provenance import cached_file_provenance, utc_now, write_sidecar
from settlement_names import canonical_key

LAMAS_API_URL = "https://data.gov.il/api/3/action/datastore_search"
RESOURCE_ID = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
LIMIT = 32000
CACHE_FILE = os.path.join(os.path.dirname(__file__), "cache", "lamas_data.csv")
CACHE_MAX_AGE_DAYS = 7
REQUEST_TIMEOUT = (10, 90)
MAX_REQUEST_ATTEMPTS = 3
USER_AGENT = "datagov-external-client"
logger = logging.getLogger(__name__)


def _clean_code(value) -> str:
    """Normalize a CKAN code ("101", " 101 ", 101.0, None -> "101"/"")."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "nat", "<na>"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text


def _string_column(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in df.columns:
        return df[column].astype("string").str.strip().fillna(default)
    return pd.Series([default] * len(df), index=df.index, dtype="string")


def _code_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="string")
    return df[column].map(_clean_code).astype("string")


def normalize_lamas_data(df):
    """Keep the canonical street id *and* the metadata needed to trust it.

    The data.gov.il synonyms resource has both ``street_code`` (the code of
    this row's name, including synonyms) and ``official_code`` (the canonical
    street code the synonym points at).  ``official_code`` is only unique
    *within a city*, so ``LAMAS_key`` = ``city_code:official_code`` is the
    globally unambiguous identifier.  Keeping ``street_name_status`` lets the
    diagnostics count official streets without guessing from code length.
    """
    df = df.copy()
    df.columns = df.columns.map(lambda column: str(column).strip())

    normalized_schema = {"LAMAS_id", "LAMAS_name", "city"}.issubset(df.columns)
    raw_schema = {"official_code", "street_name", "city_name"}.issubset(df.columns)
    if normalized_schema:
        id_column, name_column, city_column = "LAMAS_id", "LAMAS_name", "city"
    elif raw_schema:
        id_column, name_column, city_column = "official_code", "street_name", "city_name"
    else:
        raise ValueError(
            "LAMAS CSV must contain LAMAS_id/LAMAS_name/city or "
            "official_code/street_name/city_name columns"
        )

    normalized = pd.DataFrame({
        "LAMAS_id": df[id_column].map(_clean_code).astype("string"),
        "LAMAS_name": _string_column(df, name_column),
        "city": _string_column(df, city_column),
    })
    if raw_schema:
        normalized["official_code"] = df[id_column].map(_clean_code).astype("string")
    else:
        normalized["official_code"] = normalized["LAMAS_id"]
    normalized["street_code"] = _code_column(df, "street_code")
    normalized["street_name_status"] = _string_column(df, "street_name_status")
    normalized["city_code"] = _code_column(df, "city_code")
    if "region_code" in df.columns:
        normalized["region_code"] = _code_column(df, "region_code")

    status = normalized["street_name_status"].str.casefold()
    official_code = normalized["official_code"].fillna("")
    street_code = normalized["street_code"].fillna("")
    same_code = (
        official_code.ne("") & official_code.eq(street_code)
    )
    legacy_without_status = (
        normalized["street_name_status"].eq("")
        & street_code.eq("")
        & normalized["LAMAS_id"].ne("")
    )
    is_official = status.eq("official") | same_code | legacy_without_status

    # Some rows lack official_code but are themselves official street rows.
    fallback = street_code.where(
        normalized["LAMAS_id"].eq("") & is_official, ""
    )
    normalized.loc[fallback.ne(""), "LAMAS_id"] = fallback
    normalized["id_source"] = "official_code"
    normalized.loc[
        normalized["LAMAS_id"].eq(street_code)
        & normalized["LAMAS_id"].ne(official_code),
        "id_source",
    ] = "street_code_fallback"

    city_part = normalized["city_code"].where(
        normalized["city_code"].ne(""),
        normalized["city"].fillna("").map(canonical_key).astype("string"),
    )
    normalized["city_code"] = city_part
    normalized["LAMAS_key"] = (
        city_part.fillna("").astype(str) + ":" + normalized["LAMAS_id"].fillna("").astype(str)
    )
    normalized["is_official"] = is_official

    name_lower = normalized["LAMAS_name"].str.lower()
    valid = (
        normalized["LAMAS_id"].ne("")
        & normalized["LAMAS_name"].ne("")
        & ~name_lower.isin(["nan", "none", "null"])
        & ~normalized["LAMAS_name"].str.fullmatch(r"[-–—\s]*", na=False)
        & normalized["city"].ne("")
    )
    normalized = normalized.loc[valid].copy()
    normalized = normalized.drop_duplicates(
        subset=["LAMAS_key", "LAMAS_name", "city"]
    )
    if normalized.empty:
        raise ValueError("LAMAS data contains no usable street records")

    columns = [
        "LAMAS_id", "LAMAS_name", "city", "city_code", "official_code",
        "street_code", "street_name_status", "is_official", "id_source",
        "LAMAS_key", "region_code",
    ]
    return normalized[[column for column in columns if column in normalized.columns]].reset_index(drop=True)


# Backwards-compatible private name used by existing callers/tests.
_normalize_lamas_data = normalize_lamas_data


def _load_csv_cache():
    df = pd.read_csv(CACHE_FILE, dtype=str)
    df = normalize_lamas_data(df)
    df.attrs["provenance"] = _source_provenance(
        cached_file_provenance(CACHE_FILE) or {}
    )
    return df


def _source_provenance(extra: dict) -> dict:
    return {
        "source": "data.gov.il",
        "resource_id": RESOURCE_ID,
        **{key: value for key, value in extra.items() if key not in {"source", "resource_id"}},
    }


def _csv_cache_is_fresh():
    if not os.path.exists(CACHE_FILE):
        return False
    age = datetime.datetime.now() - datetime.datetime.fromtimestamp(
        os.path.getmtime(CACHE_FILE)
    )
    return age.days <= CACHE_MAX_AGE_DAYS


def _get_api_json(session, params):
    """Request one CKAN API page, retrying transient network and server errors."""
    for attempt in range(MAX_REQUEST_ATTEMPTS):
        try:
            response = session.get(
                LAMAS_API_URL, params=params, timeout=REQUEST_TIMEOUT
            )
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == MAX_REQUEST_ATTEMPTS - 1:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = max(0, min(float(retry_after), 30)) if retry_after else 2**attempt
                except ValueError:
                    delay = 2**attempt
                logger.warning(
                    "LAMAS API returned HTTP %s; retrying in %s seconds",
                    response.status_code,
                    delay,
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not payload.get("success"):
                raise ValueError(f"LAMAS API returned an unsuccessful response: {payload}")
            return payload
        except (requests.ConnectionError, requests.Timeout):
            if attempt == MAX_REQUEST_ATTEMPTS - 1:
                raise
            delay = 2**attempt
            logger.warning("LAMAS API request failed; retrying in %s seconds", delay)
            time.sleep(delay)

    raise RuntimeError("LAMAS API request exhausted retries")


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
    retrieved_at = utc_now()
    all_records = []
    offset = 0
    total = None
    try:
        with requests.Session() as session:
            session.headers.update(
                {"User-Agent": USER_AGENT, "Accept": "application/json"}
            )
            while total is None or offset < total:
                payload = _get_api_json(
                    session,
                    {
                        "resource_id": RESOURCE_ID,
                        "limit": LIMIT,
                        "offset": offset,
                    },
                )
                result = payload.get("result")
                if not isinstance(result, dict):
                    raise ValueError("LAMAS API response is missing its result object")
                current_total = result.get("total")
                records = result.get("records")
                if not isinstance(current_total, int) or current_total < 0:
                    raise ValueError("LAMAS API returned an invalid total record count")
                if not isinstance(records, list):
                    raise ValueError("LAMAS API response is missing its records list")
                if total is None:
                    total = current_total
                    if not total:
                        raise ValueError("LAMAS API returned no records")
                    print(f"סה\"כ רשומות לשליפה: {total}")
                elif current_total != total:
                    raise ValueError(
                        f"LAMAS dataset changed during download "
                        f"(total was {total}, now {current_total})"
                    )

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
        LAMAS_df = normalize_lamas_data(LAMAS_raw_df)
        LAMAS_df.attrs["provenance"] = _source_provenance({"retrieved_at": retrieved_at})

        # שמירה לקובץ מקומי
        try:
            os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
            LAMAS_df.to_csv(CACHE_FILE, index=False)
            print(f"הנתונים נשמרו לקובץ מקומי: {CACHE_FILE}")
            write_sidecar(CACHE_FILE, LAMAS_df.attrs["provenance"])
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