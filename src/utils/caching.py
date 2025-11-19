# src/utils/caching.py
import os
import re
import datetime
import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(__file__), "../../data")
os.makedirs(CACHE_DIR, exist_ok=True)
LAMAS_CACHE = os.path.join(CACHE_DIR, "LAMAS_data.pkl")
OSM_CACHE_TEMPLATE = os.path.join(CACHE_DIR, "osm_data_{place}.pkl")
SIX_MONTHS_DAYS = 182

def _is_fresh(path, max_age_days=SIX_MONTHS_DAYS):
    if not os.path.exists(path):
        return False
    age_days = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path))).days
    return age_days <= max_age_days

def _safe_place_name(place: str) -> str:
    # convert place to a filesystem-safe token
    return re.sub(r'[^0-9A-Za-z_\-\u0590-\u05FF]', '_', place)

def _save_intermediate_df(df, step_name, place):
    """Saves an intermediate DataFrame/GeoDataFrame to the cache directory."""
    safe_place = _safe_place_name(place)
    # Use CSV for the final report, PKL for intermediate steps to preserve data types
    file_ext = "csv" if step_name.startswith("diagnostic_report") else "pkl"
    filename = f"{step_name}_{safe_place}.{file_ext}"
    path = os.path.join(CACHE_DIR, filename)

    try:
        if file_ext == "pkl":
            df.to_pickle(path)
        elif file_ext == "csv":
            # Ensure geometry is dropped if saving GeoDataFrame to CSV
            df_to_save = df.drop(columns=['geometry'], errors='ignore')
            df_to_save.to_csv(path, index=False, encoding='utf-8')

        print(f"  -> Saved intermediate result for {step_name} to {path}")
    except Exception as e:
        print(f"  -> Warning: Failed to save {step_name} intermediate result: {e}")

def load_or_fetch_LAMAS(force_refresh: bool = False, max_age_days: int = SIX_MONTHS_DAYS):
    """Load LAMAS data from cache if fresh, otherwise fetch and cache it."""
    if not force_refresh and _is_fresh(LAMAS_CACHE, max_age_days):
        try:
            return pd.read_pickle(LAMAS_CACHE)
        except Exception:
            pass

    from src.data_management.lamas_streets import fetch_all_LAMAS_data
    df = fetch_all_LAMAS_data()
    try:
        df.to_pickle(LAMAS_CACHE)
    except Exception:
        pass
    return df

def load_or_fetch_osm(place: str, force_refresh: bool = False, max_age_days: int = SIX_MONTHS_DAYS):
    """Load OSM GeoDataFrame from cache if fresh, otherwise fetch and cache it."""
    safe = _safe_place_name(place)
    cache_path = OSM_CACHE_TEMPLATE.format(place=safe)
    if not force_refresh and _is_fresh(cache_path, max_age_days):
        try:
            return pd.read_pickle(cache_path)
        except Exception:
            pass

    from src.data_management.OSM_streets import fetch_osm_street_data
    gdf = fetch_osm_street_data(place)
    try:
        # GeoDataFrame is a subclass of DataFrame; pickle preserves geometry
        gdf.to_pickle(cache_path)
    except Exception:
        pass
    return gdf
