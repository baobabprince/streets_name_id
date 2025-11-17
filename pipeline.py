# pipeline.py
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import numpy as np
import requests
import time
import json
from fuzzywuzzy import fuzz
import re
import datetime
import sys

# --- Import necessary utility functions (assuming these files exist in the project) ---
from lamas_streets import fetch_all_LAMAS_data
from OSM_streets import fetch_osm_street_data, place_name as OSM_PLACE_NAME
from map_of_adjacents import build_adjacency_map
from normalization import normalize_street_name, find_fuzzy_candidates


# --- הגדרות API ---
# קריאה למפתח ה-API ממשתנה הסביבה GEMINI_API_KEY
API_KEY = os.getenv("GEMINI_API_KEY") 
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={API_KEY}"
MAX_RETRIES = 3

# Caching settings
CACHE_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(CACHE_DIR, exist_ok=True)
LAMAS_CACHE = os.path.join(CACHE_DIR, "LAMAS_data.pkl")
OSM_CACHE_TEMPLATE = os.path.join(CACHE_DIR, "osm_data_{place}.pkl")
SIX_MONTHS_DAYS = 182


# --- פונקציות עזר לקריאת API ---
def get_ai_resolution(prompt, osm_id):
    """
    מבצע קריאת API אמיתית למודל Gemini כדי להכריע בזיהוי רחובות.
    כולל מנגנון Retry פשוט.
    """
    if not API_KEY:
        print("  -> ERROR: GEMINI_API_KEY not found or is empty. Skipping AI resolution.")
        return 'None'
        
    print(f"  -> Consulting AI for OSM ID: {osm_id}...")
    
    # הגדרות פרומפט ופרסונה עבור המודל
    system_prompt = "אתה מערכת GIS אוטומטית שתפקידה למצוא את המזהה המספרי היחיד של רחוב (LAMAS ID) מתוך רשימת מועמדים, על בסיס שם וקונטקסט גיאוגרפי (שמות רחובות משיקים). השב עם המספר של ה-ID בלבד, או עם המילה 'None' אם לא נמצאה התאמה ודאית."
    
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "tools": [{"google_search": {}}] # מאפשר שימוש בחיפוש להקשר היסטורי/כינויים
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GEMINI_API_URL, 
                headers={'Content-Type': 'application/json'}, 
                data=json.dumps(payload)
            )
            response.raise_for_status() # שגיאות HTTP יעלו חריגה
            
            result = response.json()
            # חילוץ הטקסט
            text = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', 'None').strip()
            
            # ניקוי התשובה - ודא שרק ה-ID המספרי או 'None' מוחזר
            clean_text = ''.join(filter(str.isdigit, text))
            if not clean_text:
                return 'None'
            
            return clean_text

        except requests.exceptions.RequestException as e:
            print(f"API Error (Attempt {attempt+1}/{MAX_RETRIES}) for {osm_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt) # Exponential backoff
            else:
                return 'None' # החזרת None לאחר כל הניסיונות

    return 'None'


# --- 1. פונקציות עזר לטיפול ב-CACHE וטעינת נתונים ---
def _is_fresh(path, max_age_days=SIX_MONTHS_DAYS):
    if not os.path.exists(path):
        return False
    age_days = (datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(path))).days
    return age_days <= max_age_days

def _safe_place_name(place: str) -> str:
    # convert place to a filesystem-safe token
    return re.sub(r'[^0-9A-Za-z_\-\u0590-\u05FF]', '_', place)

def _normalize_city(col: pd.Series) -> pd.Series:
    """Robustly normalizes city names."""
    return (
        col.astype(str)
        .str.replace(r'[\u2010\u2011\u2012\u2013\u2014\-]', ' ', regex=True)  # various dashes -> space
        .str.replace(r'\s+', ' ', regex=True) # multiple spaces -> single space
        .str.replace(r'עיריית', '', regex=False) # remove common suffix/prefix
        .str.strip()
    )

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

    gdf = fetch_osm_street_data(place)
    try:
        # GeoDataFrame is a subclass of DataFrame; pickle preserves geometry
        gdf.to_pickle(cache_path)
    except Exception:
        pass
    return gdf


# ----------------------------------------------------------------------------------
#                                 ORCHESTRATION START
# ----------------------------------------------------------------------------------

def run_pipeline(place: str | None = None, force_refresh: bool = False, use_ai: bool = False):
    """
    מארגן את כל ה-pipeline למיפוי מזהי הרחובות.
    """
    print("--- Starting Street Mapping Orchestrator ---")
    if not API_KEY:
        print("\n*** WARNING: GEMINI_API_KEY environment variable not set. AI resolution will be skipped. ***)")

    if not use_ai:
        print("\n*** INFO: AI resolution is disabled for this run (use_ai=False). ***")
    
    # STEP 1: Data Acquisition (with caching)
    try:
        chosen_place = place or OSM_PLACE_NAME or "Tel Aviv-Yafo, Israel"
        print(f"Using place: {chosen_place}")
        LAMAS_df = load_or_fetch_LAMAS(force_refresh=force_refresh)
        osm_gdf = load_or_fetch_osm(chosen_place, force_refresh=force_refresh)


        # If OSM doesn't include a 'city' column, populate it from the place string
        if 'city' not in osm_gdf.columns:
            city_label = chosen_place.split(',')[0].strip()
            osm_gdf['city'] = city_label

        # Normalize city name formatting in both dataframes (CRITICAL FIX)
        if 'city' in LAMAS_df.columns:
            LAMAS_df['city'] = _normalize_city(LAMAS_df['city'])
        osm_gdf['city'] = _normalize_city(osm_gdf['city'])
        
        # Save intermediate normalized data
        _save_intermediate_df(LAMAS_df, "step1_lamas_raw", chosen_place)
        _save_intermediate_df(osm_gdf, "step1_osm_raw", chosen_place)
        
    except Exception as e:
        print(f"FATAL ERROR during Data Acquisition: {e}")
        return

    # STEP 2: Preprocessing and Normalization
    print("\n[Step 2/7] Normalizing street names...")
    LAMAS_df['normalized_name'] = LAMAS_df['LAMAS_name'].apply(normalize_street_name)
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
    
    LAMAS_df.dropna(subset=['normalized_name'], inplace=True)
    osm_gdf.dropna(subset=['normalized_name'], inplace=True)

    _save_intermediate_df(LAMAS_df, "step2_lamas_normalized", chosen_place)
    _save_intermediate_df(osm_gdf, "step2_osm_normalized", chosen_place)
    
    # STEP 3: Topology (Adjacency)
    print("\n[Step 3/7] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    # STEP 4: Candidate Matching (CREATES candidates_df)
    print("\n[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
    candidates_df = find_fuzzy_candidates(osm_gdf, LAMAS_df)
    
    _save_intermediate_df(candidates_df, "step4_candidates", chosen_place)

    # STEP 5: AI Resolution (CREATES ai_decisions_df)
    print("\n[Step 5/7] (Optional) Invoking Real AI for ambiguous cases...")
    ai_results = []

    if use_ai and API_KEY:
        # Filter candidates to the city being processed to avoid processing irrelevant streets
        osm_city_label = osm_gdf['city'].iloc[0]
        osm_gdf_in_city = osm_gdf[osm_gdf['city'] == osm_city_label]
        
        ai_candidates_to_process = candidates_df[
            (candidates_df['status'] == 'NEEDS_AI') & 
            (candidates_df['osm_id'].isin(osm_gdf_in_city['osm_id']))
        ].copy()
        
        # Make a set of OSM IDs to check against
        osm_id_set = set(osm_gdf_in_city['osm_id'])
        
        for _, row in ai_candidates_to_process.iterrows():
            osm_id = row['osm_id']

            # Safety check: skip if osm_id is not in the current city (shouldn't happen with filter above)
            if osm_id not in osm_id_set:
                 continue

            # Building the full prompt (including topological context)
            osm_street_name = osm_gdf_in_city[osm_gdf_in_city['osm_id'] == osm_id]['normalized_name'].iloc[0]
            
            # Find adjacent OSM IDs and look up their normalized names
            adjacent_osm_ids = map_of_adjacents.get(osm_id, [])
            adjacent_names = osm_gdf_in_city[osm_gdf_in_city['osm_id'].isin(adjacent_osm_ids)]['normalized_name'].tolist()

            prompt = (f"OSM Street Name: '{osm_street_name}'. "
                      f"Adjacent Streets: {', '.join(adjacent_names) if adjacent_names else 'None'}. "
                      f"LAMAS Candidates: {row['all_candidates']}. Choose the best LAMAS ID (number only or 'None').")

            ai_decision_id = get_ai_resolution(prompt, osm_id)
            ai_results.append({'osm_id': osm_id, 'ai_LAMAS_id': ai_decision_id})
    else:
        # AI disabled or missing API key — produce empty results
        ai_results = []

    # ensure DataFrame has expected columns even if empty
    ai_decisions_df = pd.DataFrame(ai_results, columns=['osm_id', 'ai_LAMAS_id'])
    _save_intermediate_df(ai_decisions_df, "step5_ai_decisions", chosen_place)

    # STEP 6: Final Merge and Mapping
    print("\n[Step 6/7] Merging results to create final diagnostic table...")
    
    # Merge AI decisions back into the candidates table
    ai_decisions_merged = candidates_df.merge(ai_decisions_df, on='osm_id', how='left')
    
    # Convert all ID columns to string for safe merging and final output consistency
    ai_decisions_merged['osm_id'] = ai_decisions_merged['osm_id'].astype(str)
    osm_gdf['osm_id'] = osm_gdf['osm_id'].astype(str)
    
    # 1. Create final_mapping_df (the rows that successfully received a final ID)
    final_mapping_df = ai_decisions_merged[
        (ai_decisions_merged['status'] == 'CONFIDENT') | 
        ((ai_decisions_merged['status'] == 'NEEDS_AI') & (ai_decisions_merged['ai_LAMAS_id'].astype(str) != 'None'))
    ].copy()

    # Determine the final ID source
    final_mapping_df['final_LAMAS_id'] = final_mapping_df.apply(
        lambda row: row['best_LAMAS_id'] if row['status'] == 'CONFIDENT' else row['ai_LAMAS_id'], axis=1
    ).astype(str)

    # Select key diagnostic columns from candidates/AI merge
    diagnostic_cols = ['osm_id', 'status', 'best_score', 'best_LAMAS_name', 'all_candidates', 'ai_LAMAS_id']
    
    # Create the full diagnostic table by merging OSM data with the candidates/AI data
    diagnostic_df_full = osm_gdf[['osm_id', 'osm_name', 'normalized_name', 'city', 'geometry']].merge(
        ai_decisions_merged[diagnostic_cols], on='osm_id', how='left'
    )
    
    # Merge the final LAMAS ID
    diagnostic_df_full = diagnostic_df_full.merge(
        final_mapping_df[['osm_id', 'final_LAMAS_id']], on='osm_id', how='left'
    )

    # Export final diagnostic report to CSV (new comprehensive file)
    _save_intermediate_df(diagnostic_df_full, "diagnostic_report", chosen_place)
    
    # Merge final mapping back into GeoDataFrame for visualization (Step 7)
    osm_gdf_final = osm_gdf.merge(diagnostic_df_full[['osm_id', 'final_LAMAS_id']], on='osm_id', how='left')

    print("\n-----------------------------------------------------")
    print("                 PIPELINE COMPLETED                  ")
    print("-----------------------------------------------------")
    print(f"Total OSM streets considered: {len(osm_gdf_final)}")
    
    matched_count = final_mapping_df.shape[0]
    unmatched_count = len(osm_gdf_final) - matched_count
    
    print(f"Total successfully matched (Confident + AI): {matched_count}")
    print(f"Total unmatched (Missing/AI rejected): {unmatched_count}")
    print("\n--- Final Mapping Result Sample ---")
    print(osm_gdf_final[osm_gdf_final['final_LAMAS_id'].notna()][['osm_id', 'osm_name', 'final_LAMAS_id']].head(5))

    # STEP 6.5: Calculate Diagnostic Summary
    print("\n[Step 6.5/7] Calculating diagnostic summary...")
    osm_city_label = osm_gdf['city'].iloc[0]
    lamas_in_city_df = LAMAS_df[LAMAS_df['city'] == osm_city_label]

    total_osm_streets = len(osm_gdf)
    total_lamas_streets = len(lamas_in_city_df)

    confident_matches = diagnostic_df_full[diagnostic_df_full['status'] == 'CONFIDENT'].shape[0]
    ai_resolved_matches = diagnostic_df_full[(diagnostic_df_full['status'] == 'NEEDS_AI') & (diagnostic_df_full['final_LAMAS_id'].notna())].shape[0]
    total_matched = confident_matches + ai_resolved_matches
    unmatched_osm_streets = total_osm_streets - total_matched

    # Find unmatched LAMAS streets
    if not lamas_in_city_df.empty:
        # Ensure final_LAMAS_id is string, handling potential float representations
        final_mapping_df['final_LAMAS_id'] = final_mapping_df['final_LAMAS_id'].astype(str).str.replace(r'\.0$', '', regex=True)
        matched_lamas_ids = set(final_mapping_df['final_LAMAS_id'].dropna())

        lamas_in_city_df['LAMAS_id'] = lamas_in_city_df['LAMAS_id'].astype(str)
        unmatched_lamas_df = lamas_in_city_df[~lamas_in_city_df['LAMAS_id'].isin(matched_lamas_ids)]
        unmatched_lamas_street_names = sorted(unmatched_lamas_df['LAMAS_name'].tolist())
        unmatched_lamas_count = len(unmatched_lamas_df)
        unmatched_lamas_percentage = (unmatched_lamas_count / total_lamas_streets) * 100 if total_lamas_streets > 0 else 0
    else:
        unmatched_lamas_street_names = []
        unmatched_lamas_count = 0
        unmatched_lamas_percentage = 0

    diagnostics = {
        "total_osm_streets": total_osm_streets,
        "total_lamas_streets": total_lamas_streets,
        "confident_matches": confident_matches,
        "ai_resolved_matches": ai_resolved_matches,
        "total_matched": total_matched,
        "unmatched_osm_streets": unmatched_osm_streets,
        "unmatched_lamas_count": unmatched_lamas_count,
        "unmatched_lamas_percentage": f"{unmatched_lamas_percentage:.1f}%",
        "unmatched_lamas_street_names": unmatched_lamas_street_names
    }
    print(f"-> Diagnostic Summary: {diagnostics}")

    # STEP 7: Generate HTML Visualization
    print("\n[Step 7/7] Generating HTML visualization of all streets...")
    try:
        from generate_html import create_html_from_gdf
        os.makedirs("HTML", exist_ok=True)
        # Pass the GeoDataFrame and the new diagnostics object
        create_html_from_gdf(diagnostic_df_full, chosen_place, diagnostics)
    except Exception as e:
        print(f"Warning: failed to generate HTML visualization: {e}")

if __name__ == "__main__":
    place_arg = None
    force = False
    no_ai = False
    if len(sys.argv) > 1:
        place_arg = sys.argv[1]
    if "--refresh" in sys.argv:
        force = True
    if "--no-ai" in sys.argv:
        no_ai = True

    run_pipeline(place=place_arg, force_refresh=force, use_ai=(not no_ai))