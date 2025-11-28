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
from enum import Enum

class PipelineStatus(Enum):
    SUCCESS = "SUCCESS"
    NO_DATA = "NO_DATA"
    FAILURE = "FAILURE"

# --- Import necessary utility functions (assuming these files exist in the project) ---
from lamas_streets import fetch_all_LAMAS_data
from OSM_streets import fetch_osm_street_data, place_name as OSM_PLACE_NAME
from map_of_adjacents import build_adjacency_map
from map_of_adjacents import build_adjacency_map
from normalization import normalize_street_name, find_fuzzy_candidates
from local_ai_resolver import LocalAIResolver, get_local_ai_resolution


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
        print("  -> ERROR: GEMINI_API_KEY not found or is empty. Skipping AI resolution.")
        return 'None'
        
    print(f"  -> Consulting AI for OSM ID: {osm_id}...")
    
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
            
        print(f"  -> Saved intermediate result for {step_name} to {path}")
    except Exception as e:
        print(f"  -> Warning: Failed to save {step_name} intermediate result: {e}")


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

def load_or_fetch_osm(place: str | dict, force_refresh: bool = False, max_age_days: int = SIX_MONTHS_DAYS):
    """Load OSM GeoDataFrame from cache if fresh, otherwise fetch and cache it."""
    
    if isinstance(place, dict):
        place_str_for_cache = place.get('display_name', 'unknown_place')
    else:
        place_str_for_cache = place

    safe = _safe_place_name(place_str_for_cache)
    cache_path = OSM_CACHE_TEMPLATE.format(place=safe)
    
    if not force_refresh and _is_fresh(cache_path, max_age_days):
        try:
            return pd.read_pickle(cache_path)
        except Exception:
            pass

    gdf = fetch_osm_street_data(place) # pass the object (str or dict)
    
    if gdf is None:
        return None

    try:
        gdf.to_pickle(cache_path)
    except Exception:
        pass
    return gdf


def calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf):
    """Calculates the diagnostic summary statistics."""
    # --- OSM Statistics (based on unique street names) ---
    # Filter out streets with no normalized name, as they aren't processed
    named_osm_gdf = osm_gdf.dropna(subset=['normalized_name'])
    total_osm_streets = named_osm_gdf['normalized_name'].nunique()

    # Calculate matches based on unique names in the diagnostic dataframe
    confident_matches = diagnostic_df_full[diagnostic_df_full['status'] == 'CONFIDENT']['normalized_name'].nunique()
    ai_resolved_matches = diagnostic_df_full[
        (diagnostic_df_full['status'] == 'NEEDS_AI') & (diagnostic_df_full['final_LAMAS_id'].notna())
    ]['normalized_name'].nunique()

    # Total matched is the count of unique OSM names that have a final LAMAS ID
    total_matched = diagnostic_df_full[diagnostic_df_full['final_LAMAS_id'].notna()]['normalized_name'].nunique()
    unmatched_osm_streets = total_osm_streets - total_matched
    
    # Get the list of unmatched OSM street names
    matched_osm_names = set(diagnostic_df_full[diagnostic_df_full['final_LAMAS_id'].notna()]['normalized_name'].unique())
    all_osm_names = set(named_osm_gdf['normalized_name'].unique())
    unmatched_osm_names = sorted(list(all_osm_names - matched_osm_names))

    unmatched_osm_percentage = (unmatched_osm_streets / total_osm_streets) * 100 if total_osm_streets > 0 else 0

    # --- LAMAS Statistics (based on unique LAMAS IDs) ---
    if not lamas_in_city_df.empty:
        # Filter LAMAS data to include only actual streets (3-digit codes)
        # 4-digit codes represent non-streets: neighborhoods, squares, settlements, etc.
        lamas_streets_only = lamas_in_city_df[
            lamas_in_city_df['LAMAS_id'].astype(str).str.len() == 3
        ]
        
        total_lamas_streets = lamas_streets_only['LAMAS_id'].nunique()

        # Get the set of unique LAMAS IDs that were successfully matched to at least one OSM street
        matched_lamas_ids = set(diagnostic_df_full['final_LAMAS_id'].dropna().astype(str).str.replace(r'\.0$', '', regex=True))

        # Get the set of unique LAMAS IDs that were successfully matched
        matched_lamas_ids = set(diagnostic_df_full['final_LAMAS_id'].dropna().astype(str).str.replace(r'\.0$', '', regex=True))

        # Create a boolean Series that is True for every row where the LAMAS_id was matched
        is_matched = lamas_streets_only['LAMAS_id'].astype(str).isin(matched_lamas_ids)

        # All rows for LAMAS streets that were never matched
        unmatched_lamas_df = lamas_streets_only[~is_matched]

        # From the unmatched streets, get ONE name per unique LAMAS_id (to avoid showing all synonyms)
        # Group by LAMAS_id and take the first name for each ID
        unmatched_lamas_street_names = sorted(
            unmatched_lamas_df.groupby('LAMAS_id')['LAMAS_name'].first().tolist()
        )
        unmatched_lamas_count = unmatched_lamas_df['LAMAS_id'].nunique()
        unmatched_lamas_percentage = (unmatched_lamas_count / total_lamas_streets) * 100 if total_lamas_streets > 0 else 0
    else:
        total_lamas_streets = 0
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
        "unmatched_osm_percentage": f"{unmatched_osm_percentage:.1f}%",
        "unmatched_osm_street_names": unmatched_osm_names,
        "unmatched_lamas_count": unmatched_lamas_count,
        "unmatched_lamas_percentage": f"{unmatched_lamas_percentage:.1f}%",
        "unmatched_lamas_street_names": unmatched_lamas_street_names
    }
    return diagnostics


# ----------------------------------------------------------------------------------
#                                 ORCHESTRATION START
# ----------------------------------------------------------------------------------

def run_pipeline(place: str | dict | None = None, force_refresh: bool = False, use_ai: bool = False, use_local_ai: bool = True, skip_html: bool = False):
    """
    מארגן את כל ה-pipeline למיפוי מזהי הרחובות.
    'place' can be a string for a search query, or a dict from Nominatim.
    """
    print("--- Starting Street Mapping Orchestrator ---")
    if not API_KEY:
        print("\n*** WARNING: GEMINI_API_KEY environment variable not set. AI resolution will be skipped. ***")

    if not use_ai:
        print("\n*** INFO: AI resolution is disabled for this run (use_ai=False). ***")
    
    # STEP 1: Data Acquisition (with caching)
    try:
        # Determine the object to use for OSM fetching and the string for display/caching
        place_obj_for_osm = place
        if isinstance(place, dict):
            # If a dict is provided, use it directly for OSM. Use its display_name for logging.
            chosen_place_str = place.get('display_name', 'unknown_place')
        elif isinstance(place, str):
            # If a string is provided, append ", Israel" for disambiguation
            chosen_place_str = place
            if "israel" not in chosen_place_str.lower() and "palestine" not in chosen_place_str.lower():
                chosen_place_str = f"{chosen_place_str}, Israel"
            place_obj_for_osm = chosen_place_str
        else:
            # Default fallback if place is None
            chosen_place_str = OSM_PLACE_NAME or "Tel Aviv-Yafo, Israel"
            place_obj_for_osm = chosen_place_str

        print(f"Using place: {chosen_place_str}")
        LAMAS_df = load_or_fetch_LAMAS(force_refresh=force_refresh)
        
        # Pass the correct object (dict or string) to the fetcher
        osm_gdf = load_or_fetch_osm(place_obj_for_osm, force_refresh=force_refresh)

        # Check if OSM data was successfully fetched
        if osm_gdf is None:
            print(f"WARNING: No OSM data available for {chosen_place_str}")
            print("This settlement may not have street data in OpenStreetMap.")
            return PipelineStatus.NO_DATA
        
        # Check if OSM data is empty
        if len(osm_gdf) == 0:
            print(f"WARNING: OSM data for {chosen_place_str} is empty (no streets found)")
            return PipelineStatus.NO_DATA

        # GEOGRAPHIC VALIDATION: Check if the data is within Israel/Palestine bounds
        # Israel/Palestine bounds: lat (29.0 to 33.5), lon (34.0 to 36.0)
        ISRAEL_BOUNDS = (29.0, 34.0, 33.7, 36.0)  # (min_lat, min_lon, max_lat, max_lon)
        
        # Get the bounds of all geometries to check location
        try:
            # Use bounds instead of centroid to avoid CRS warning
            total_bounds = osm_gdf.total_bounds  # [minx, miny, maxx, maxy]
            avg_lon = (total_bounds[0] + total_bounds[2]) / 2
            avg_lat = (total_bounds[1] + total_bounds[3]) / 2
            
            min_lat, min_lon, max_lat, max_lon = ISRAEL_BOUNDS
            if not (min_lat <= avg_lat <= max_lat and min_lon <= avg_lon <= max_lon):
                print(f"ERROR: Downloaded data appears to be outside Israel/Palestine!")
                print(f"  Average coordinates: ({avg_lat:.4f}, {avg_lon:.4f})")
                print(f"  Expected bounds: lat ({min_lat} to {max_lat}), lon ({min_lon} to {max_lon})")
                print(f"  This likely means '{place}' matched to a location outside Israel.")
                print(f"  Please check the settlement name and try again.")
                return PipelineStatus.FAILURE
        except Exception as e:
            print(f"Warning: Could not validate geographic location: {e}")
        
        # SIZE VALIDATION: Warn if dataset is suspiciously large
        # A single city should typically have < 50,000 street segments
        if len(osm_gdf) > 50000:
            print(f"WARNING: Downloaded {len(osm_gdf)} street segments - this seems very large for a single city!")
            print(f"  This might indicate that the entire region was downloaded instead of just the city.")
            print(f"  Proceeding anyway, but results may be slow or incorrect.")

        # If OSM doesn't include a 'city' column, populate it from the place string
        if 'city' not in osm_gdf.columns:
            city_label = chosen_place_str.split(',')[0].strip()
            osm_gdf['city'] = city_label

        # Normalize city name formatting in both dataframes (CRITICAL FIX)
        if 'city' in LAMAS_df.columns:
            LAMAS_df['city'] = _normalize_city(LAMAS_df['city'])
        osm_gdf['city'] = _normalize_city(osm_gdf['city'])
        
        # Save intermediate normalized data
        _save_intermediate_df(LAMAS_df, "step1_lamas_raw", chosen_place_str)
        _save_intermediate_df(osm_gdf, "step1_osm_raw", chosen_place_str)
        
    except Exception as e:
        print(f"FATAL ERROR during Data Acquisition: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return PipelineStatus.FAILURE


    # STEP 2: Preprocessing and Normalization
    print("\n[Step 2/7] Normalizing street names...")
    LAMAS_df['normalized_name'] = LAMAS_df['LAMAS_name'].apply(normalize_street_name)
    
    # Normalize osm_name (which already prioritizes Hebrew from OSM_streets.py)
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
    
    # Also normalize the original name and name:he if they exist for diagnostic purposes
    if 'osm_name_original' in osm_gdf.columns:
        osm_gdf['normalized_original'] = osm_gdf['osm_name_original'].apply(normalize_street_name)
    if 'name:he' in osm_gdf.columns:
        osm_gdf['normalized_name:he'] = osm_gdf['name:he'].apply(normalize_street_name)

    # Drop rows where the final normalized_name is null
    LAMAS_df.dropna(subset=['normalized_name'], inplace=True)
    osm_gdf.dropna(subset=['normalized_name'], inplace=True)

    # Check if all OSM streets were unnamed (and thus dropped)
    if len(osm_gdf) == 0:
        print(f"\nWARNING: All streets in {chosen_place_str} are unnamed")
        print("No streets can be matched without names.")
        return PipelineStatus.NO_DATA

    _save_intermediate_df(LAMAS_df, "step2_lamas_normalized", chosen_place_str)
    _save_intermediate_df(osm_gdf, "step2_osm_normalized", chosen_place_str)
    
    # STEP 3: Topology (Adjacency)
    print("\n[Step 3/7] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    # STEP 4: Candidate Matching (CREATES candidates_df)
    print("\n[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
    
    # OPTIMIZATION: Filter LAMAS data to the relevant city BEFORE fuzzy matching
    # This prevents matching against the entire country's street list
    osm_city_label = osm_gdf['city'].iloc[0]
    print(f"Filtering LAMAS data for city: '{osm_city_label}'")
    
    lamas_city_df = LAMAS_df[LAMAS_df['city'] == osm_city_label].copy()
    
    if lamas_city_df.empty:
        print(f"WARNING: No LAMAS data found for city '{osm_city_label}'. Fuzzy matching will fail.")
        # Fallback? Or just proceed (will result in MISSING)
    
    candidates_df = find_fuzzy_candidates(osm_gdf, lamas_city_df)
    
    _save_intermediate_df(candidates_df, "step4_candidates", chosen_place_str)

    # STEP 5: AI Resolution (CREATES ai_decisions_df)
    print("\n[Step 5/7] (Optional) Invoking AI for ambiguous cases...")
    ai_results = []

    if use_ai:
        # Initialize Local AI if requested
        local_resolver = None
        if use_local_ai:
            try:
                print("Initializing Local AI Resolver...")
                local_resolver = LocalAIResolver()
                if not local_resolver.is_available():
                    print("Local AI not available (is_available=False), will fall back to Gemini if API key exists.")
                    local_resolver = None
                else:
                    print("Local AI initialized successfully.")
            except Exception as e:
                print(f"Failed to initialize Local AI: {e}")
                local_resolver = None

        # Filter candidates to the city being processed
        osm_gdf_in_city = osm_gdf[osm_gdf['city'] == osm_city_label]
        
        # Merge candidates with normalized_name from osm_gdf to process by name
        ai_candidates_to_process = candidates_df[
            (candidates_df['status'] == 'NEEDS_AI') & 
            (candidates_df['osm_id'].isin(osm_gdf_in_city['osm_id']))
        ].merge(osm_gdf_in_city[['osm_id', 'normalized_name']], on='osm_id')

        # Get unique street names that need AI resolution
        unique_ai_streets = ai_candidates_to_process.drop_duplicates(subset=['normalized_name'])
        
        print(f"Found {len(unique_ai_streets)} unique street names requiring AI resolution.")

        # Cache for AI decisions to avoid re-processing the same name
        ai_decision_cache = {}

        for _, row in unique_ai_streets.iterrows():
            normalized_street_name = row['normalized_name']
            
            # Use the first osm_id as a representative for this street name (for logging/context)
            representative_osm_id = row['osm_id']

            # --- Information Gathering for the unique street ---
            # 1. Get all OSM IDs for this street name
            all_osm_ids_for_street = osm_gdf_in_city[osm_gdf_in_city['normalized_name'] == normalized_street_name]['osm_id'].tolist()
            
            # 2. Get all unique adjacent street names for this street
            all_adjacent_ids = set()
            for osm_id in all_osm_ids_for_street:
                all_adjacent_ids.update(map_of_adjacents.get(osm_id, []))
            
            adjacent_names = osm_gdf_in_city[osm_gdf_in_city['osm_id'].isin(all_adjacent_ids)]['normalized_name'].unique().tolist()

            # 3. All other info (candidates, city) is the same for the whole street
            lamas_candidates_str = row['all_candidates']
            city_name = osm_gdf_in_city['city'].iloc[0]


            ai_decision_id = 'None'
            confidence = 0.0
            reasoning = ""
            method = "none"

            # Try Local AI first
            if local_resolver:
                print(f" -> Consulting Local AI for street: '{normalized_street_name}'...")
                # We need to construct the candidates list for the local AI from the string
                local_lamas_candidates = []
                if pd.notna(lamas_candidates_str):
                    for line in lamas_candidates_str.split('\n'):
                        id_match = re.search(r'ID:\s*(\d+)', line)
                        name_match = re.search(r"Name:\s*['\"]([^'\"]+)['\"]", line)
                        score_match = re.search(r'Score:\s*([\d.]+)', line)
                        if id_match and name_match:
                            local_lamas_candidates.append({
                                'id': id_match.group(1), 'name': name_match.group(1), 
                                'score': float(score_match.group(1)) if score_match else 0.0
                            })
                
                local_result = local_resolver.resolve_street(
                    representative_osm_id, normalized_street_name, city_name,
                    local_lamas_candidates, adjacent_names
                )

                ai_decision_id = local_result.get('lamas_id')
                confidence = local_result.get('confidence', 0.0)
                reasoning = local_result.get('reasoning', '')
                method = "local_ai"
                
                if str(ai_decision_id) == 'None':
                     print(f"    Local AI found no match for '{normalized_street_name}'. Confidence: {confidence}")
                else:
                     print(f"    Local AI matched '{normalized_street_name}' to {ai_decision_id}. Confidence: {confidence}")

            # Fallback to Gemini if Local AI didn't run or found no match
            if (str(ai_decision_id) == 'None' or method == "none") and API_KEY:
                if method == "local_ai":
                     print(f"    Falling back to Gemini API for '{normalized_street_name}'...")
                
                prompt = (f"OSM Street Name: '{normalized_street_name}'. "
                          f"Adjacent Streets: {', '.join(adjacent_names) if adjacent_names else 'None'}. "
                          f"LAMAS Candidates: {lamas_candidates_str}. Choose the best LAMAS ID (number only or 'None').")

                ai_decision_id = get_ai_resolution(prompt, representative_osm_id) # Pass representative_osm_id for logging
                method = "gemini"
                confidence = 0.0 # Gemini function doesn't return confidence currently
            
            # Cache the decision for this unique street name
            ai_decision_cache[normalized_street_name] = {
                'ai_LAMAS_id': ai_decision_id,
                'ai_confidence': confidence,
                'ai_reasoning': reasoning,
                'ai_method': method
            }

        # Now, apply the cached decisions to all relevant segments
        for _, row in ai_candidates_to_process.iterrows():
            street_name = row['normalized_name']
            cached_result = ai_decision_cache.get(street_name)
            if cached_result:
                ai_results.append({
                    'osm_id': row['osm_id'],
                    **cached_result
                })
    else:
        # AI disabled — produce empty results
        ai_results = []

    # ensure DataFrame has expected columns even if empty
    ai_decisions_df = pd.DataFrame(ai_results, columns=['osm_id', 'ai_LAMAS_id', 'ai_confidence', 'ai_reasoning', 'ai_method'])
    _save_intermediate_df(ai_decisions_df, "step5_ai_decisions", chosen_place_str)

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
        ((ai_decisions_merged['status'] == 'NEEDS_AI') & 
         (ai_decisions_merged['ai_LAMAS_id'].astype(str) != 'None') & 
         (ai_decisions_merged['ai_LAMAS_id'].astype(str) != 'nan'))
    ].copy()

    # Determine the final ID source
    final_mapping_df['final_LAMAS_id'] = final_mapping_df.apply(
        lambda row: row['best_LAMAS_id'] if row['status'] == 'CONFIDENT' else row['ai_LAMAS_id'], axis=1
    ).astype(str)

    # Select key diagnostic columns from candidates/AI merge
    diagnostic_cols = ['osm_id', 'status', 'best_score', 'best_LAMAS_name', 'all_candidates', 'ai_LAMAS_id', 'ai_reasoning']
    
    # Create the full diagnostic table by merging OSM data with the candidates/AI data
    osm_cols_for_diag = ['osm_id', 'osm_name', 'normalized_name', 'city', 'geometry']
    if 'osm_name_original' in osm_gdf.columns:
        osm_cols_for_diag.insert(2, 'osm_name_original')  # Insert after osm_name
    if 'name:he' in osm_gdf.columns:
        osm_cols_for_diag.insert(2, 'name:he')  # Insert after osm_name

    diagnostic_df_full = osm_gdf[osm_cols_for_diag].merge(
        ai_decisions_merged[diagnostic_cols], on='osm_id', how='left'
    )
    
    # Merge the final LAMAS ID
    diagnostic_df_full = diagnostic_df_full.merge(
        final_mapping_df[['osm_id', 'final_LAMAS_id']], on='osm_id', how='left'
    )

    # Export final diagnostic report to CSV (new comprehensive file)
    _save_intermediate_df(diagnostic_df_full, "diagnostic_report", chosen_place_str)
    
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

    diagnostics = calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf)
    print(f"-> Diagnostic Summary: {diagnostics}")

    # STEP 7: Generate HTML Visualization
    if not skip_html:
        print("\n[Step 7/7] Generating HTML visualization of all streets...")
        try:
            from generate_html import create_html_from_gdf
            os.makedirs("HTML", exist_ok=True)
            # Pass the GeoDataFrame and the new diagnostics object
            create_html_from_gdf(diagnostic_df_full, chosen_place_str, diagnostics)
        except Exception as e:
            print(f"Warning: failed to generate HTML visualization: {e}")
    
    return PipelineStatus.SUCCESS  # Indicate successful completion


if __name__ == "__main__":
    place_arg = None
    force = False
    no_local_ai = False
    if len(sys.argv) > 1:
        place_arg = sys.argv[1]
    if "--refresh" in sys.argv:
        force = True
    no_ai = False
    if "--no-ai" in sys.argv:
        no_ai = True
    if "--no-local-ai" in sys.argv:
        no_local_ai = True

    run_pipeline(place=place_arg, force_refresh=force, use_ai=(not no_ai), use_local_ai=(not no_local_ai))