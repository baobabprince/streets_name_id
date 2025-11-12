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
                # Handling response structure from Google Gen AI
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
    #                                 ORCHESTRATION START
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

            # Normalize city name formatting in both dataframes (strip, normalize dashes/spaces)
            def _normalize_city(col: pd.Series) -> pd.Series:
                return (
                    col.astype(str)
                    .str.replace(r'[\u2010\u2011\u2012\u2013\u2014\-]', ' ', regex=True)  # various dashes -> space
                    .str.replace(r'\s+', ' ', regex=True)
                    .str.strip()
                )

            if 'city' in LAMAS_df.columns:
                LAMAS_df['city'] = _normalize_city(LAMAS_df['city'])
            osm_gdf['city'] = _normalize_city(osm_gdf['city'])
        except Exception as e:
            print(f"FATAL ERROR during Data Acquisition: {e}")
            return

        # STEP 2: Preprocessing and Normalization
        print("\n[Step 2/7] Normalizing street names...")
        LAMAS_df['normalized_name'] = LAMAS_df['LAMAS_name'].apply(normalize_street_name)
        osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
        
        LAMAS_df.dropna(subset=['normalized_name'], inplace=True)
        osm_gdf.dropna(subset=['normalized_name'], inplace=True)
        
        # STEP 3: Topology (Adjacency)
        print("\n[Step 3/7] Building Adjacency Map (Topology)...")
        map_of_adjacents = build_adjacency_map(osm_gdf)

        # STEP 4: Candidate Matching (CREATES candidates_df)
        print("\n[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
        candidates_df = find_fuzzy_candidates(osm_gdf, LAMAS_df)

        # STEP 5: AI Resolution (CREATES ai_decisions_df)
        print("\n[Step 5/7] (Optional) Invoking Real AI for ambiguous cases — skipped unless enabled and API key present.")
        ai_results = []

        if use_ai and API_KEY:
            ai_candidates_to_process = candidates_df[candidates_df['status'] == 'NEEDS_AI'].copy()
            for _, row in ai_candidates_to_process.iterrows():
                osm_id = row['osm_id']

                # בניית הפרומפט המלא (כולל קונטקסט טופולוגי)
                osm_street_name = osm_gdf[osm_gdf['osm_id'] == osm_id]['normalized_name'].iloc[0]
                adjacent_names = osm_gdf[osm_gdf['osm_id'].isin(map_of_adjacents.get(osm_id, []))]['normalized_name'].tolist()

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

        # STEP 6: Final Merge and Mapping
        print("\n[Step 6/7] Merging results to create final mapping table...")
        
        # Merge AI decisions back into the candidates table
        ai_decisions_merged = candidates_df.merge(ai_decisions_df, on='osm_id', how='left')

        # --- התחלת התיקון: ודא סוגי נתונים אחידים ---

        # 1. הכן את final_mapping_df (המסגרת עם ה-LAMAS ID הממופה)
        final_mapping_df = ai_decisions_merged[
            (ai_decisions_merged['status'] == 'CONFIDENT') | 
            ((ai_decisions_merged['status'] == 'NEEDS_AI') & (ai_decisions_merged['ai_LAMAS_id'] != 'None'))
        ].copy()

        # המרת OSM ID ו-LAMAS ID למחרוזות כדי למנוע כשלון במיזוג
        final_mapping_df['osm_id'] = final_mapping_df['osm_id'].astype(str)
        final_mapping_df['final_LAMAS_id'] = final_mapping_df['best_LAMAS_id'].astype(str).fillna(
            final_mapping_df['ai_LAMAS_id'].astype(str)
        )

        # בחר רק את העמודות הסופיות
        final_mapping_df = final_mapping_df[['osm_id', 'final_LAMAS_id']]

        # 2. הכן את GeoDataFrame הבסיסי (ודא ש-osm_id הוא מחרוזת גם כאן)
        osm_gdf['osm_id'] = osm_gdf['osm_id'].astype(str) # <-- חשוב! המרת ה-ID המקורי למחרוזת
        
        # 3. המיזוג הסופי
        osm_gdf_final = osm_gdf.merge(final_mapping_df, on='osm_id', how='left')

        print("\n-----------------------------------------------------")
        print("                 PIPELINE COMPLETED                  ")
        print("-----------------------------------------------------")
        print(f"Total OSM streets considered: {len(osm_gdf_final)}")
        
        matched_count = final_mapping_df.shape[0]
        unmatched_count = len(osm_gdf_final) - matched_count
        
        print(f"Total successfully matched (Confident + AI): {matched_count}")
        print(f"Total unmatched (Missing/AI rejected): {unmatched_count}")
        print("\n--- Final Mapping Result (OSM ID -> LAMAS ID) ---")
        print(osm_gdf_final[['osm_id', 'osm_name', 'final_LAMAS_id']].dropna())

        # Export final mapping to CSV for inspection
        try:
            safe_place = _safe_place_name(chosen_place)
            export_path = os.path.join(CACHE_DIR, f"final_mapping_{safe_place}.csv")
            export_df = osm_gdf_final[['osm_id', 'osm_name', 'final_LAMAS_id']].copy()
            export_df.to_csv(export_path, index=False)
            print(f"\nFinal mapping exported to: {export_path}")
        except Exception as e:
            print(f"Warning: failed to export final mapping CSV: {e}")

        # STEP 7: Generate SVG Visualization
        print("\n[Step 7/7] Generating SVG visualization of all streets...")
        try:
            # Ensure the SVG directory exists
            from generate_svg import create_svg_from_gdf # FIX: Moved this import to the top
            os.makedirs("SVG", exist_ok=True)
            create_svg_from_gdf(osm_gdf_final, chosen_place)
        except Exception as e:
            print(f"Warning: failed to generate SVG visualization: {e}")

    if __name__ == "__main__":
        # Allow optional CLI args: first arg = place, --refresh to force re-download
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