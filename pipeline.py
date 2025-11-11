import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import numpy as np
import requests
import time
import json
from fuzzywuzzy import fuzz

# --- הגדרות API ---
# קריאה למפתח ה-API ממשתנה הסביבה GEMINI_API_KEY
API_KEY = os.getenv("GEMINI_API_KEY") 
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={API_KEY}"
MAX_RETRIES = 3

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
    system_prompt = "אתה מערכת GIS אוטומטית שתפקידה למצוא את המזהה המספרי היחיד של רחוב (LAMS ID) מתוך רשימת מועמדים, על בסיס שם וקונטקסט גיאוגרפי (שמות רחובות משיקים). השב עם המספר של ה-ID בלבד, או עם המילה 'None' אם לא נמצאה התאמה ודאית."
    
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


# --- 1. פונקציות שליפת דאטה: השתמש בפונקציות מתוך המודולים החיצוניים ---
# יבוא פונקציות לשליפת נתוני הלמ"ס ו-OSM המוגדרות בקבצים הפרוייקט
from lamas_streets import fetch_all_lams_data
from OSM_streets import fetch_osm_street_data, place_name as OSM_PLACE_NAME

# --- 2. פונקציות הנרמול (normalization.py) ---
def normalize_street_name(name):
    """הפונקציה מנרמלת את שמות הרחובות (מטפלת בקיצורים, פיסוק ורווחים)."""
    if pd.isna(name) or name is None: return None
    name = str(name).strip()
    replacements = {r'\bשד[\'|\.]': 'שדרות', r'\bרח[\'|\.]': 'רחוב'}
    for old, new in replacements.items():
        name = name.replace(old, new).replace("'", "").replace(".", "")
    name = name.replace('-', ' ').strip()
    return name

# --- 3. טופולוגיה (map_of_adjacents.py) ---
def build_adjacency_map(osm_gdf):
    """מייצר מפת סמיכות של מזהי OSM (מי משיק למי)."""
    print("Building Adjacency Map (Topology)...")
    # בפועל, זו תהיה לוגיקה גיאומטרית מורכבת על ה-GeoDataFrame
    return {
        '5001-A': [],
        '5002-B': ['5003-C'], 
        '5003-C': ['5002-B', '5005-E'],
        '5004-D': [],
        '5005-E': ['5003-C']
    }

# --- 4. התאמת מועמדים (find_fuzzy_candidates) ---
def find_fuzzy_candidates(osm_df, lams_df):
    """
    מבצע Fuzzy Matching מתוחכם ובוחר את המועמדים המובילים לכל רחוב ב-OSM.
    """
    print("Executing Fuzzy Matching and Candidate Selection...")
    candidates = []
    
    # 1. איטרציה על כל רחוב ב-OSM
    for _, osm_row in osm_df.iterrows():
        osm_id = osm_row['osm_id']
        osm_name = osm_row['normalized_name']
        osm_city = osm_row['city'] 

        # 2. סינון רחובות הלמ"ס לאותה עיר (חיסכון במשאבי חישוב)
        lams_subset = lams_df[lams_df['city'] == osm_city].copy() # שימוש ב-copy כדי למנוע SettingWithCopyWarning
        
        # 3. חישוב ציוני דמיון
        scores = []
        for _, lams_row in lams_subset.iterrows():
            lams_id = lams_row['lams_id']
            lams_name = lams_row['normalized_name']
            
            # חישוב 3 מדדים שונים:
            ratio = fuzz.ratio(osm_name, lams_name)
            token_sort = fuzz.token_sort_ratio(osm_name, lams_name)
            token_set = fuzz.token_set_ratio(osm_name, lams_name) # הכי חשוב לשמות חלקיים
            
            # ניקוד ממוצע משוקלל
            weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

            scores.append({
                'lams_id': lams_id,
                'lams_name': lams_name,
                'weighted_score': weighted_score,
                'token_set_score': token_set
            })

        # 4. סינון וסיווג: בחירת המועמדים הטובים ביותר
        if not scores:
             candidates.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_lams_id': None,
                'best_score': 0,
                'all_candidates': None
            })
             continue
             
        scores_df = pd.DataFrame(scores).sort_values(by='weighted_score', ascending=False)
        
        # התאמה ודאית: אם יש ציון גבוה מאוד (מעל 98, לדוגמה)
        confident_match = scores_df[scores_df['weighted_score'] >= 98].head(1)
        if not confident_match.empty:
            candidates.append({
                'osm_id': osm_id,
                'status': 'CONFIDENT',
                'best_lams_id': confident_match.iloc[0]['lams_id'],
                'best_score': confident_match.iloc[0]['weighted_score'],
                'all_candidates': None
            })
            continue

        # מועמדים ל-AI: אם יש ציונים סבירים (בין 80 ל-98)
        ai_candidates = scores_df[(scores_df['weighted_score'] >= 80) & (scores_df['weighted_score'] < 98)].head(5).copy() 
        
        if not ai_candidates.empty:
            # איסוף פרטי המועמדים לטקסט
            candidate_list = ai_candidates.apply(
                lambda r: f"ID: {r['lams_id']}, Name: '{r['lams_name']}' (Score: {r['weighted_score']:.2f})", 
                axis=1
            ).tolist()
            
            candidates.append({
                'osm_id': osm_id,
                'status': 'NEEDS_AI',
                'best_lams_id': None,
                'best_score': ai_candidates.iloc[0]['weighted_score'],
                'all_candidates': "\n".join(candidate_list)
            })
        
        # רחובות ללא התאמה: יטופלו כ-Missing later
        else:
            candidates.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_lams_id': None,
                'best_score': scores_df.iloc[0]['weighted_score'] if not scores_df.empty else 0,
                'all_candidates': None
            })

    return pd.DataFrame(candidates)

# ----------------------------------------------------------------------------------
#                                 ORCHESTRATION START
# ----------------------------------------------------------------------------------

def run_pipeline():
    """
    מארגן את כל ה-pipeline למיפוי מזהי הרחובות.
    """
    print("--- Starting Street Mapping Orchestrator ---")
    if not API_KEY:
        print("\n*** WARNING: GEMINI_API_KEY environment variable not set. AI resolution will be skipped. ***")


    # STEP 1: Data Acquisition
    try:
        # כאן יש להטמיע את הייבוא: from lamas_streets import fetch_all_lams_data
        # ו- from OSM_streets import fetch_osm_street_data
        lams_df = fetch_all_lams_data() # מחזיר נתונים אמיתיים
        # fetch_osm_street_data expects a 'place' argument; use the module's default place name
        osm_gdf = fetch_osm_street_data(OSM_PLACE_NAME)
    except Exception as e:
        print(f"FATAL ERROR during Data Acquisition: {e}")
        return

    # STEP 2: Preprocessing and Normalization
    print("\n[Step 2/6] Normalizing street names...")
    lams_df['normalized_name'] = lams_df['lams_name'].apply(normalize_street_name)
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
    
    lams_df.dropna(subset=['normalized_name'], inplace=True)
    osm_gdf.dropna(subset=['normalized_name'], inplace=True)
    
    # STEP 3: Topology (Adjacency)
    print("\n[Step 3/6] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    # STEP 4: Candidate Matching (CREATES candidates_df)
    print("\n[Step 4/6] Running Fuzzy Matching and Candidate Creation...")
    candidates_df = find_fuzzy_candidates(osm_gdf, lams_df)

    # STEP 5: AI Resolution (CREATES ai_decisions_df)
    print("\n[Step 5/6] Invoking Real AI for ambiguous cases (This will take time)...")
    ai_candidates_to_process = candidates_df[candidates_df['status'] == 'NEEDS_AI'].copy()
    
    ai_results = []
    
    # הלולאה הזו מבצעת קריאות API אמיתיות וממתינה להכרעת ה-AI
    for _, row in ai_candidates_to_process.iterrows():
        osm_id = row['osm_id']
        
        # בניית הפרומפט המלא (כולל קונטקסט טופולוגי)
        osm_street_name = osm_gdf[osm_gdf['osm_id'] == osm_id]['normalized_name'].iloc[0]
        adjacent_names = osm_gdf[osm_gdf['osm_id'].isin(map_of_adjacents.get(osm_id, []))]['normalized_name'].tolist()

        prompt = (f"OSM Street Name: '{osm_street_name}'. "
                  f"Adjacent Streets: {', '.join(adjacent_names) if adjacent_names else 'None'}. "
                  f"LAMS Candidates: {row['all_candidates']}. Choose the best LAMS ID (number only or 'None').")
        
        ai_decision_id = get_ai_resolution(prompt, osm_id)
        
        ai_results.append({'osm_id': osm_id, 'ai_lams_id': ai_decision_id})

    ai_decisions_df = pd.DataFrame(ai_results)

    # STEP 6: Final Merge and Mapping
    print("\n[Step 6/6] Merging results to create final mapping table...")
    
    # Merge AI decisions back into the candidates table
    ai_decisions_merged = candidates_df.merge(ai_decisions_df, on='osm_id', how='left')

    # Start with Confident matches
    final_mapping_df = ai_decisions_merged[ai_decisions_merged['status'] == 'CONFIDENT'].copy()
    # המרת ל-str כדי שנוכל לחבר עם תוצאות ה-AI
    final_mapping_df['final_lams_id'] = final_mapping_df['best_lams_id'].astype(str)
    final_mapping_df = final_mapping_df[['osm_id', 'final_lams_id']]

    # Append AI Resolved matches
    ai_resolved_rows = ai_decisions_merged[ai_decisions_merged['status'] == 'NEEDS_AI'].copy()
    ai_resolved_rows['final_lams_id'] = ai_resolved_rows['ai_lams_id'].astype(str)
    
    # Filter out cases where AI returned 'None'
    ai_resolved_rows = ai_resolved_rows[ai_resolved_rows['final_lams_id'] != 'None']

    # Combine both sets of successful matches
    final_mapping_df = pd.concat([
        final_mapping_df, 
        ai_resolved_rows[['osm_id', 'final_lams_id']]
    ], ignore_index=True)
    
    # Final Summary
    osm_gdf_final = osm_gdf.merge(final_mapping_df, on='osm_id', how='left')

    print("\n-----------------------------------------------------")
    print("                 PIPELINE COMPLETED                  ")
    print("-----------------------------------------------------")
    print(f"Total OSM streets considered: {len(osm_gdf_final)}")
    
    matched_count = final_mapping_df.shape[0]
    unmatched_count = len(osm_gdf_final) - matched_count
    
    print(f"Total successfully matched (Confident + AI): {matched_count}")
    print(f"Total unmatched (Missing/AI rejected): {unmatched_count}")
    print("\n--- Final Mapping Result (OSM ID -> LAMS ID) ---")
    print(osm_gdf_final[['osm_id', 'osm_name', 'final_lams_id']].dropna())

if __name__ == "__main__":
    run_pipeline()