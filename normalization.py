import re
import pandas as pd
from fuzzywuzzy import fuzz
import numpy as np

def normalize_street_name(name):
    """הפונקציה מנרמלת את שמות הרחובות (מטפלת בקיצורים, פיסוק ורווחים)."""
    if pd.isna(name) or name is None: return None
    name = str(name).strip()
    
    # Use regex for replacements to handle variations like 'שד.', 'שד'', 'שד'
    replacements = {
        r"\bשד['\.]?\b": 'שדרות',  # Handles שד, שד. and שד' as whole words
        r"\bרח['\.]?\b": 'רחוב',   # Handles רח, רח. and רח' as whole words
        r"\bכי['\.]?\b": 'כיכר',   # Handles כי, כי. and כי' as whole words
    }
    for old, new in replacements.items():
        name = re.sub(old, new, name, flags=re.I) # flags=re.I for case-insensitive replacement

    # Clean up leftover apostrophes from abbreviations like רח'
    name = name.replace("'", "")
    
    # Remove Hebrew gershayim (double quotes) used in abbreviations like רש"י, רמב"ם
    # This is critical for matching abbreviated names of rabbis/scholars
    name = name.replace('"', '')

    # 2. הסרת סימני פיסוק מיותרים והחלפתם ברווח
    # מטפל ב: ., -
    name = re.sub(r'[.,-]', ' ', name)
    
    # 3. ניקוי רווחים כפולים
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def find_fuzzy_candidates(osm_df, LAMAS_df):
    """
    מבצע Fuzzy Matching מתוחכם ובוחר את המועמדים המובילים לכל רחוב ב-OSM.
    """
    print("Executing Fuzzy Matching and Candidate Selection...")
    candidates = []
    
    # Statistics for threshold analysis
    all_best_scores = []  # Track best score for each OSM street
    
    # Optimization: Pre-filter LAMAS data by city to avoid repeated filtering inside the loop
    # Group OSM data by city to process each city's streets against its relevant LAMAS records
    osm_grouped = osm_df.groupby('city')
    
    from tqdm import tqdm
    
    for city, city_osm_df in osm_grouped:
        print(f"Processing {len(city_osm_df)} streets in {city}...")
        
        # Filter LAMAS data for this city ONCE
        city_lamas_df = LAMAS_df[LAMAS_df['city'] == city].copy()
        
        if city_lamas_df.empty:
            print(f"Warning: No LAMAS data found for city '{city}'")
            # Handle case where no LAMAS data exists for the city
            for _, osm_row in city_osm_df.iterrows():
                candidates.append({
                    'osm_id': osm_row['osm_id'],
                    'status': 'MISSING',
                    'best_LAMAS_id': None,
                    'best_LAMAS_name': None,
                    'best_score': 0,
                    'all_candidates': None
                })
            continue

        # Iterate over streets in this city
        # Optimization: Process unique OSM names instead of all segments
        # This drastically reduces runtime for large cities with many segments per street
        unique_osm_names = city_osm_df[['normalized_name', 'osm_name']].drop_duplicates('normalized_name')
        print(f"Optimized Matching: Processing {len(unique_osm_names)} unique names (out of {len(city_osm_df)} total segments) for {city}")
        
        name_to_results = {}
        
        # Iterate over unique names
        for _, row in tqdm(unique_osm_names.iterrows(), total=len(unique_osm_names), desc=f"Fuzzy Matching (Unique Names) for {city}"):
            osm_name = row['normalized_name']
            
            scores_for_unique_name = []
            for _, LAMAS_row in city_lamas_df.iterrows():
                LAMAS_id = LAMAS_row['LAMAS_id']
                LAMAS_name_raw = LAMAS_row['LAMAS_name']
                LAMAS_name = LAMAS_row['normalized_name']
                
                ratio = fuzz.ratio(osm_name, LAMAS_name)
                token_sort = fuzz.token_sort_ratio(osm_name, LAMAS_name)
                token_set = fuzz.token_set_ratio(osm_name, LAMAS_name)
                
                weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

                scores_for_unique_name.append({
                    'LAMAS_id': LAMAS_id,
                    'LAMAS_name': LAMAS_name_raw,
                    'weighted_score': weighted_score,
                    'token_set_score': token_set
                })

            if not scores_for_unique_name:
                name_to_results[osm_name] = {
                    'status': 'MISSING',
                    'best_LAMAS_id': None,
                    'best_LAMAS_name': None,
                    'best_score': 0,
                    'all_candidates': None
                }
                continue
            
            scores_df = pd.DataFrame(scores_for_unique_name).sort_values(by='weighted_score', ascending=False)
            
            # Logic for classification
            result = {}
            
            # CONFIDENT
            confident_match = scores_df[scores_df['weighted_score'] >= 90].head(1)
            if not confident_match.empty:
                result = {
                    'status': 'CONFIDENT',
                    'best_LAMAS_id': confident_match.iloc[0]['LAMAS_id'],
                    'best_LAMAS_name': confident_match.iloc[0]['LAMAS_name'],
                    'best_score': confident_match.iloc[0]['weighted_score'],
                    'all_candidates': None
                }
            else:
                # NEEDS_AI
                ai_candidates = scores_df[
                    (scores_df['weighted_score'] >= 80) & 
                    (scores_df['weighted_score'] < 90)
                ].head(5).copy()
                
                if not ai_candidates.empty:
                    candidate_list = ai_candidates.apply(
                        lambda r: f"ID: {r['LAMAS_id']}, Name: '{r['LAMAS_name']}' (Score: {r['weighted_score']:.2f})", 
                        axis=1
                    ).tolist()
                    
                    result = {
                        'status': 'NEEDS_AI',
                        'best_LAMAS_id': ai_candidates.iloc[0]['LAMAS_id'],
                        'best_LAMAS_name': ai_candidates.iloc[0]['LAMAS_name'],
                        'best_score': ai_candidates.iloc[0]['weighted_score'],
                        'all_candidates': "\n".join(candidate_list)
                    }
                else:
                    # MISSING
                    result = {
                        'status': 'MISSING',
                        'best_LAMAS_id': None,
                        'best_LAMAS_name': scores_df.iloc[0]['LAMAS_name'] if not scores_df.empty else None,
                        'best_score': scores_df.iloc[0]['weighted_score'] if not scores_df.empty else 0,
                        'all_candidates': None
                    }
            
            name_to_results[osm_name] = result

        # Map results back to all OSM segments
        print(f"Mapping results to {len(city_osm_df)} segments...")
        for _, osm_row in city_osm_df.iterrows():
            osm_id = osm_row['osm_id']
            osm_name = osm_row['normalized_name']
            
            res = name_to_results.get(osm_name, {
                'status': 'MISSING',
                'best_LAMAS_id': None,
                'best_LAMAS_name': None,
                'best_score': 0,
                'all_candidates': None
            })
            
            candidates.append({
                'osm_id': osm_id,
                'status': res['status'],
                'best_LAMAS_id': res['best_LAMAS_id'],
                'best_LAMAS_name': res['best_LAMAS_name'],
                'best_score': res['best_score'],
                'all_candidates': res['all_candidates']
            })
            
            # Track statistics
            all_best_scores.append(res['best_score'])


    # Print statistics about score distribution
    print("\n" + "="*60)
    print("FUZZY MATCHING THRESHOLD ANALYSIS")
    print("="*60)
    if all_best_scores:
        all_best_scores_array = np.array(all_best_scores)
        print(f"Total streets processed: {len(all_best_scores)}")
        print(f"\nScore Distribution:")
        print(f"  Mean score: {all_best_scores_array.mean():.2f}")
        print(f"  Median score: {np.median(all_best_scores_array):.2f}")
        print(f"  Std deviation: {all_best_scores_array.std():.2f}")
        print(f"\nPercentiles:")
        for p in [10, 25, 50, 75, 90, 95, 99]:
            print(f"  {p}th percentile: {np.percentile(all_best_scores_array, p):.2f}")
        
        print(f"\nCurrent Thresholds Impact:")
        confident = sum(1 for s in all_best_scores if s >= 90)
        needs_ai = sum(1 for s in all_best_scores if 80 <= s < 90)
        missing = sum(1 for s in all_best_scores if s < 80)
        print(f"  CONFIDENT (≥90): {confident} streets ({100*confident/len(all_best_scores):.1f}%)")
        print(f"  NEEDS_AI (80-90): {needs_ai} streets ({100*needs_ai/len(all_best_scores):.1f}%)")
        print(f"  MISSING (<80): {missing} streets ({100*missing/len(all_best_scores):.1f}%)")
        
        print(f"\nAlternative Threshold Scenarios:")
        for threshold in [70, 75, 80, 85]:
            count = sum(1 for s in all_best_scores if s >= threshold)
            print(f"  If threshold was {threshold}: {count} streets would qualify ({100*count/len(all_best_scores):.1f}%)")
    print("="*60 + "\n")

    return pd.DataFrame(candidates)

def prepare_ai_prompt(osm_id, candidates_str, map_of_adjacents, osm_df):
    """
    מרכיב את הפרומפט המלא עבור מודל ה-AI.
    """
    osm_row = osm_df[osm_df['osm_id'] == osm_id].iloc[0]
    
    osm_name = osm_row['normalized_name']
    osm_city = osm_row['city']
    
    # מציאת שמות הרחובות המשיקים
    adjacent_osm_ids = map_of_adjacents.get(osm_id, [])
    adjacent_names = osm_df[osm_df['osm_id'].isin(adjacent_osm_ids)]['normalized_name'].tolist()
    
    # בניית הטקסט הסופי
    prompt = f"""
    הערך שוויון ערך (Synonymity) והתאמה של רחוב ב-OSM ל-ID של רחוב בלמ"ס.
    ההכרעה נדרשת בגלל שוני בשם (כמו שם חלקי או כינוי).
    
    פרטי רחוב OSM:
    - עיר: {osm_city}
    - שם ב-OSM: '{osm_name}'
    - רחובות משיקים ב-OSM (קונטקסט טופולוגי): {', '.join(adjacent_names) if adjacent_names else 'אין'}
    
    מועמדים ל-ID של הלמ"ס:
    {candidates_str}
    
    על בסיס הקונטקסט הטופולוגי ומידת הדמיון הטקסטואלי (שם חלקי / כינוי), בחר את ה-ID היחיד המתאים ביותר.
    השב עם ה-ID בלבד. אם אין התאמה ודאית, השב 'None'.
    """
    return prompt.strip()
