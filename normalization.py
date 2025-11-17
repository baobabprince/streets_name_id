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

    # 2. הסרת סימני פיסוק מיותרים והחלפתם ברווח
    # מטפל ב: ., -
    name = re.sub(r'[.,-]', ' ', name)
    
    # 3. ניקוי רווחים כפולים
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def _calculate_scores(osm_name, lamas_subset):
    """Helper function to calculate fuzzy scores for a single OSM name against a LAMAS subset."""
    if not osm_name or pd.isna(osm_name):
        return []

    scores = []
    for _, lamas_row in lamas_subset.iterrows():
        lamas_id = lamas_row['LAMAS_id']
        lamas_name_raw = lamas_row['LAMAS_name']
        lamas_name_normalized = lamas_row['normalized_name']

        ratio = fuzz.ratio(osm_name, lamas_name_normalized)
        token_sort = fuzz.token_sort_ratio(osm_name, lamas_name_normalized)
        token_set = fuzz.token_set_ratio(osm_name, lamas_name_normalized)

        weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

        scores.append({
            'LAMAS_id': lamas_id,
            'LAMAS_name': lamas_name_raw,
            'weighted_score': weighted_score,
            'token_set_score': token_set,
            'match_source': osm_name  # Track which OSM name produced the match
        })
    return scores

def find_fuzzy_candidates(osm_df, LAMAS_df):
    """
    Performs prioritized fuzzy matching. It tries the Hebrew name first,
    and if no confident match is found, it falls back to all other available names.
    """
    print("Executing Prioritized Fuzzy Matching and Candidate Selection...")

    # Ensure osm_df is not empty to avoid errors
    if osm_df.empty:
        print("Warning: osm_df is empty. Returning an empty candidates DataFrame.")
        # Return a DataFrame with the expected columns to prevent downstream KeyErrors
        return pd.DataFrame(columns=['osm_id', 'status', 'best_LAMAS_id', 'best_LAMAS_name', 'best_score', 'all_candidates'])

    candidates = []
    
    # Identify all normalized name columns available in the OSM DataFrame
    normalized_name_cols = [col for col in osm_df.columns if col.startswith('normalized_')]
    print(f"Found normalized name columns: {normalized_name_cols}")

    for _, osm_row in osm_df.iterrows():
        osm_id = osm_row['osm_id']
        osm_city = osm_row['city']
        
        lamas_subset = LAMAS_df[LAMAS_df['city'] == osm_city].copy()

        if lamas_subset.empty:
            # If there are no LAMAS streets for this city, mark as missing and continue
            candidates.append({'osm_id': osm_id, 'status': 'MISSING', 'best_LAMAS_id': None, 'best_LAMAS_name': None, 'best_score': 0, 'all_candidates': None})
            continue

        all_scores = []

        # --- Prioritized Matching ---
        # 1. Try Hebrew name first
        hebrew_name = osm_row.get('normalized_name:he')
        if hebrew_name:
            hebrew_scores = _calculate_scores(hebrew_name, lamas_subset)
            all_scores.extend(hebrew_scores)
            
            # Check for a confident match from Hebrew scores
            confident_hebrew_match = [s for s in hebrew_scores if s['weighted_score'] >= 90]
            if confident_hebrew_match:
                best_match = max(confident_hebrew_match, key=lambda x: x['weighted_score'])
                candidates.append({
                    'osm_id': osm_id,
                    'status': 'CONFIDENT',
                    'best_LAMAS_id': best_match['LAMAS_id'],
                    'best_LAMAS_name': best_match['LAMAS_name'],
                    'best_score': best_match['weighted_score'],
                    'all_candidates': None
                })
                continue # Move to the next OSM street

        # 2. Fallback: If no confident Hebrew match, use all available names
        fallback_names = [
            osm_row.get(col) for col in normalized_name_cols
            if col != 'normalized_name:he' and pd.notna(osm_row.get(col))
        ]

        for name in set(fallback_names): # Use set to avoid re-scoring duplicate names
            all_scores.extend(_calculate_scores(name, lamas_subset))

        # --- Classification based on aggregated scores ---
        if not all_scores:
            candidates.append({'osm_id': osm_id, 'status': 'MISSING', 'best_LAMAS_id': None, 'best_LAMAS_name': None, 'best_score': 0, 'all_candidates': None})
            continue

        scores_df = pd.DataFrame(all_scores).sort_values(by='weighted_score', ascending=False).drop_duplicates(subset=['LAMAS_id'])
        
        if scores_df.empty:
            candidates.append({'osm_id': osm_id, 'status': 'MISSING', 'best_LAMAS_id': None, 'best_LAMAS_name': None, 'best_score': 0, 'all_candidates': None})
            continue

        # Confident match from fallback names
        confident_match = scores_df[scores_df['weighted_score'] >= 90].head(1)
        if not confident_match.empty:
            candidates.append({
                'osm_id': osm_id,
                'status': 'CONFIDENT',
                'best_LAMAS_id': confident_match.iloc[0]['LAMAS_id'],
                'best_LAMAS_name': confident_match.iloc[0]['LAMAS_name'],
                'best_score': confident_match.iloc[0]['weighted_score'],
                'all_candidates': None
            })
            continue

        # AI candidates from all names
        ai_candidates = scores_df[(scores_df['weighted_score'] >= 80) & (scores_df['weighted_score'] < 90)].head(5)
        if not ai_candidates.empty:
            candidate_list = ai_candidates.apply(lambda r: f"ID: {r['LAMAS_id']}, Name: '{r['LAMAS_name']}' (Score: {r['weighted_score']:.2f})", axis=1).tolist()
            candidates.append({
                'osm_id': osm_id,
                'status': 'NEEDS_AI',
                'best_LAMAS_id': ai_candidates.iloc[0]['LAMAS_id'],
                'best_LAMAS_name': ai_candidates.iloc[0]['LAMAS_name'],
                'best_score': ai_candidates.iloc[0]['weighted_score'],
                'all_candidates': "\n".join(candidate_list)
            })
        else:
            candidates.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_LAMAS_id': None,
                'best_LAMAS_name': None,
                'best_score': scores_df.iloc[0]['weighted_score'],
                'all_candidates': None
            })

    # Final check: if candidates list is empty, return a DataFrame with the correct structure
    if not candidates:
        print("Warning: No candidates were generated. Returning an empty DataFrame with the correct structure.")
        return pd.DataFrame(columns=['osm_id', 'status', 'best_LAMAS_id', 'best_LAMAS_name', 'best_score', 'all_candidates'])

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
