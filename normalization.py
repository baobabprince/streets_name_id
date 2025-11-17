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

def find_fuzzy_candidates(osm_df, LAMAS_df):
    """
    Performs sophisticated Fuzzy Matching using all available OSM names and selects the top candidates for each OSM street.
    """
    print("Executing Fuzzy Matching and Candidate Selection...")
    candidates = []
    
    # 1. Iterate over each street in OSM
    for _, osm_row in osm_df.iterrows():
        osm_id = osm_row['osm_id']
        osm_city = osm_row['city']
        all_osm_names = osm_row.get('all_osm_names')

        # If there are no names for the street, mark as MISSING and continue
        if not all_osm_names:
            candidates.append({
                'osm_id': osm_id, 'status': 'MISSING', 'best_LAMAS_id': None,
                'best_LAMAS_name': None, 'best_score': 0, 'all_candidates': None,
                'matched_osm_name': None
            })
            continue

        # 2. Filter LAMAS streets for the same city
        LAMAS_subset = LAMAS_df[LAMAS_df['city'] == osm_city].copy()
        
        # 3. Calculate similarity scores for each OSM name against all LAMAS names
        best_match_for_street = None

        for osm_name in all_osm_names:
            normalized_osm_name = normalize_street_name(osm_name)
            if not normalized_osm_name:
                continue

            for _, lamas_row in LAMAS_subset.iterrows():
                lamas_id = lamas_row['LAMAS_id']
                lamas_name_raw = lamas_row['LAMAS_name']
                normalized_lamas_name = lamas_row['normalized_name']

                # Calculate weighted average score
                ratio = fuzz.ratio(normalized_osm_name, normalized_lamas_name)
                token_sort = fuzz.token_sort_ratio(normalized_osm_name, normalized_lamas_name)
                token_set = fuzz.token_set_ratio(normalized_osm_name, normalized_lamas_name)
                weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

                # Keep track of the best match found so far for this OSM street
                if best_match_for_street is None or weighted_score > best_match_for_street['weighted_score']:
                    best_match_for_street = {
                        'LAMAS_id': lamas_id,
                        'LAMAS_name': lamas_name_raw,
                        'weighted_score': weighted_score,
                        'matched_osm_name': osm_name  # Store which OSM name gave the best match
                    }

        # After checking all names, decide the status based on the best match found
        if best_match_for_street and best_match_for_street['weighted_score'] >= 90:
            candidates.append({
                'osm_id': osm_id,
                'status': 'CONFIDENT',
                'best_LAMAS_id': best_match_for_street['LAMAS_id'],
                'best_LAMAS_name': best_match_for_street['LAMAS_name'],
                'best_score': best_match_for_street['weighted_score'],
                'all_candidates': None,
                'matched_osm_name': best_match_for_street['matched_osm_name']
            })
        elif best_match_for_street and best_match_for_street['weighted_score'] >= 80:
             # For NEEDS_AI, we still need to collect other good candidates
            ai_candidates_scores = []
            for _, lamas_row in LAMAS_subset.iterrows():
                normalized_lamas_name = lamas_row['normalized_name']
                # Re-calculate score against the best-matched OSM name for consistency
                best_osm_name_norm = normalize_street_name(best_match_for_street['matched_osm_name'])

                ratio = fuzz.ratio(best_osm_name_norm, normalized_lamas_name)
                token_sort = fuzz.token_sort_ratio(best_osm_name_norm, normalized_lamas_name)
                token_set = fuzz.token_set_ratio(best_osm_name_norm, normalized_lamas_name)
                score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

                if score >= 80:
                    ai_candidates_scores.append({
                        'LAMAS_id': lamas_row['LAMAS_id'],
                        'LAMAS_name': lamas_row['LAMAS_name'],
                        'score': score
                    })

            ai_candidates_df = pd.DataFrame(ai_candidates_scores).sort_values(by='score', ascending=False).head(5)
            candidate_list = ai_candidates_df.apply(
                lambda r: f"ID: {r['LAMAS_id']}, Name: '{r['LAMAS_name']}' (Score: {r['score']:.2f})", axis=1
            ).tolist()

            candidates.append({
                'osm_id': osm_id,
                'status': 'NEEDS_AI',
                'best_LAMAS_id': best_match_for_street['LAMAS_id'],
                'best_LAMAS_name': best_match_for_street['LAMAS_name'],
                'best_score': best_match_for_street['weighted_score'],
                'all_candidates': "\n".join(candidate_list),
                'matched_osm_name': best_match_for_street['matched_osm_name']
            })
        else: # No suitable match found
            candidates.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_LAMAS_id': None,
                'best_LAMAS_name': None,
                'best_score': best_match_for_street['weighted_score'] if best_match_for_street else 0,
                'all_candidates': None,
                'matched_osm_name': None
            })

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
