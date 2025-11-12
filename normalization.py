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
