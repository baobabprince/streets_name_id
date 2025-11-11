import pandas as pd
# lams_df ו-osm_gdf טעונים כעת מהמקורות (או Mock Data מתוקן)

from fuzzywuzzy import fuzz
import numpy as np

def find_fuzzy_candidates(osm_df, lams_df):
    """
    מבצע Fuzzy Matching מתוחכם ובוחר את המועמדים המובילים לכל רחוב ב-OSM.
    """
    candidates = []
    
    # 1. איטרציה על כל רחוב ב-OSM
    for _, osm_row in osm_df.iterrows():
        osm_id = osm_row['osm_id']
        osm_name = osm_row['normalized_name']
        osm_city = osm_row['city'] # חיוני לסנן לפי עיר

        # 2. סינון רחובות הלמ"ס לאותה עיר (חיסכון במשאבי חישוב)
        lams_subset = lams_df[lams_df['city'] == osm_city]
        
        # 3. חישוב ציוני דמיון
        scores = []
        for _, lams_row in lams_subset.iterrows():
            lams_id = lams_row['lams_id']
            lams_name = lams_row['normalized_name']
            
            # חישוב 3 מדדים שונים:
            ratio = fuzz.ratio(osm_name, lams_name)
            token_sort = fuzz.token_sort_ratio(osm_name, lams_name)
            token_set = fuzz.token_set_ratio(osm_name, lams_name) # הכי חשוב לשמות חלקיים
            
            # ניקוד ממוצע משוקלל (ניתן לשנות את המשקולות)
            weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])

            scores.append({
                'lams_id': lams_id,
                'lams_name': lams_name,
                'weighted_score': weighted_score,
                'token_set_score': token_set # נשמור את זה במיוחד
            })

        # 4. סינון וסיווג: בחירת המועמדים הטובים ביותר
        scores_df = pd.DataFrame(scores).sort_values(by='weighted_score', ascending=False)
        
        # התאמה ודאית: אם יש ציון גבוה מאוד (מעל 98, לדוגמה)
        confident_match = scores_df[scores_df['weighted_score'] >= 98]
        if not confident_match.empty:
            candidates.append({
                'osm_id': osm_id,
                'status': 'CONFIDENT',
                'best_lams_id': confident_match.iloc[0]['lams_id'],
                'best_score': confident_match.iloc[0]['weighted_score'],
                'all_candidates': None # אין צורך ב-AI
            })
            continue

        # מועמדים ל-AI: אם יש ציונים סבירים (בין 80 ל-98)
        ai_candidates = scores_df[(scores_df['weighted_score'] >= 80) & (scores_df['weighted_score'] < 98)].head(5) # 5 המועמדים המובילים ל-AI
        
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
        if confident_match.empty and ai_candidates.empty:
            candidates.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_lams_id': None,
                'best_score': 0,
                'all_candidates': None
            })

    return pd.DataFrame(candidates)

# לוגיקה זו דורשת שהלוגיקה של העיר תהיה מדויקת.
# נניח שהתאמת 'city' בין OSM ללמ"ס כבר בוצעה.
# candidates_df = find_fuzzy_candidates(osm_gdf, lams_df)
# print("\n--- דוגמה למועמדים שדורשים AI ---")
# print(candidates_df[candidates_df['status'] == 'NEEDS_AI'].head())

# map_of_adjacents נוצר מהקוד הקודם (build_adjacency_map)

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

# דוגמה לשימוש ברשומה ספציפית:
# ai_record = candidates_df[candidates_df['status'] == 'NEEDS_AI'].iloc[0]
# prompt_to_send = prepare_ai_prompt(
#     ai_record['osm_id'], 
#     ai_record['all_candidates'], 
#     map_of_adjacents, 
#     osm_gdf
# )
# print("\n--- פרומפט לדוגמה שיישלח ל-AI ---")
# print(prompt_to_send)

if __name__ == "__main__":
    # Apply normalization only when running this module directly (demo mode).
    try:
        lams_df['normalized_name'] = lams_df['lams_name'].apply(normalize_street_name)
        osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)

        # ניקוי רשומות חסרות נרמול (שאין להן שם רחוב בכלל)
        lams_df = lams_df.dropna(subset=['normalized_name'])
        osm_gdf = osm_gdf.dropna(subset=['normalized_name'])

        print("בוצע נרמול שמות על נתוני הלמ\"ס ו-OSM.")
    except NameError:
        print("lams_df או osm_gdf לא הוגדרו - דילוג על דוגמת נרמול.")