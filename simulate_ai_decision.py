import random
import pandas as pd
# candidates_df, osm_gdf, map_of_adjacents, ו-lams_df טעונים
# ו-prepare_ai_prompt מוגדר כפי שהוסכם

def simulate_ai_decision(prompt, ai_candidates_str):
    """
    מדמה את החלטת מודל ה-AI על סמך הפרומפט.
    בפועל, כאן תתרחש קריאת ה-API למודל AI (כגון OpenAI, Gemini וכו').
    """
    # 1. ניתוח: הוצאת ה-IDs והציונים (ה-Token Set Ratio הוא קריטי כאן)
    # לצורך הסימולציה, נדמה שה-AI מעדיף את ה-lams_id עם הציון הגבוה ביותר
    # מתוך המועמדים שהוגשו בפרומפט.

    # פירוק המועמדים מהמחרוזת
    candidate_lines = ai_candidates_str.split('\n')
    best_candidate_id = None
    best_score = -1
    
    for line in candidate_lines:
        try:
            # מניח שהמבנה הוא: ID: 1234, Name: 'Name' (Score: 92.50)
            lams_id = int(line.split('ID: ')[1].split(',')[0])
            score = float(line.split('Score: ')[1].strip(')'))
            
            # לדמות החלטה: בחירת ה-ID עם הציון המשוקלל הגבוה ביותר
            if score > best_score:
                best_score = score
                best_candidate_id = lams_id
        except:
            continue
            
    # הוספת רכיב אקראי: סיכוי של 10% שה-AI לא מוצא התאמה (ומחזיר None)
    if random.random() < 0.10:
        return 'None'
    
    return str(best_candidate_id)

if __name__ == "__main__":
    # ביצוע הסימולציה (רק כשמריצים את המודול כסקריפט)
    try:
        ai_results = []
        ai_candidates_to_process = candidates_df[candidates_df['status'] == 'NEEDS_AI']

        print(f"\n--- מתחיל סימולציית הכרעה ל-{len(ai_candidates_to_process)} רשומות ---")

        for _, row in ai_candidates_to_process.iterrows():
            osm_id = row['osm_id']

            # הכנת הפרומפט (אם ה-AI היה אמיתי)
            # prompt = prepare_ai_prompt(osm_id, row['all_candidates'], map_of_adjacents, osm_gdf)

            # סימולציה של התשובה
            ai_decision_id = simulate_ai_decision(None, row['all_candidates'])

            ai_results.append({
                'osm_id': osm_id,
                'ai_lams_id': ai_decision_id
            })

        ai_decisions_df = pd.DataFrame(ai_results)
        print("סימולציה הסתיימה.")
        print(ai_decisions_df.head())
    except NameError:
        print("Missing required variables (candidates_df, osm_gdf, map_of_adjacents). Skipping simulation.")