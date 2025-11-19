# 1. עדכון ההכרעות של ה-AI ל-candidates_df
ai_decisions_merged = candidates_df.merge(ai_decisions_df, on='osm_id', how='left')

# 2. יצירת עמודת המזהה הסופי (Final LAMS ID)
final_map = ai_decisions_merged[['osm_id']].copy()

# התחלה מהרשומות הוודאיות
confident_rows = ai_decisions_merged[ai_decisions_merged['status'] == 'CONFIDENT']
final_map = confident_rows[['osm_id', 'best_lams_id']].rename(columns={'best_lams_id': 'final_lams_id'})

# הוספת רשומות שהוכרעו על ידי AI
ai_resolved_rows = ai_decisions_merged[ai_decisions_merged['status'] == 'NEEDS_AI']
ai_resolved_rows['final_lams_id'] = ai_resolved_rows['ai_lams_id'].astype(str) # לוודא שזה מחרוזת

# סינון רשומות שה-AI קבע שאין להן התאמה
ai_resolved_rows = ai_resolved_rows[ai_resolved_rows['final_lams_id'] != 'None']

# איחוד כל הרשומות שהותאמו בהצלחה
final_mapping_df = pd.concat([
    final_map, 
    ai_resolved_rows[['osm_id', 'final_lams_id']]
], ignore_index=True)

# 3. הוספת המזהה הסופי ל-GeoDataFrame של OSM
osm_gdf_final = osm_gdf.merge(final_mapping_df, on='osm_id', how='left')

# 4. זיהוי הרשומות שלא נמצאה להן התאמה (למעקב ידני/מעבר חוזר)
unmatched_count = osm_gdf_final['final_lams_id'].isnull().sum()
total_osm = len(osm_gdf_final)

print("\n--- סיכום תוצאות המיפוי ---")
print(f"סה\"כ רשומות OSM: {total_osm}")
print(f"רשומות שהותאמו בהצלחה: {total_osm - unmatched_count}")
print(f"רשומות שלא נמצאה להן התאמה (חייבות בדיקה ידנית/איטרציה נוספת): {unmatched_count}")
print("\n--- טבלת המיפוי הסופית (דוגמה) ---")
print(osm_gdf_final[['osm_id', 'osm_name', 'final_lams_id']].dropna().head(10))