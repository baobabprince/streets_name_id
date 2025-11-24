# מדריך לבדיקת השפעת שינוי סף ה-Fuzzy Matching

## רקע

הפרויקט משתמש ב-fuzzy matching כדי להתאים בין שמות רחובות ב-OSM לבין שמות רחובות במאגר LAMAS.
כרגע, הסף (threshold) מוגדר כך:

- **CONFIDENT** (התאמה ודאית): ציון ≥ **90**
- **NEEDS_AI** (דורש AI): ציון בין **80** ל-**90**
- **MISSING** (ללא התאמה): ציון < **80**

## מה שונה?

הוספתי סטטיסטיקות מפורטות ל-`normalization.py` שיעזרו לך לקבל החלטה מושכלת לגבי שינוי הסף.

### שינויים שבוצעו:

1. **הוספת ניתוח סטטיסטי** - כעת כאשר מריצים את הפייפליין, יודפסו סטטיסטיקות מפורטות:
   - התפלגות הציונים (ממוצע, חציון, סטיית תקן)
   - אחוזונים (percentiles)
   - השפעת הסף הנוכחי
   - סימולציה של ספים אלטרנטיביים

2. **כלי בדיקה עצמאיים**:
   - `test_threshold.py` - לבדיקת השפעת סף מסוים
   - `analyze_threshold.py` - לניתוח מעמיק של נתונים קיימים

## איך להשתמש?

### אופציה 1: הרצת Pipeline רגילה (מומלץ להתחלה)

פשוט תריץ את הפייפליין כרגיל:

```bash
python3 pipeline.py "אלעד"
```

בסוף שלב ה-Fuzzy Matching, תראה פלט כזה:

```
============================================================
FUZZY MATCHING THRESHOLD ANALYSIS
============================================================
Total streets processed: 450

Score Distribution:
  Mean score: 85.32
  Median score: 88.50
  Std deviation: 15.23

Percentiles:
  10th percentile: 62.15
  25th percentile: 75.40
  50th percentile: 88.50
  75th percentile: 95.20
  90th percentile: 98.10
  95th percentile: 99.00
  99th percentile: 100.00

Current Thresholds Impact:
  CONFIDENT (≥90): 245 streets (54.4%)
  NEEDS_AI (80-90): 135 streets (30.0%)
  MISSING (<80): 70 streets (15.6%)

Alternative Threshold Scenarios:
  If threshold was 70: 410 streets would qualify (91.1%)
  If threshold was 75: 385 streets would qualify (85.6%)
  If threshold was 80: 380 streets would qualify (84.4%)
  If threshold was 85: 320 streets would qualify (71.1%)
============================================================
```

**איך לפרש את התוצאות?**

- אם רוב הרחובות נמצאים בטווח 80-90, אולי כדאי להוריד את הסף ל-75 או 78
- אם יש הרבה רחובות עם ציונים גבוהים (>95), הסף הנוכחי טוב
- אם יש פיזור גדול (סטיית תקן גבוהה), אולי צריך גישה שונה

### אופציה 2: בדיקת סף ספציפי

אם אתה רוצה לבדוק איך סף מסוים ישפיע על התוצאות, השתמש ב-`test_threshold.py`.
הסקריפט טוען את נתוני ה-OSM (קבצי PKL) ונתוני LAMAS באופן עצמאי:

```bash
# בדיקה עם סף AI של 75 במקום 80
python test_threshold.py "בית שאן" --ai-threshold 75

# בדיקה עם סף CONFIDENT של 88 במקום 90
python test_threshold.py "בית שאן" --confident-threshold 88

# בדיקה עם שני הספים ביחד
python test_threshold.py "בית שאן" --ai-threshold 75 --confident-threshold 88
```

**שים לב**: הסקריפט מבצע אופטימיזציה ומחשב ציונים רק עבור שמות רחובות ייחודיים, ולכן הוא רץ מהר מאוד גם על ערים גדולות.

הסקריפט יציג:
- כמה רחובות יסווגו כ-CONFIDENT, NEEDS_AI, ו-MISSING
- דוגמאות מכל קטגוריה
- סטטיסטיקות מפורטות


### אופציה 3: השוואת מספר סטים של סף

אם אתה רוצה לראות השוואה בין מספר אפשרויות:

```bash
python test_threshold.py "אלעד" --compare
```

זה יציג טבלה שמשווה:
- Current (90, 80) - הסף הנוכחי
- Slightly relaxed (88, 78)
- Moderately relaxed (85, 75)
- More relaxed (82, 70)

## איך לשנות את הסף בפועל?

אחרי שהחלטת על הסף החדש, ערוך את `normalization.py`:

### שורה 113 - סף CONFIDENT:
```python
confident_match = scores_df[scores_df['weighted_score'] >= 90].head(1)
```
שנה את `90` לערך החדש (למשל `88`).

### שורה 128 - סף NEEDS_AI:
```python
ai_candidates = scores_df[
    (scores_df['weighted_score'] >= 80) & 
    (scores_df['weighted_score'] < 90)
].head(5).copy()
```
שנה את `80` לערך החדש (למשל `75`).

**חשוב**: וודא שהסף של NEEDS_AI תמיד נמוך מהסף של CONFIDENT!

## דוגמאות לשימוש

### דוגמה 1: בדיקה מהירה
```bash
# הרץ pipeline על עיר קטנה ובדוק את הסטטיסטיקות
python3 pipeline.py "אלעד"
# בדוק את הפלט בסוף שלב 4
```

### דוגמה 2: ניסוי עם סף שונה
```bash
# בדוק איך סף של 75 ישפיע
python test_threshold.py "אלעד" --ai-threshold 75
```

### דוגמה 3: השוואה מקיפה
```bash
# השווה מספר אפשרויות
python test_threshold.py "אלעד" --compare
```

## טיפים

1. **התחל עם עיר קטנה** - "אלעד" היא בחירה טובה כי יש לה מספר סביר של רחובות
2. **בדוק את ה-percentiles** - אם ה-75th percentile הוא 95, זה אומר ש-75% מהרחובות קיבלו ציון מעל 95
3. **שים לב ל-NEEDS_AI** - אם יש יותר מדי רחובות ב-NEEDS_AI, זה יעלה עלויות AI
4. **בדוק דוגמאות** - הסקריפטים מציגים דוגמאות מכל קטגוריה - בדוק אותן ידנית

## קבצים שנוצרו

הסקריפטים שומרים קבצי CSV עם התוצאות:
- `data/<city>_threshold_<confident>_<ai>.csv` - תוצאות עם סף מסוים
- `data/<city>_threshold_analysis.csv` - ניתוח מקיף

## שאלות נפוצות

**ש: מה קורה אם אוריד את הסף יותר מדי?**
ת: תקבל יותר false positives - רחובות שיסווגו כ-CONFIDENT אבל בעצם לא מתאימים.

**ש: מה קורה אם אעלה את הסף?**
ת: תקבל פחות התאמות אוטומטיות, ויותר רחובות ידרשו AI או יסומנו כ-MISSING.

**ש: איך אני יודע מה הסף האופטימלי?**
ת: זה תלוי במטרה שלך:
- אם אתה רוצה דיוק מקסימלי - השאר את הסף גבוה (90)
- אם אתה רוצה כיסוי מקסימלי - הורד את הסף (75-80)
- אם אתה רוצה איזון - נסה 85-88

**ש: האם אני צריך לשנות גם את משקלי הציון?**
ת: לא בהכרח. המשקלים הנוכחיים (0.2, 0.3, 0.5) נותנים עדיפות ל-token_set_ratio, שהוא הכי טוב לשמות חלקיים.
