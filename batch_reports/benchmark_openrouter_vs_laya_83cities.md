# 83-City AI Benchmark: `perplexity/pplx-decider-v1.3:free` vs Local Laya

## Executive Summary
- **Model**: `perplexity/pplx-decider-v1.3:free` (`shortlist_size=25`, `questions_per_request=150`)
- **Shortlist recall vs Local Laya approved hits**: **475/476 (99.79%)**
- **Wall time (83 cities / 2927 unmatched streets)**: **41.78s** (pure API time: 1.91s)
- **OpenRouter usage**: `api_requests=1`, `cache_hits=2842`, `input_tokens=54,323`, `cost=$0.0000`
- **Fallback / errors**: `fallback_events=0`, `local_fallback_resolved=0`, `errors=0`

| Metric | Local Laya | OpenRouter Decider V1.3 | Delta |
|---|---:|---:|---:|
| Pre-AI unmatched streets | 2,927 | 2,927 | 0 |
| AI raw suggestions | 2,199 | 1,484 | -715 |
| AI explicit `no_match` | 728 | 1,443 | +715 |
| **APPROVED_AI total** | **476** | **854** | **+378** |
| APPROVED_AI agree / override | 407 / 69 | 786 / 68 | - |
| Filtered out by guardrails | 1,723 | 630 | -1,093 |
| Total city coverage | 91.17% (25,308/27,759) | 92.53% (25,686/27,759) | +1.36pp |

## Guardrail Rejection Breakdown
- Local Laya: `{"override_low_confidence": 932, "magnet_raw_hits": 321, "override_low_core_wratio": 170, "feature_type_mismatch": 135, "digit_mismatch": 20, "magnet_post_hits": 11, "low_confidence": 123, "low_core_wratio": 6, "given_name_only_match": 5}`
- OpenRouter Decider: `{"low_confidence": 294, "override_low_core_wratio": 9, "override_low_confidence": 247, "digit_mismatch": 10, "feature_type_mismatch": 65, "low_core_wratio": 5}`

## Match Differences Summary: Gained=546, Dropped=168, Changed ID=7, High-Conf Semantic Blocked by WRatio=6

### Top Cities by Net APPROVED_AI Gain
| City | Named | Unmatched | Laya AI | OpenRouter AI | Delta | Laya % | OpenRouter % |
|---|---:|---:|---:|---:|---:|---:|---:|
| ירושלים | 2653 | 400 | 55 | 153 | +98 | 87.0% | 90.7% |
| רחובות | 632 | 109 | 23 | 56 | +33 | 86.4% | 91.6% |
| תל אביב - יפו | 2238 | 195 | 31 | 54 | +23 | 92.7% | 93.7% |
| חיפה | 1317 | 158 | 14 | 36 | +22 | 89.1% | 90.7% |
| ראשון לציון | 1222 | 127 | 49 | 66 | +17 | 93.6% | 95.0% |
| אשדוד | 663 | 65 | 15 | 32 | +17 | 92.5% | 95.0% |
| באר שבע | 1187 | 109 | 25 | 38 | +13 | 92.9% | 94.0% |
| אשקלון | 880 | 55 | 15 | 28 | +13 | 95.5% | 96.9% |
| פתח תקווה | 989 | 92 | 8 | 18 | +10 | 91.5% | 92.5% |
| בית שמש | 443 | 47 | 2 | 12 | +10 | 89.8% | 92.1% |
| כפר סבא | 500 | 40 | 6 | 15 | +9 | 93.2% | 95.0% |
| מעלות-תרשיחא | 213 | 15 | 0 | 7 | +7 | 93.0% | 96.2% |
| בני ברק | 408 | 32 | 6 | 12 | +6 | 93.6% | 95.1% |
| הרצליה | 688 | 59 | 6 | 12 | +6 | 92.3% | 93.2% |
| קרית אתא | 252 | 26 | 6 | 12 | +6 | 92.1% | 94.4% |
| קרית גת | 329 | 12 | 2 | 8 | +6 | 97.0% | 98.8% |
| רמת גן | 622 | 67 | 11 | 16 | +5 | 91.0% | 91.8% |
| הוד השרון | 428 | 37 | 5 | 10 | +5 | 92.5% | 93.7% |
| מודיעין-מכבים-רעות | 471 | 28 | 4 | 9 | +5 | 94.9% | 96.0% |
| רמת השרון | 374 | 46 | 3 | 8 | +5 | 88.5% | 89.8% |
| עפולה | 357 | 44 | 20 | 24 | +4 | 93.3% | 94.4% |
| נהריה | 264 | 20 | 6 | 10 | +4 | 94.7% | 96.2% |
| קרית מלאכי | 130 | 13 | 1 | 5 | +4 | 90.8% | 93.8% |
| גבעת שמואל | 135 | 40 | 8 | 11 | +3 | 76.3% | 78.5% |
| ראש העין | 357 | 22 | 3 | 6 | +3 | 94.7% | 95.5% |

### Gained Matches (OpenRouter Approved, Laya Missed) (546 total, showing up to 100)
| city | osm_street | or_lamas_name | or_conf | fuzzy_score | laya_status |
|---|---|---|---|---|---|
| ירושלים | אל מכפה | אל מקפה | 0.892 | 85.7 | filtered:override_low_confidence (הירשלר) |
| ירושלים | הרב רחמים חי חויתה | הרב חוויטה כהן רחמי | 0.93 | 88.2 | no_match |
| ירושלים | יהודה לאו פיקרד | לאו פיקרד | 0.891 | 85.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | א נאוורס | אל-נוורס | 0.939 | 87.5 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | נתן יעקב זלץ | זלץ | 0.87 | 84.2 | filtered:override_low_confidence (המושבה הגרמנית) |
| ירושלים | הכרפס | כרפס | 0.866 | 88.9 | filtered:override_low_confidence (התפוח) |
| ירושלים | דביר DVIR | דביר | 0.974 | 76.9 | filtered:override_low_core_wratio (אוצר הגאונים) |
| ירושלים | אלמדינה אלמנוורה | אל מדינה אל מונארה | 0.931 | 87.5 | no_match |
| ירושלים | יוסף לוריא | לוריא | 0.809 | 80.0 | filtered:override_low_confidence (בית קנדה) |
| ירושלים | Dresner | דרזנר | 0.915 | 0.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | אברהם שלום יהודה | אברהם ש י | 0.959 | 86.2 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | בונבנטורה | אנצ'ו בונהונטורה | 0.915 | 88.9 | filtered:override_low_confidence (התפוח) |
| ירושלים | מנהרת אמת המים התחתונה | אמת המים | 0.844 | 72.0 | filtered:override_low_core_wratio (שאר ישוב) |
| ירושלים | רבי חיסדא | הרב חסדא | 0.924 | 82.9 | no_match |
| ירושלים | רבי חיא הגדול | חיא | 0.954 | 80.9 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | Gedud 68 | גדוד 68 | 0.966 | 41.8 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | הרפובליקה הדומיניקנית | הרפוב הדומינ | 0.989 | 89.5 | filtered:override_low_core_wratio (שאר ישוב) |
| ירושלים | צבי לייבוביץ | ליבוביץ | 0.725 | 88.0 | filtered:low_confidence (ליבוביץ) |
| ירושלים | שקד | השקד | 0.886 | 85.7 | filtered:override_low_confidence (התפוח) |
| ירושלים | אריה ל מוצקין | מוצקין | 0.973 | 82.5 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | יצחק מ בודנהיימר | בודנהיימר | 0.965 | 83.2 | no_match |
| ירושלים | הרב אברהם שאג | הרב שאג | 0.927 | 85.7 | filtered:override_low_confidence (הרב שאג) |
| ירושלים | יחיאל צלנוב | צ'לנוב | 0.756 | 77.5 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | כיכר אריסטידס דה סוזה | אריסטידס סוזה מנדס | 0.988 | 85.9 | filtered:override_low_confidence (כיכר צ'אונה) |
| ירושלים | אברהם אבא אורי | אבא אורי | 0.929 | 83.6 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | יוסף בר ניסן | בר ניסן | 0.73 | 84.2 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | יהושוע יבין | יהושע ייבין | 0.927 | 76.2 | filtered:override_low_confidence (יהושע ייבין) |
| ירושלים | לב העברי | הלב העברי | 0.951 | 83.5 | filtered:override_low_confidence (התפוח) |
| ירושלים | קירשנבוים | אברהם מ.קירשנבוים | 0.987 | 85.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | גורג וושינגטון | וושינגטון | 0.873 | 87.0 | filtered:low_confidence (וושינגטון) |
| ירושלים | פיר וורן | וורן | 0.798 | 80.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | HaMekubalim Road | בני סנאה | 0.932 | 10.0 | filtered:override_low_confidence (המושבה הגרמנית) |
| ירושלים | פול אמיל בוטה | אמיל בוטה | 0.983 | 89.1 | filtered:override_low_confidence (התפוח) |
| ירושלים | מעלות הרב גורן | גורן שלמה | 0.742 | 85.7 | filtered:override_low_confidence (האזוב) |
| ירושלים | הכותל המערבי | הכותל | 0.846 | 75.3 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | רחבת הכותל המערבי | הכותל | 0.834 | 75.3 | filtered:low_confidence (הכותל) |
| ירושלים | סנט מארק | סנט מרכוס | 0.964 | 87.5 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | Mamilla Avenue | ככר המכס | 0.933 | 11.1 | filtered:override_low_confidence (יד ושם) |
| ירושלים | הקרן הקיימת לישראל | הקרן הקיימת | 0.916 | 85.5 | filtered:override_low_confidence (מוזאון ישראל) |
| ירושלים | קרן הקיימת לישראל | הקרן הקיימת | 0.956 | 86.1 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | רון אביטל | אביטל | 0.702 | 82.9 | filtered:override_low_confidence (התפוח) |
| ירושלים | כיכר נח פלוג | כיכר נח)הנריק(פלוג | 0.993 | 88.0 | filtered:override_low_confidence (כיכר ולירו) |
| ירושלים | החצב | החוצב | 0.923 | 88.9 | filtered:low_confidence (החוצב) |
| ירושלים | הרב שאול אבן דנן | אבן דנאן | 0.975 | 88.9 | no_match |
| ירושלים | שפרבר | דוד שפרבר | 0.955 | 82.9 | no_match |
| ירושלים | Suq Aftimos | סוק אבטימוס | 0.987 | 13.3 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | Suq Al Attarin | אל מטראן | 0.953 | 21.1 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | הוחזק דמטריוס הקדוש | דמיטריוס | 0.944 | 78.8 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | Saint Helena Road | הלנה | 0.975 | 18.2 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | El Jawalida Rd | אל גוואלידה | 0.97 | 21.1 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | طلعه الحلبيون | טלעת אל חלביון | 0.952 | 75.0 | filtered:override_low_confidence (התפוח) |
| ירושלים | רבקה סטיונבסקי | סטוינובסקי | 0.971 | 89.7 | no_match |
| ירושלים | מבוא יזהר ערמוני | יזהר | 0.765 | 83.2 | no_match |
| ירושלים | מבוא חנוך ילון | חנוך ילון | 0.908 | 87.0 | filtered:override_low_confidence (התפוח) |
| ירושלים | בן שטח | בן שטח שמעון | 0.934 | 80.0 | no_match |
| ירושלים | מעלות אל בטיח | האבטיחים | 0.856 | 85.7 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | מרדכי עליאש | אליאש | 0.968 | 77.5 | filtered:override_low_confidence (מרדכי )מנתא( ניסן) |
| ירושלים | דו נאס | דונואס | 0.94 | 83.3 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | משה זמורה | זמורה | 0.84 | 82.9 | filtered:override_low_confidence (התפוח) |
| ירושלים | אמרו אלקיס | אימרו אל קיס | 0.908 | 86.7 | filtered:override_low_confidence (התפוח) |
| ירושלים | מבוא יהשוע ילין | יהושע ילין | 0.97 | 83.3 | no_match |
| ירושלים | Bab Hutta Road | אל חוטה | 0.883 | 21.1 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | תבור | התבור | 0.944 | 88.9 | filtered:low_confidence (התבור) |
| ירושלים | סימה בליליוס | בלילוס | 0.969 | 84.2 | filtered:override_low_confidence (התפוח) |
| ירושלים | משה דוד גאון | גאון | 0.956 | 70.1 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | אל אהטאל | אל אח'טל | 0.934 | 80.0 | no_match |
| ירושלים | הרב יוסף גרשון הורוביץ | גרשון הורביץ | 0.846 | 77.5 | no_match |
| ירושלים | יאקט אל האמאווי | יאקוט אלחמווי | 0.987 | 78.6 | filtered:override_low_confidence (אל אסמאעי) |
| ירושלים | הרב שמחה זיסל ברוידא | ש בתי ברוידא | 0.708 | 66.9 | filtered:override_low_confidence (ירושלים מנחם) |
| ירושלים | יצחק למפרונטי | למפורטי | 0.861 | 85.7 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | משה דב לנדנר | לנדנר | 0.942 | 75.3 | filtered:override_low_confidence (התפוח) |
| ירושלים | דוד בן עמרם | בן אברהם דוד | 0.787 | 86.7 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | קפאח יחיא | קאפח | 0.888 | 88.9 | filtered:override_low_confidence (משטרת הישובים) |
| ירושלים | בלאזר | בלזר | 0.854 | 88.9 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | האדמור רבי שלמה | האדמור מזוועהיל | 0.946 | 81.7 | filtered:override_low_confidence (שאר ישוב) |
| ירושלים | האטאם טאווי | חאתם א טאי | 0.932 | 63.8 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | מוחמד א סלאח | מוחמד א-צלח | 0.876 | 87.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | אלי יבנ אבו טאלאב | אבו טאלב | 0.916 | 74.5 | filtered:override_low_confidence (התפוח) |
| ירושלים | דויד חזן | דוד החזן | 0.927 | 87.5 | filtered:override_low_confidence (התפוח) |
| ירושלים | ישראל אהרון פישל | אהרון פישל | 0.948 | 86.2 | no_match |
| ירושלים | פולנסקי שמשון | פולונסקי | 0.91 | 82.0 | filtered:override_low_confidence (שמשון) |
| ירושלים | רגב נשאשבי | נששיבי | 0.868 | 85.7 | no_match |
| ירושלים | אל אדיב אסעאף אל נשאשיבי | איסעאף נשאשיבי | 0.941 | 86.2 | filtered:override_low_confidence (בית אסתר מוס) |
| ירושלים | סימאם אל מליקי | אימאם אל מלכי | 0.974 | 69.2 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | וורטהיימר | הרב וורטהיימר שלמה | 0.915 | 87.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | מבוא כנסת מרדכי | כנסת מרדכי | 0.864 | 88.0 | no_match |
| ירושלים | יטבתה | יוטבתא | 0.944 | 80.0 | filtered:override_low_confidence (התפוח) |
| ירושלים | ויזניץ | אדמו"ר מויז'ניץ | 0.846 | 82.4 | filtered:low_confidence (קרית ויזניץ) |
| ירושלים | סיירת חיגה | סייאראת חג'ה | 0.848 | 88.9 | filtered:override_low_confidence (חגי) |
| ירושלים | שפרן | הרב שפרן | 0.783 | 80.0 | filtered:override_low_confidence (שפרה) |
| ירושלים | בויאן | אדמו"ר מבויאן | 0.881 | 80.0 | filtered:override_low_confidence (התפוח) |
| ירושלים | רוזין האדמור מרוזין | אדמו"ר מרוז'ין | 0.993 | 88.8 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | צכנוב הצדיק | האדמור מצכנוב | 0.98 | 80.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | אריך מנדלסון | מנדלסון | 0.979 | 84.2 | filtered:override_low_confidence (התפוח) |
| ירושלים | יוסלה רוזנבלט | יוסף רוזנבלט | 0.847 | 88.0 | filtered:override_low_confidence (התפוח) |
| ירושלים | הרב רפאל ברוך טולדנו | הרב טולדנו | 0.968 | 80.0 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | זמזאם | זמזם | 0.784 | 88.9 | filtered:low_confidence (זמזם) |
| ירושלים | אל כותר | אל קות'ר | 0.859 | 85.7 | filtered:magnet_raw_hits (התפוח) |
| ירושלים | אל מסכ | אל מסק | 0.889 | 83.3 | filtered:override_low_confidence (אל כאלה סמ9) |
| ירושלים | יואל אמנון פרידלר | יואל פרידלר | 0.853 | 87.1 | filtered:magnet_raw_hits (התפוח) |

### Dropped Matches (Laya Approved, OpenRouter Missed/Rejected) (168 total, showing up to 100)
| city | osm_street | laya_lamas_name | laya_conf | or_status |
|---|---|---|---|---|
| ירושלים | קציעה | קצילה | 0.84 | filtered:low_confidence (מבוא קציעה, conf=0.517) |
| ירושלים | מבוא הקנה | הקנה | 0.993 | filtered:override_low_confidence (הקנה, conf=0.459) |
| ירושלים | כיכר שרה ועמינדב אשבל | אשבל דב כיכר | 1.0 | filtered:low_confidence (אשבל דב כיכר, conf=0.429) |
| ירושלים | יצחק צבי אטינגר | אטינגר | 1.0 | filtered:low_confidence (אטינגר, conf=0.595) |
| ירושלים | רוטנברג אריה | רוטנברג | 1.0 | filtered:low_confidence (רוטנברג, conf=0.545) |
| ירושלים | הטיילת העליונה | דרך הטיילת | 0.831 | filtered:low_confidence (דרך הטיילת, conf=0.31) |
| ירושלים | אררט המלאך | המלאך | 1.0 | no_match (conf=0.285) |
| ירושלים | הפטריארכיה | הפטריארכיה הקופטית | 0.972 | filtered:override_low_confidence (גריק פטריאכט, conf=0.31) |
| ירושלים | מרדכי נרקיס | מרדכי )מנתא( ניסן | 0.987 | no_match (conf=0.726) |
| ירושלים | אלישיר יצחק | אלישר | 0.952 | filtered:override_low_confidence (אלישיב, conf=0.523) |
| ירושלים | אברהם טיכו | טיכו | 1.0 | filtered:override_low_confidence (טיכו, conf=0.743) |
| ירושלים | שדי חמד | שדה חמד | 1.0 | filtered:low_confidence (שדה חמד, conf=0.461) |
| ירושלים | יהושע רייכמן | רייכמן | 1.0 | filtered:override_low_confidence (רייכמן, conf=0.554) |
| ירושלים | רייכמן יהושע | רייכמן | 0.9 | filtered:override_low_confidence (רייכמן, conf=0.707) |
| ירושלים | ברנוביץ | רחבת רבינוביץ | 0.888 | filtered:low_confidence (רבינוביץ, conf=0.24) |
| ירושלים | דוד בן גוריון | בן גוריון | 0.734 | filtered:low_confidence (בן גוריון, conf=0.55) |
| ירושלים | האוניברסיטה העברית | אוניברסיטה | 1.0 | filtered:low_confidence (שד האוניברסיטה, conf=0.46) |
| ירושלים | אש הרטום | הרטום | 0.798 | filtered:low_confidence (הרטום, conf=0.463) |
| ירושלים | אהרן צבי פרופסור | פרופס | 0.978 | no_match (conf=0.383) |
| ירושלים | פרופס אהרן צבי | פרופס | 1.0 | no_match (conf=0.46) |
| תל אביב - יפו | ורשקובסקי | ורשבסקי | 0.855 | filtered:low_confidence (ורשבסקי, conf=0.483) |
| תל אביב - יפו | שלמה שילר | שילר | 1.0 | filtered:override_low_confidence (שילר, conf=0.792) |
| תל אביב - יפו | אברהם ענתבי | ענתבי | 1.0 | filtered:low_confidence (ענתבי, conf=0.674) |
| תל אביב - יפו | אצטדיון בלומפילד | בלומנפלד | 0.999 | filtered:low_confidence (בלומנפלד, conf=0.621) |
| תל אביב - יפו | רבקה ושמעון אבולעפיה | אבולעפיה | 1.0 | filtered:low_confidence (אבולעפיה רבקה ושלמה, conf=0.536) |
| תל אביב - יפו | אליעזר קפלן | קפלן | 0.939 | no_match (conf=0.486) |
| תל אביב - יפו | זלדה שניאורסון מישקובסקי | זלדה | 1.0 | filtered:low_confidence (זלדה, conf=0.365) |
| תל אביב - יפו | הרב אהרונסון | אהרונסון | 0.77 | no_match (conf=0.431) |
| תל אביב - יפו | הטיילת עש שלמה להט | הטיילת | 0.974 | filtered:low_confidence (הטיילת, conf=0.303) |
| תל אביב - יפו | אריאל שרון | השרון | 1.0 | filtered:low_confidence (אדיכל אריה שרון, conf=0.403) |
| תל אביב - יפו | יורק שטיינר | שטיינר | 1.0 | filtered:low_confidence (שטיינר, conf=0.611) |
| תל אביב - יפו | ישראל אשכנזי | אשכנזי | 1.0 | filtered:low_confidence (אשכנזי, conf=0.661) |
| תל אביב - יפו | דרך אריאל שרון | השרון | 1.0 | no_match (conf=0.339) |
| חיפה | דרך נוף עובדיה | עובדיה | 0.996 | filtered:low_confidence (עובדיה, conf=0.424) |
| חיפה | ואדי אחוזה תחתון | אחוזה | 0.888 | no_match (conf=0.657) |
| חיפה | פרוג שמעון | פרוג | 0.998 | filtered:override_low_confidence (פרוג, conf=0.516) |
| חיפה | אהובה עוזרי | אהובה תומר | 0.791 | no_match (conf=0.602) |
| חיפה | יגאל בשן | בשן | 0.882 | no_match (conf=0.641) |
| חיפה | חורשת הבנים | החורשה | 0.875 | no_match (conf=0.36) |
| חיפה | קרפל ליפה | ד"ר קרפל | 0.76 | filtered:low_confidence (ד"ר קרפל, conf=0.638) |
| חיפה | שב בלהה | בלהה | 0.769 | filtered:low_confidence (בלהה, conf=0.501) |
| חיפה | שדרות הנשיא טרומן | טרומן | 1.0 | filtered:override_low_confidence (טרומן, conf=0.795) |
| פתח תקווה | משעול דוד לוי | לוי | 1.0 | no_match (conf=0.764) |
| פתח תקווה | משעול יצחק קאמינקר | קאמינקר | 0.999 | filtered:low_confidence (קאמינקר, conf=0.648) |
| פתח תקווה | דר וייסברג | ויסבורג | 0.75 | filtered:low_confidence (ויסבורג, conf=0.65) |
| ראשון לציון | הפרח בגני | הפרח | 1.0 | filtered:low_confidence (הפרח, conf=0.625) |
| ראשון לציון | שרה ואליהו קפלן | שרה ואליהו | 0.999 | filtered:low_confidence (שרה ואליהו, conf=0.655) |
| ראשון לציון | יעקב דורי | דורי | 1.0 | filtered:override_low_confidence (דורי, conf=0.688) |
| ראשון לציון | תחנת רכבת הראושנים | הרכבת | 1.0 | filtered:override_low_confidence (שד' הראשונים, conf=0.321) |
| ראשון לציון | שמואל יבניאלי | יבניאלי | 0.968 | filtered:low_confidence (יבניאלי, conf=0.441) |
| ראשון לציון | דוד וולפסון | וולפסון | 1.0 | filtered:low_confidence (וולפסון, conf=0.576) |
| ראשון לציון | אברהם מאפו | מאפו | 0.999 | filtered:override_low_confidence (מאפו, conf=0.67) |
| ראשון לציון | אליעזר קפלן | קפלן | 0.97 | filtered:override_low_confidence (קפלן, conf=0.67) |
| ראשון לציון | יוסף אהרונוביץ | אהרונוביץ | 1.0 | filtered:low_confidence (אהרונוביץ, conf=0.472) |
| ראשון לציון | השר יעקב קנר | קנר | 1.0 | filtered:override_low_confidence (קנר, conf=0.769) |
| ראשון לציון | גשר אצל | גשר | 0.999 | filtered:override_low_confidence (אצ"ל, conf=0.641) |
| ראשון לציון | צבי גרץ | גרץ | 1.0 | no_match (conf=0.535) |
| ראשון לציון | יוסף טרומפלדור | טרומפלדור | 1.0 | filtered:low_confidence (טרומפלדור, conf=0.64) |
| ראשון לציון | בלקינד נעמן | בלקינד | 0.883 | no_match (conf=0.604) |
| ראשון לציון | זאב אברמוביץ | ש אברמוביץ | 0.917 | no_match (conf=0.412) |
| ראשון לציון | אביהו לוי מדינה | לוי מונטלצ'יני | 0.923 | no_match (conf=0.738) |
| בני ברק | מעבר לקרית הרצוג מאיילת השחר | קרית הרצוג | 0.783 | no_match (conf=0.153) |
| נתניה | אברהם קריניצי | קריניצי | 1.0 | filtered:low_confidence (קריניצי, conf=0.462) |
| נתניה | יחזקאל בן בוזי | יחזקאל | 1.0 | no_match (conf=0.721) |
| נתניה | אריה מוצקין | מוצקין | 1.0 | filtered:low_confidence (מוצקין, conf=0.638) |
| נתניה | שבזי שלום | שבזי | 0.995 | filtered:low_confidence (שבזי, conf=0.501) |
| נתניה | אריאל בן ציון | בן ציון | 1.0 | no_match (conf=0.392) |
| נתניה | יוסף ויתקין | ויתקין | 1.0 | filtered:low_confidence (ויתקין, conf=0.633) |
| נתניה | פנחס חורגין | חורגין | 1.0 | filtered:low_confidence (חורגין, conf=0.548) |
| נתניה | אליעזר קפלן | קפלן | 0.961 | filtered:override_low_confidence (קפלן, conf=0.452) |
| נתניה | יצחק אפשטיין | אפשטיין | 1.0 | filtered:low_confidence (אפשטיין, conf=0.667) |
| נתניה | בכור שיטרית | שיטריט | 0.837 | no_match (conf=0.367) |
| נתניה | אברהם שלונסקי | שלונסקי | 1.0 | filtered:low_confidence (שלונסקי, conf=0.666) |
| נתניה | אריה בן אליעזר | בן אליעזר | 1.0 | no_match (conf=0.479) |
| נתניה | מאיר בר אילן | בר אילן | 1.0 | no_match (conf=0.366) |
| נתניה | אליהו גולומב | גולומב | 1.0 | filtered:low_confidence (גולומב, conf=0.513) |
| נתניה | הרב יוסף ריבלין | ריבלין | 0.969 | no_match (conf=0.447) |
| נתניה | חיים וייצמאן | וייצמן | 0.974 | filtered:low_confidence (וייצמן, conf=0.661) |
| נתניה | נחום סוקולוב | סוקולוב | 1.0 | filtered:low_confidence (סוקולוב, conf=0.694) |
| נתניה | פייר גרנדוס | גרנדוס | 1.0 | filtered:low_confidence (גרנדוס, conf=0.642) |
| אשדוד | הר פארן | נחל פארן | 0.907 | no_match (conf=0.583) |
| אשדוד | מבוא האתרוג | אתרוג | 0.74 | no_match (conf=0.615) |
| אשדוד | מבוא הנרקיס | הנרקיס | 0.999 | filtered:low_confidence (הנרקיס, conf=0.69) |
| באר שבע | פנחס רוטנברג | רוטנברג | 1.0 | filtered:low_confidence (רוטנברג, conf=0.539) |
| באר שבע | יצחק סלמה | סלמה | 0.925 | filtered:override_low_confidence (סלמה, conf=0.698) |
| באר שבע | גרשון דובינבוים | גרשון | 1.0 | no_match (conf=0.688) |
| באר שבע | מיכאל הלפרין | הלפרין | 0.874 | filtered:override_low_confidence (הלפרין, conf=0.59) |
| באר שבע | מרדכי אנילביץ | אנילביץ | 0.999 | filtered:low_confidence (אנילביץ, conf=0.49) |
| באר שבע | עופר גראנד קניון | קניון הנגב | 1.0 | no_match (conf=0.397) |
| באר שבע | נתן אלתרמן | אלתרמן | 1.0 | filtered:low_confidence (אלתרמן, conf=0.602) |
| באר שבע | זלמן שניאור | שניאור | 1.0 | filtered:low_confidence (שניאור, conf=0.667) |
| באר שבע | אליהו גולומב | גולומב | 1.0 | no_match (conf=0.546) |
| באר שבע | צבי גרץ | גרץ | 1.0 | no_match (conf=0.594) |
| באר שבע | מרכז אורן | באר שבע קלישר )מ | 0.922 | no_match (conf=0.388) |
| חולון | דוד רמז | רמז | 0.995 | no_match (conf=0.514) |
| חולון | סמטת ארמון | החרמון | 0.896 | filtered:low_confidence (הרימון, conf=0.466) |
| חולון | אליעזר קפלן | קפלן | 1.0 | filtered:override_low_confidence (קפלן, conf=0.695) |
| חולון | יל פרץ | פרץ | 0.847 | no_match (conf=0.32) |
| חולון | בן יהודה אליעזר | בן יהודה | 1.0 | filtered:override_low_confidence (בן יהודה, conf=0.72) |
| רמת גן | השלום | השלם | 1.0 | no_match (conf=0.579) |

### Changed LAMAS ID (Both Approved, Different Target) (7 total, showing up to 100)
| city | osm_street | laya_lamas_name | laya_conf | or_lamas_name | or_conf |
|---|---|---|---|---|---|
| ירושלים | מאיר פינשטיין | אינשטיין | 0.978 | פיינשטיין | 0.714 |
| ירושלים | שדרות האוניברסיטה העברית | אוניברסיטה | 0.96 | שד האוניברסיטה | 0.87 |
| תל אביב - יפו | אידלסון | אידלסון רחל ושמואל | 1.0 | אברהם אידלסון | 0.78 |
| חיפה | ברזילי יהושע | יהושע | 1.0 | ברזילאי | 0.759 |
| ראשון לציון | יהושע לייב חנקין | בית יהושע | 0.998 | חנקין | 0.842 |
| אשדוד | רבי יהודה בן זכאי | בן יהודה | 0.977 | בן זכאי | 0.931 |
| נהריה | שדרות הנשיא בן צבי | הנשיא | 1.0 | ביס רכיבה דקל | 0.826 |

### High-Confidence OpenRouter Matches (>=0.85) Blocked by WRatio Guardrail (6 total, showing up to 80)
| city | osm_street | or_lamas_name | or_conf | reason |
|---|---|---|---|---|
| ירושלים | הכביש האמריקאי | הדרך האמריקאית | 0.866 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_15 [filtered: override_low_core_wratio(71.4<80.0)] |
| פתח תקווה | מצדה | מסדה | 0.869 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_6 [filtered: override_low_core_wratio(75.0<80.0)] |
| רמת גן | Sderot Ben Gurion | בן גוריון | 0.866 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_1 [filtered: override_low_core_wratio(76.2<80.0)] |
| רעננה | הרב ניסים | הרב נסים יצחק | 0.896 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_1 [filtered: low_core_wratio(72.7<75.0)] |
| נתיבות | הרב מיימון | הרמב"ם | 0.881 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_11 [filtered: override_low_core_wratio(67.5<80.0)] |
| חריש | Savyon | הסביון | 0.899 | OpenRouter (perplexity/pplx-decider-v1.3:free) selected s_2 [filtered: override_low_core_wratio(57.1<80.0)] |