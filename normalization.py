"""
Street Name Normalization and Fuzzy Matching Module

This module provides functions for:
1. Normalizing Hebrew street names (expanding abbreviations, removing punctuation)
2. Finding fuzzy match candidates between OSM and LAMAS street data
"""

import re
import pandas as pd
import requests
import os
import json
from rapidfuzz import fuzz
from typing import List, Dict, Tuple

# Cache for AI results to avoid redundant API calls
AI_TRANSLITERATION_CACHE = {}

def polish_transliteration_with_ai(arabic_name: str, algorithmic_hebrew: str) -> str:
    """
    Refine the algorithmic transliteration using AI (Gemini).
    
    Args:
        arabic_name: Original Arabic street name
        algorithmic_hebrew: Initial Hebrew transliteration
        
    Returns:
        Polished Hebrew name
    """
    # 1. Check cache
    cache_key = f"{arabic_name}_{algorithmic_hebrew}"
    if cache_key in AI_TRANSLITERATION_CACHE:
        return AI_TRANSLITERATION_CACHE[cache_key]
        
    # 2. Check for API key
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return algorithmic_hebrew # Fallback to algorithmic if no key
        
    # 3. Prepare AI prompt
    prompt = f"""You are a linguistic expert specializing in Arabic-to-Hebrew phonetic transliteration for Israeli street names.
I have an Arabic street name and an initial algorithmic transliteration. 
Please provide the most accurate, natural-sounding Hebrew transliteration.

Arabic Name: {arabic_name}
Initial Transliteration: {algorithmic_hebrew}

Instructions:
- Correct phonetic errors.
- If the Arabic name is a standard word (e.g., 'Al-Salam'), ensure the Hebrew is appropriate ('השלום' or 'סלאם').
- Return ONLY the polished Hebrew name, no explanation.

Hebrew Name:"""

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={api_key}"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        
        polished = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', algorithmic_hebrew).strip()
        
        # 4. Update cache and return
        AI_TRANSLITERATION_CACHE[cache_key] = polished
        return polished
        
    except Exception as e:
        print(f"  ⚠ AI Polish failed for {arabic_name}: {e}")
        return algorithmic_hebrew


def normalize_street_name(name):
    """
    Normalize a Hebrew street name by:
    1. Expanding common abbreviations (רח' -> רחוב, שד' -> שדרות, etc.)
    2. Removing punctuation and extra whitespace
    3. Standardizing format
    
    Args:
        name: Street name string (can be None or NaN)
        
    Returns:
        Normalized street name string, or None if input is invalid
    """
    if pd.isna(name) or name is None:
        return None
    
    name = str(name).strip()
    
    if not name:
        return None
    
    # Fix common typos first
    typo_fixes = [
        (r'\bסמתט\b', 'סמטת'),  # Common typo: סמתט -> סמטת
    ]
    
    for pattern, replacement in typo_fixes:
        name = re.sub(pattern, replacement, name)
    
    # Expand abbreviations - order matters! More specific patterns first
    # Use word boundaries to avoid partial matches
    abbreviations = [
        (r'\bרח\'', 'רחוב'),
        (r'\bרח\.', 'רחוב'),
        (r'\bשד\'', 'שדרות'),
        (r'\bשד\.', 'שדרות'),
        (r'\bשד\b', 'שדרות'),  # שד without punctuation
        (r'\bכי\'', 'כיכר'),
        (r'\bכי\.', 'כיכר'),
        (r'\bדר\'', 'דרך'),
        (r'\bד"ר\b', 'דוקטור'),
        (r'\bסמ\'', 'סמטה'),
        (r'\bסמ\.', 'סמטה'),
        (r'\bסמט\'', 'סמטת'),  # Handle סמט' variation
        (r'\bסמט\.', 'סמטת'),
        (r'\bפרופ\b', 'פרופסור'),
    ]
    
    for pattern, replacement in abbreviations:
        name = re.sub(pattern, replacement, name)
    
    # Arabic-specific normalization
    if is_arabic(name):
        # Remove common street prefixes in Arabic
        arabic_prefixes = [
            (r'\bشارع\s+', ''), # Street
            (r'\bطريق\s+', ''), # Way
            (r'\bحارة\s+', ''), # Neighborhood/Alley
        ]
        for pattern, replacement in arabic_prefixes:
            name = re.sub(pattern, replacement, name)
        
        # Remove 'Al-' prefix in Arabic script (ال)
        name = re.sub(r'^ال', '', name) # Start of string
        name = re.sub(r'\s+ال', ' ', name) # After space
        
        # TRANSFORMATION: Convert to Hebrew
        algorithmic_hebrew = transliterate_arabic_to_hebrew(name)
        # Polish with AI
        name = polish_transliteration_with_ai(name, algorithmic_hebrew)
    
    # Remove common street type prefixes at the beginning
    # This helps match "חטיבת הנגב" with "שדרות חטיבת הנגב"
    street_type_prefixes = [
        r'^\s*שדרות\s+',
        r'^\s*רחוב\s+',
        r'^\s*סמטה\s+',
        r'^\s*סמטת\s+',
        r'^\s*דרך\s+',
        r'^\s*משעול\s+',
        r'^\s*שביל\s+',
        r'^\s*מעלה\s+',
        r'^\s*כיכר\s+',
    ]
    
    for prefix_pattern in street_type_prefixes:
        name = re.sub(prefix_pattern, '', name)

    # Hebrew-transliterated Arabic prefixes (e.g. אל-סלאם, א-סלאם, אל סלאם)
    # We remove these to match the core name. 
    hebrew_arabic_prefixes = [
        (r'\bאל\s*[-־\s]\s*', ''),
        (r'\bא\s*[-־\s]\s*', ''),
    ]
    for pattern, replacement in hebrew_arabic_prefixes:
        name = re.sub(pattern, replacement, name)

    # Remove various types of dashes and hyphens, replace with space
    name = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\-]', ' ', name)
    
    # Remove punctuation (dots, commas, quotes, etc.)
    name = re.sub(r'[.,;:!?\'"(){}[\]]', '', name)
    
    # Aggressive prefix removal (e.g. removing "Rehov") is removed here 
    # to avoid ambiguity between e.g. "Rehov Hagefen" and "Simtat Hagefen".
    # The improved fuzzy matching (token_set_ratio) will handle the presence/absence of these words.

    
    # Normalize whitespace (multiple spaces -> single space)
    name = re.sub(r'\s+', ' ', name)
    
    # Final trim
    name = name.strip()
    
    return name if name else None


def normalize_city_name(name):
    """
    Normalize a Hebrew city name by:
    1. Removing punctuation and extra whitespace
    2. Standardizing format (similar to street name normalization)
    3. Aggressively removing special characters that cause mismatches (quotes, dashes)
    
    Args:
        name: City name string (can be None or NaN)
        
    Returns:
        Normalized city name string, or None if input is invalid
    """
    if pd.isna(name) or name is None:
        return None
    
    name = str(name).strip()
    
    if not name:
        return None
    
    # Standardize quotes (remove geresh/gershayim which are often used inconsistently)
    name = name.replace("'", "").replace('"', '').replace("״", "").replace("׳", "")

    # Remove various types of dashes and hyphens, replace with space
    name = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\-]', ' ', name)
    
    # Remove all other punctuation
    name = re.sub(r'[.,;:!?(){}[\]]', '', name)
    
    # Normalize whitespace (multiple spaces -> single space)
    name = re.sub(r'\s+', ' ', name)
    
    # Final trim
    name = name.strip()
    
    return name if name else None


def is_arabic(text: str) -> bool:
    """
    Detect if a string contains Arabic characters.
    Arabic Unicode range: U+0600–U+06FF
    
    Args:
        text: String to check
        
    Returns:
        True if Arabic characters are found, False otherwise
    """
    if not text or not isinstance(text, str):
        return False
    
    # Range for Arabic script
    arabic_pattern = re.compile(r'[\u0600-\u06FF]')
    return bool(arabic_pattern.search(text))


def transliterate_arabic_to_hebrew(text: str) -> str:
    """
    Phonetically transliterate Arabic script into Hebrew script.
    
    Args:
        text: Arabic string
        
    Returns:
        Hebrew transliteration
    """
    if not text:
        return text
        
    # Standard mapping based on common phonetic transliteration rules
    mapping = {
        '\u0627': 'א', # Alif
        '\u0628': 'ב', # Ba
        '\u062a': 'ת', # Ta
        '\u062b': 'ת', # Tha -> Ta (phonetic)
        '\u062c': 'ג', # Jim
        '\u062d': 'ח', # Hha
        '\u062e': 'ח', # Kha -> Kha/Khaf (using Het)
        '\u062f': 'ד', # Dal
        '\u0630': 'ד', # Thal -> Dal (phonetic)
        '\u0631': 'ר', # Ra
        '\u0632': 'ז', # Zain
        '\u0633': 'ס', # Sin
        '\u0634': 'ש', # Shin
        '\u0635': 'ס', # Sad -> Samekh
        '\u0636': 'ד', # Dad -> Dal
        '\u0637': 'ט', # Tah
        '\u0638': 'ז', # Zah -> Zain
        '\u0639': 'ע', # Ain
        '\u063a': 'ג', # Ghain -> Gimel
        '\u0641': 'פ', # Fa
        '\u0642': 'ק', # Qaf
        '\u0643': 'כ', # Kaf
        '\u0644': 'ל', # Lam
        '\u0645': 'מ', # Mim
        '\u0646': 'נ', # Nun
        '\u0647': 'ה', # Ha
        '\u0648': 'ו', # Waw
        '\u064a': 'י', # Ya
        '\u0629': 'ה', # Ta Marbuta -> He
        '\u0649': 'א', # Alif Maqsura -> Alef
        '\u0621': 'א', # Hamza -> Alef
        '\u0622': 'א', # Alif with Madda -> Alef
        '\u0623': 'א', # Alif with Hamza Above -> Alef
        '\u0624': 'ו', # Waw with Hamza Above -> Waw
        '\u0625': 'א', # Alif with Hamza Below -> Alef
        '\u0626': 'י', # Ya with Hamza Above -> Ya
    }
    
    # Final forms mapping for Hebrew
    final_forms = {
        'כ': 'ך',
        'מ': 'ם',
        'נ': 'ן',
        'פ': 'ף',
        'צ': 'ץ'
    }
    
    result = ""
    for char in text:
        result += mapping.get(char, char)
        
    # Apply Hebrew final forms
    words = result.split()
    final_words = []
    for word in words:
        if word and word[-1] in final_forms:
            word = word[:-1] + final_forms[word[-1]]
        final_words.append(word)
        
    return " ".join(final_words)


def find_fuzzy_candidates(osm_gdf, lamas_df, 
                         confident_threshold: int = 80,
                         needs_ai_threshold: int = 75):
    """
    Find fuzzy match candidates between OSM and LAMAS street data.
    
    This function processes unique street names (not individual segments) to improve
    efficiency. It uses fuzzy string matching to find potential matches.
    
    Args:
        osm_gdf: GeoDataFrame with OSM street data (must have 'normalized_name' and 'osm_id')
        lamas_df: DataFrame with LAMAS street data (must have 'normalized_name', 'LAMAS_id', 'LAMAS_name')
        confident_threshold: Score threshold for confident matches (default: 70)
        needs_ai_threshold: Score threshold for ambiguous matches that need AI (default: 50)
        
    Returns:
        DataFrame with columns: osm_id, status, best_score, best_LAMAS_id, best_LAMAS_name, all_candidates, diagnostics
        
    Status values:
        - 'CONFIDENT': Single high-confidence match (score >= confident_threshold)
        - 'NEEDS_AI': Multiple candidates or ambiguous match (score >= needs_ai_threshold)
        - 'MISSING': No good matches found (all scores < needs_ai_threshold)
    """
    import json
    
    # Get unique street names from OSM (optimization to avoid processing each segment)
    # We include osm_name (original) if available for diagnostic trace
    osm_cols = ['normalized_name']
    if 'osm_name' in osm_gdf.columns:
        osm_cols.append('osm_name')
    
    unique_osm_streets = osm_gdf[osm_cols].drop_duplicates(subset=['normalized_name']).dropna(subset=['normalized_name'])
    
    print(f"  -> Processing {len(unique_osm_streets)} unique OSM street names against {len(lamas_df)} LAMAS records...")
    
    # Store results for unique street names
    street_name_results = {}
    
    for _, osm_row in unique_osm_streets.iterrows():
        osm_name = osm_row['normalized_name']
        osm_original = osm_row.get('osm_name', osm_name)
        
        # Find all fuzzy matches for this street name
        matches = []
        for _, lamas_row in lamas_df.iterrows():
            lamas_name = lamas_row['normalized_name']
            
            if pd.isna(lamas_name):
                continue
                
            # Calculate fuzzy match score using weighted average
            score_ratio = fuzz.ratio(osm_name, lamas_name)
            score_token_sort = fuzz.token_sort_ratio(osm_name, lamas_name)
            score_token_set = fuzz.token_set_ratio(osm_name, lamas_name)

            # Weighted average - Adjusted for higher permissiveness
            # ratio (10%): strict exact match
            # token_sort_ratio (30%): handles word order differences but penalizes extra words
            # token_set_ratio (60%): handles partial matches/subset of words
            score = (score_ratio * 0.1) + (score_token_sort * 0.3) + (score_token_set * 0.6)
            
            if score >= needs_ai_threshold:
                matches.append({
                    'score': score,
                    'lamas_id': lamas_row['LAMAS_id'],
                    'lamas_name': lamas_row['LAMAS_name'],
                    'lamas_normalized': lamas_name,
                    'fuzz_ratio': score_ratio,
                    'token_sort_ratio': score_token_sort,
                    'token_set_ratio': score_token_set
                })
        
        # Group matches by LAMAS_id and take the best score for each
        if matches:
            grouped_matches = {}
            for m in matches:
                lid = m['lamas_id']
                if lid not in grouped_matches or m['score'] > grouped_matches[lid]['score']:
                    grouped_matches[lid] = m
            
            matches = list(grouped_matches.values())

        # Sort matches by score (descending)
        matches.sort(key=lambda x: x['score'], reverse=True)
        
        # Build diagnostic trace
        diag_info = {
            'normalization': {
                'osm_original': osm_original,
                'osm_normalized': osm_name
            },
            'scoring_breakdown': [
                {
                    'lamas_id': m['lamas_id'],
                    'lamas_name': m['lamas_name'],
                    'lamas_normalized': m['lamas_normalized'],
                    'final_score': round(m['score'], 2),
                    'fuzz_ratio': m['fuzz_ratio'],
                    'token_sort_ratio': m['token_sort_ratio'],
                    'token_set_ratio': m['token_set_ratio']
                } for m in matches[:5] # Top 5 candidates
            ]
        }

        # Determine status based on matches
        if not matches:
            status = 'MISSING'
            best_score = 0
            best_lamas_id = None
            best_lamas_name = None
            all_candidates = None
        elif len(matches) == 1 and matches[0]['score'] >= confident_threshold:
            # Single high-confidence match
            status = 'CONFIDENT'
            best_score = matches[0]['score']
            best_lamas_id = matches[0]['lamas_id']
            best_lamas_name = matches[0]['lamas_name']
            all_candidates = f"ID: {best_lamas_id}, Name: '{best_lamas_name}', Score: {best_score}"
        elif matches[0]['score'] >= confident_threshold:
            # Multiple matches, top one is high confidence
            # If top score is very high (>= 98), we're more permissive about the margin
            if matches[0]['score'] >= 98:
                status = 'CONFIDENT'
            # Otherwise, require a margin of at least 5 points to the second best
            elif len(matches) == 1 or matches[0]['score'] - matches[1]['score'] >= 5:
                status = 'CONFIDENT'
            else:
                status = 'NEEDS_AI'
            
            best_score = matches[0]['score']
            best_lamas_id = matches[0]['lamas_id']
            best_lamas_name = matches[0]['lamas_name']
            all_candidates = '\n'.join([
                f"ID: {m['lamas_id']}, Name: '{m['lamas_name']}', Score: {round(m['score'], 2)}"
                for m in matches[:5]
            ])
        else:
            # Ambiguous matches that need AI resolution
            status = 'NEEDS_AI'
            best_score = matches[0]['score']
            best_lamas_id = matches[0]['lamas_id']
            best_lamas_name = matches[0]['lamas_name']
            all_candidates = '\n'.join([
                f"ID: {m['lamas_id']}, Name: '{m['lamas_name']}', Score: {round(m['score'], 2)}"
                for m in matches[:5]
            ])
        
        # Store result for this unique street name
        street_name_results[osm_name] = {
            'status': status,
            'best_score': best_score,
            'best_LAMAS_id': best_lamas_id,
            'best_LAMAS_name': best_lamas_name,
            'all_candidates': all_candidates,
            'diagnostics': json.dumps(diag_info, ensure_ascii=False)
        }
    
    # Now map the results back to all OSM segments
    results = []
    for _, osm_row in osm_gdf.iterrows():
        osm_id = osm_row['osm_id']
        osm_name = osm_row['normalized_name']
        
        if pd.isna(osm_name) or osm_name not in street_name_results:
            results.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_score': 0,
                'best_LAMAS_id': None,
                'best_LAMAS_name': None,
                'all_candidates': None,
                'diagnostics': None
            })
        else:
            result = street_name_results[osm_name].copy()
            result['osm_id'] = osm_id
            results.append(result)
    
    candidates_df = pd.DataFrame(results)
    
    # Print summary statistics
    status_counts = candidates_df['status'].value_counts()
    print(f"  -> Fuzzy matching complete:")
    print(f"     CONFIDENT: {status_counts.get('CONFIDENT', 0)}")
    print(f"     NEEDS_AI: {status_counts.get('NEEDS_AI', 0)}")
    print(f"     MISSING: {status_counts.get('MISSING', 0)}")
    
    return candidates_df
