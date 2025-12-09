"""
Street Name Normalization and Fuzzy Matching Module

This module provides functions for:
1. Normalizing Hebrew street names (expanding abbreviations, removing punctuation)
2. Finding fuzzy match candidates between OSM and LAMAS street data
"""

import re
import pandas as pd
from fuzzywuzzy import fuzz
from typing import List, Dict, Tuple


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
        (r'\bדר\.', 'דרך'),
        (r'\bסמ\'', 'סמטה'),
        (r'\bסמ\.', 'סמטה'),
        (r'\bסמט\'', 'סמטת'),  # Handle סמט' variation
        (r'\bסמט\.', 'סמטת'),
    ]
    
    for pattern, replacement in abbreviations:
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


def find_fuzzy_candidates(osm_gdf, lamas_df, 
                         confident_threshold: int = 90,
                         needs_ai_threshold: int = 60):
    """
    Find fuzzy match candidates between OSM and LAMAS street data.
    
    This function processes unique street names (not individual segments) to improve
    efficiency. It uses fuzzy string matching to find potential matches.
    
    Args:
        osm_gdf: GeoDataFrame with OSM street data (must have 'normalized_name' and 'osm_id')
        lamas_df: DataFrame with LAMAS street data (must have 'normalized_name', 'LAMAS_id', 'LAMAS_name')
        confident_threshold: Score threshold for confident matches (default: 95)
        needs_ai_threshold: Score threshold for ambiguous matches that need AI (default: 80)
        
    Returns:
        DataFrame with columns: osm_id, status, best_score, best_LAMAS_id, best_LAMAS_name, all_candidates
        
    Status values:
        - 'CONFIDENT': Single high-confidence match (score >= confident_threshold)
        - 'NEEDS_AI': Multiple candidates or ambiguous match (score >= needs_ai_threshold)
        - 'MISSING': No good matches found (all scores < needs_ai_threshold)
    """
    
    # Get unique street names from OSM (optimization to avoid processing each segment)
    unique_osm_streets = osm_gdf[['normalized_name']].drop_duplicates().dropna()
    
    print(f"  -> Processing {len(unique_osm_streets)} unique OSM street names against {len(lamas_df)} LAMAS records...")
    
    # Store results for unique street names
    street_name_results = {}
    
    for _, osm_row in unique_osm_streets.iterrows():
        osm_name = osm_row['normalized_name']
        
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

            # Weighted average
            # ratio: strict exact match (handles typos well)
            # token_sort_ratio: handles word order differences
            # token_set_ratio: (handles partial matches/subset of words)
            score = (score_ratio * 0.4) + (score_token_sort * 0.3) + (score_token_set * 0.3)
            
            if score >= needs_ai_threshold:
                matches.append({
                    'score': score,
                    'lamas_id': lamas_row['LAMAS_id'],
                    'lamas_name': lamas_row['LAMAS_name']
                })
        
        # Sort matches by score (descending)
        matches.sort(key=lambda x: x['score'], reverse=True)
        
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
        elif matches[0]['score'] >= confident_threshold and len(matches) > 1:
            # Multiple matches, top one is high confidence but there are others
            # Check if the top match is significantly better than the second
            if len(matches) == 1 or matches[0]['score'] - matches[1]['score'] >= 5:
                status = 'CONFIDENT'
                best_score = matches[0]['score']
                best_lamas_id = matches[0]['lamas_id']
                best_lamas_name = matches[0]['lamas_name']
            else:
                status = 'NEEDS_AI'
                best_score = matches[0]['score']
                best_lamas_id = matches[0]['lamas_id']
                best_lamas_name = matches[0]['lamas_name']
            
            # Format all candidates for AI context
            all_candidates = '\n'.join([
                f"ID: {m['lamas_id']}, Name: '{m['lamas_name']}', Score: {m['score']}"
                for m in matches[:5]  # Top 5 candidates
            ])
        else:
            # Ambiguous matches that need AI resolution
            status = 'NEEDS_AI'
            best_score = matches[0]['score']
            best_lamas_id = matches[0]['lamas_id']
            best_lamas_name = matches[0]['lamas_name']
            all_candidates = '\n'.join([
                f"ID: {m['lamas_id']}, Name: '{m['lamas_name']}', Score: {m['score']}"
                for m in matches[:5]  # Top 5 candidates
            ])
        
        # Store result for this unique street name
        street_name_results[osm_name] = {
            'status': status,
            'best_score': best_score,
            'best_LAMAS_id': best_lamas_id,
            'best_LAMAS_name': best_lamas_name,
            'all_candidates': all_candidates
        }
    
    # Now map the results back to all OSM segments
    results = []
    for _, osm_row in osm_gdf.iterrows():
        osm_id = osm_row['osm_id']
        osm_name = osm_row['normalized_name']
        
        if pd.isna(osm_name) or osm_name not in street_name_results:
            # Unnamed street or not in results
            results.append({
                'osm_id': osm_id,
                'status': 'MISSING',
                'best_score': 0,
                'best_LAMAS_id': None,
                'best_LAMAS_name': None,
                'all_candidates': None
            })
        else:
            # Use the cached result for this street name
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
