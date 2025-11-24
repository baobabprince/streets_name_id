#!/usr/bin/env python3
"""
כלי לבדיקת השפעת שינוי סף ה-Fuzzy Matching

שימוש:
  python test_threshold.py <city_name> [--ai-threshold <value>] [--confident-threshold <value>]

דוגמאות:
  python test_threshold.py "אלעד"
  python test_threshold.py "אלעד" --ai-threshold 75
  python test_threshold.py "אלעד" --ai-threshold 75 --confident-threshold 88
"""

import argparse
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from normalization import normalize_street_name

def test_threshold(osm_file, lamas_file, ai_threshold=80, confident_threshold=90):
    """
    בודק את ההשפעה של סף מסוים על תוצאות ה-matching
    """
    print(f"\n{'='*70}")
    print(f"Testing with thresholds: CONFIDENT ≥ {confident_threshold}, AI ≥ {ai_threshold}")
    print(f"{'='*70}\n")
    
    # Load data
    print("Loading data...")
    osm_df = pd.read_csv(osm_file)
    lamas_df = pd.read_csv(lamas_file)
    
    # Normalize names
    if 'normalized_name' not in osm_df.columns:
        osm_df['normalized_name'] = osm_df['name'].apply(normalize_street_name)
    if 'normalized_name' not in lamas_df.columns:
        lamas_df['normalized_name'] = lamas_df['LAMAS_name'].apply(normalize_street_name)
    
    print(f"OSM streets: {len(osm_df)}")
    print(f"LAMAS streets: {len(lamas_df)}")
    
    # Calculate scores for unique OSM names to speed up processing
    unique_osm_names = osm_df[['normalized_name', 'name']].drop_duplicates('normalized_name')
    print(f"Processing {len(unique_osm_names)} unique street names (out of {len(osm_df)} total segments)")
    
    name_to_result = {}
    
    from tqdm import tqdm
    
    for _, row in tqdm(unique_osm_names.iterrows(), total=len(unique_osm_names), desc="Calculating scores"):
        osm_name_norm = row['normalized_name']
        osm_name_original = row['name']
        
        best_score = 0
        best_lamas_id = None
        best_lamas_name = None
        
        # Skip empty names
        if not osm_name_norm or pd.isna(osm_name_norm):
            name_to_result[osm_name_norm] = {
                'best_score': 0,
                'best_lamas_name': None,
                'status': 'MISSING'
            }
            continue
            
        for _, lamas_row in lamas_df.iterrows():
            lamas_id = lamas_row['LAMAS_id']
            lamas_name = lamas_row['normalized_name']
            lamas_name_raw = lamas_row['LAMAS_name']
            
            # Ensure names are strings
            if pd.isna(lamas_name): lamas_name = ""
            lamas_name = str(lamas_name)
            
            # Calculate fuzzy scores
            ratio = fuzz.ratio(str(osm_name_norm), lamas_name)
            token_sort = fuzz.token_sort_ratio(str(osm_name_norm), lamas_name)
            token_set = fuzz.token_set_ratio(str(osm_name_norm), lamas_name)
            
            weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])
            
            if weighted_score > best_score:
                best_score = weighted_score
                best_lamas_id = lamas_id
                best_lamas_name = lamas_name_raw
        
        # Classify based on thresholds
        if best_score >= confident_threshold:
            status = 'CONFIDENT'
        elif best_score >= ai_threshold:
            status = 'NEEDS_AI'
        else:
            status = 'MISSING'
            
        name_to_result[osm_name_norm] = {
            'best_score': best_score,
            'best_lamas_name': best_lamas_name,
            'status': status
        }
    
    # Map results back to all OSM segments
    results = []
    all_scores = []
    
    for _, osm_row in osm_df.iterrows():
        osm_id = osm_row['osm_id']
        osm_name_norm = osm_row['normalized_name']
        
        result = name_to_result.get(osm_name_norm, {
            'best_score': 0, 
            'best_lamas_name': None, 
            'status': 'MISSING'
        })
        
        all_scores.append(result['best_score'])
        
        results.append({
            'osm_id': osm_id,
            'osm_name': osm_row['name'],
            'best_score': result['best_score'],
            'best_lamas_name': result['best_lamas_name'],
            'status': result['status']
        })

    results_df = pd.DataFrame(results)
    
    # Print statistics
    print(f"\n{'='*70}")
    print("RESULTS SUMMARY")
    print(f"{'='*70}")
    
    confident_count = len(results_df[results_df['status'] == 'CONFIDENT'])
    ai_count = len(results_df[results_df['status'] == 'NEEDS_AI'])
    missing_count = len(results_df[results_df['status'] == 'MISSING'])
    total = len(results_df)
    
    print(f"\nStatus Distribution:")
    print(f"  CONFIDENT (≥{confident_threshold}): {confident_count:4d} ({100*confident_count/total:5.1f}%)")
    print(f"  NEEDS_AI ({ai_threshold}-{confident_threshold}):  {ai_count:4d} ({100*ai_count/total:5.1f}%)")
    print(f"  MISSING (<{ai_threshold}):   {missing_count:4d} ({100*missing_count/total:5.1f}%)")
    
    print(f"\nScore Statistics:")
    scores_array = np.array(all_scores)
    print(f"  Mean: {scores_array.mean():.2f}")
    print(f"  Median: {np.median(scores_array):.2f}")
    print(f"  Std Dev: {scores_array.std():.2f}")
    
    print(f"\nPercentiles:")
    for p in [10, 25, 50, 75, 90, 95, 99]:
        print(f"  {p:2d}th: {np.percentile(scores_array, p):5.2f}")
    
    # Show examples from each category
    print(f"\n{'='*70}")
    print("SAMPLE STREETS BY CATEGORY")
    print(f"{'='*70}")
    
    for status in ['CONFIDENT', 'NEEDS_AI', 'MISSING']:
        status_df = results_df[results_df['status'] == status]
        if not status_df.empty:
            print(f"\n{status} ({len(status_df)} streets) - Top 5 examples:")
            sample = status_df.nlargest(5, 'best_score')
            for _, row in sample.iterrows():
                print(f"  [{row['best_score']:5.2f}] '{row['osm_name']}' → '{row['best_lamas_name']}'")
    
    print(f"\n{'='*70}\n")
    
    return results_df

def compare_thresholds(osm_file, lamas_file):
    """
    משווה מספר סטים של סף כדי לעזור להחליט
    """
    threshold_sets = [
        (90, 80, "Current (strict)"),
        (88, 78, "Slightly relaxed"),
        (85, 75, "Moderately relaxed"),
        (82, 70, "More relaxed"),
    ]
    
    print(f"\n{'='*70}")
    print("THRESHOLD COMPARISON")
    print(f"{'='*70}\n")
    
    # Load data once
    osm_df = pd.read_csv(osm_file)
    lamas_df = pd.read_csv(lamas_file)
    
    if 'normalized_name' not in osm_df.columns:
        osm_df['normalized_name'] = osm_df['name'].apply(normalize_street_name)
    if 'normalized_name' not in lamas_df.columns:
        lamas_df['normalized_name'] = lamas_df['LAMAS_name'].apply(normalize_street_name)
    
    # Calculate all scores once
    print("Calculating scores for all streets...")
    all_scores = []
    
    from tqdm import tqdm
    for _, osm_row in tqdm(osm_df.iterrows(), total=len(osm_df)):
        osm_name = osm_row['normalized_name']
        best_score = 0
        
        for _, lamas_row in lamas_df.iterrows():
            lamas_name = lamas_row['normalized_name']
            ratio = fuzz.ratio(osm_name, lamas_name)
            token_sort = fuzz.token_sort_ratio(osm_name, lamas_name)
            token_set = fuzz.token_set_ratio(osm_name, lamas_name)
            weighted_score = np.average([ratio, token_sort, token_set], weights=[0.2, 0.3, 0.5])
            best_score = max(best_score, weighted_score)
        
        all_scores.append(best_score)
    
    # Compare different thresholds
    print(f"\n{'Threshold Set':<25} {'CONFIDENT':<12} {'NEEDS_AI':<12} {'MISSING':<12}")
    print("-" * 70)
    
    for confident_th, ai_th, label in threshold_sets:
        confident = sum(1 for s in all_scores if s >= confident_th)
        needs_ai = sum(1 for s in all_scores if ai_th <= s < confident_th)
        missing = sum(1 for s in all_scores if s < ai_th)
        total = len(all_scores)
        
        print(f"{label:<25} {confident:4d} ({100*confident/total:4.1f}%)  "
              f"{needs_ai:4d} ({100*needs_ai/total:4.1f}%)  "
              f"{missing:4d} ({100*missing/total:4.1f}%)")
    
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test fuzzy matching threshold impact')
    parser.add_argument('city', help='City name (e.g., "אלעד")')
    parser.add_argument('--ai-threshold', type=float, default=80,
                        help='Minimum score for AI candidates (default: 80)')
    parser.add_argument('--confident-threshold', type=float, default=90,
                        help='Minimum score for confident matches (default: 90)')
    parser.add_argument('--compare', action='store_true',
                        help='Compare multiple threshold sets')
    
    args = parser.parse_args()
    
    import glob
    import pickle
    import os
    
    # 1. Load OSM Data
    # Try to find OSM pickle files
    city_for_file = args.city.replace(' ', '_')
    pattern = f"data/osm_data_{city_for_file}*.pkl"
    matching_files = glob.glob(pattern)
    
    if not matching_files:
        # Try with spaces
        pattern = f"data/osm_data_{args.city}*.pkl"
        matching_files = glob.glob(pattern)
    
    if not matching_files:
        print(f"\nError: Could not find OSM data for '{args.city}'")
        print(f"Searched for: {pattern}")
        exit(1)
    
    # Sort files by size (ascending) - assuming smaller file is the correct specific city data
    # and larger file might be a mistake or contain too much data
    matching_files.sort(key=lambda x: os.path.getsize(x))
    
    osm_file = matching_files[0]
    print(f"Loading OSM data from: {osm_file}")
    print(f"File size: {os.path.getsize(osm_file) / 1024 / 1024:.2f} MB")
    
    try:
        with open(osm_file, 'rb') as f:
            osm_gdf = pickle.load(f)
        
        # Ensure we have a DataFrame with necessary columns
        if 'name' not in osm_gdf.columns and 'osm_name' in osm_gdf.columns:
            osm_gdf = osm_gdf.rename(columns={'osm_name': 'name'})
            
        if 'name' not in osm_gdf.columns:
            print("Error: OSM data missing 'name' column")
            print(f"Columns: {osm_gdf.columns}")
            exit(1)
            
        # Normalize OSM names
        if 'normalized_name' not in osm_gdf.columns:
            print("Normalizing OSM street names...")
            osm_gdf['normalized_name'] = osm_gdf['name'].apply(normalize_street_name)
            
        # GEOGRAPHIC VALIDATION
        try:
            # Israel/Palestine bounds: lat (29.0 to 33.5), lon (34.0 to 36.0)
            ISRAEL_BOUNDS = (29.0, 34.0, 33.5, 36.0)
            
            # Check if we have geometry
            if 'geometry' in osm_gdf.columns:
                # Calculate average coordinates
                # Handle both LineString and other geometries
                centroids = osm_gdf.geometry.centroid
                avg_lon = centroids.x.mean()
                avg_lat = centroids.y.mean()
                
                print(f"Data location: ({avg_lat:.4f}, {avg_lon:.4f})")
                
                min_lat, min_lon, max_lat, max_lon = ISRAEL_BOUNDS
                if not (min_lat <= avg_lat <= max_lat and min_lon <= avg_lon <= max_lon):
                    print(f"\n{'!'*60}")
                    print(f"WARNING: Data appears to be OUTSIDE Israel!")
                    print(f"Expected bounds: lat ({min_lat}-{max_lat}), lon ({min_lon}-{max_lon})")
                    print(f"This explains why you see foreign street names.")
                    print(f"Please delete the file '{osm_file}' and run the pipeline again with --refresh")
                    print(f"{'!'*60}\n")
        except Exception as e:
            print(f"Warning: Could not validate geographic location: {e}")

        # Statistics about the data
        total_segments = len(osm_gdf)
        named_segments = osm_gdf['name'].notna().sum()
        unique_names = osm_gdf['name'].nunique()
        
        print(f"OSM Data Stats:")
        print(f"  - Total segments: {total_segments}")
        print(f"  - Named segments: {named_segments}")
        print(f"  - Unique street names: {unique_names}")
        
        osm_df = osm_gdf[['osm_id', 'name', 'normalized_name']].drop_duplicates('osm_id')
        print(f"Loaded {len(osm_df)} unique OSM streets")
        
        # 2. Load LAMAS Data
        lamas_pickle = 'data/LAMAS_data.pkl'
        print(f"Loading LAMAS data from {lamas_pickle}...")
        
        with open(lamas_pickle, 'rb') as f:
            lamas_full = pickle.load(f)
        
        print(f"Filtering LAMAS data for city: {args.city}")
        
        # Filter LAMAS data for this city
        lamas_df = lamas_full[lamas_full['city'] == args.city].copy()
        
        if lamas_df.empty:
            print(f"Warning: No LAMAS data found for city '{args.city}'")
            # Try fuzzy matching for city name in LAMAS
            unique_cities = lamas_full['city'].unique()
            from fuzzywuzzy import process
            best_match = process.extractOne(args.city, unique_cities)
            if best_match and best_match[1] > 80:
                print(f"Did you mean '{best_match[0]}'?")
                lamas_df = lamas_full[lamas_full['city'] == best_match[0]].copy()
            else:
                print("Available cities in LAMAS:")
                print(lamas_full['city'].value_counts().head(20))
                exit(1)
        
        # Normalize LAMAS names if not already done

        if 'normalized_name' not in lamas_df.columns:
            print("Normalizing LAMAS street names...")
            lamas_df['normalized_name'] = lamas_df['LAMAS_name'].apply(normalize_street_name)
        
        print(f"Loaded {len(osm_df)} OSM streets and {len(lamas_df)} LAMAS streets")

        
        # Create temporary CSV files for the existing functions
        import tempfile
        import os
        
        with tempfile.TemporaryDirectory() as tmpdir:
            osm_file = os.path.join(tmpdir, "osm.csv")
            lamas_file = os.path.join(tmpdir, "lamas.csv")
            
            osm_df.to_csv(osm_file, index=False)
            lamas_df.to_csv(lamas_file, index=False)
            
            if args.compare:
                compare_thresholds(osm_file, lamas_file)
            else:
                results = test_threshold(osm_file, lamas_file, 
                                        args.ai_threshold, args.confident_threshold)
                
                # Save results
                output_file = f"data/{args.city}_threshold_{int(args.confident_threshold)}_{int(args.ai_threshold)}.csv"
                results.to_csv(output_file, index=False)
                print(f"Results saved to: {output_file}")
    
    except Exception as e:
        print(f"\nError processing data: {e}")
        import traceback
        traceback.print_exc()

