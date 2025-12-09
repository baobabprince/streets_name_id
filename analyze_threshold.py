#!/usr/bin/env python3
"""
כלי לניתוח השפעת שינוי סף ה-Fuzzy Matching

שימוש:
  python analyze_threshold.py <city_name>

דוגמה:
  python analyze_threshold.py "אלעד"
"""

import sys
import os
import pandas as pd
from normalization import find_fuzzy_candidates, normalize_street_name

def analyze_threshold_impact(osm_file, lamas_file):
    """
    מנתח את ההשפעה של סף ה-fuzzy matching על בסיס נתונים קיימים
    """
    print("Loading data...")
    osm_df = pd.read_csv(osm_file)
    lamas_df = pd.read_csv(lamas_file)
    
    # Normalize names if not already done
    if 'normalized_name' not in osm_df.columns:
        osm_df['normalized_name'] = osm_df['name'].apply(normalize_street_name)
    if 'normalized_name' not in lamas_df.columns:
        lamas_df['normalized_name'] = lamas_df['LAMAS_name'].apply(normalize_street_name)
    
    print(f"\nAnalyzing {len(osm_df)} OSM streets against {len(lamas_df)} LAMAS streets...")
    
    # Run fuzzy matching - this will now print detailed statistics
    candidates_df = find_fuzzy_candidates(osm_df, lamas_df)
    
    # Additional analysis: show examples of streets in each category
    print("\n" + "="*60)
    print("SAMPLE STREETS BY CATEGORY")
    print("="*60)
    
    for status in ['CONFIDENT', 'NEEDS_AI', 'MISSING']:
        status_df = candidates_df[candidates_df['status'] == status]
        if not status_df.empty:
            print(f"\n{status} ({len(status_df)} streets):")
            # Merge with OSM data to get street names
            merged = status_df.merge(osm_df[['osm_id', 'name', 'normalized_name']], on='osm_id')
            
            # Show top 5 examples
            sample = merged.head(5)
            for _, row in sample.iterrows():
                osm_name = row['name']
                lamas_name = row.get('best_LAMAS_name', 'N/A')
                score = row['best_score']
                print(f"  • OSM: '{osm_name}' → LAMAS: '{lamas_name}' (Score: {score:.2f})")
    
    print("="*60)
    
    return candidates_df

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_threshold.py <city_name>")
        print("Example: python analyze_threshold.py 'אלעד'")
        sys.exit(1)
    
    city = sys.argv[1]
    
    # Find matching files in data directory
    data_dir = "data"
    osm_files = [f for f in os.listdir(data_dir) if f.startswith(f"step2_osm_normalized_{city}") and f.endswith(".pkl")]
    lamas_files = [f for f in os.listdir(data_dir) if f.startswith(f"step2_lamas_normalized_{city}") and f.endswith(".pkl")]
    
    if not osm_files or not lamas_files:
        print(f"Error: Could not find normalized data files for city query '{city}' in {data_dir}")
        print(f"Make sure you have run the pipeline for this city first.")
        sys.exit(1)
        
    osm_file = os.path.join(data_dir, osm_files[0])
    lamas_file = os.path.join(data_dir, lamas_files[0])
    
    print(f"Using OSM file: {osm_file}")
    print(f"Using LAMAS file: {lamas_file}")
    
    try:
        # Load pickle files directly
        print("Loading data...")
        osm_df = pd.read_pickle(osm_file)
        lamas_df = pd.read_pickle(lamas_file)
        
        # Filter LAMAS data for the specific city if needed
        # (The pipeline does this in step 4, we should replicate or rely on correct data)
        if 'city' in osm_df.columns and not osm_df.empty:
            city_name = osm_df['city'].iloc[0]
            print(f"Filtering LAMAS data for city: {city_name}")
            lamas_df = lamas_df[lamas_df['city'] == city_name]
        
        print(f"\nAnalyzing {len(osm_df)} OSM streets against {len(lamas_df)} LAMAS streets...")
        
        # Run fuzzy matching
        candidates_df = find_fuzzy_candidates(osm_df, lamas_df)
        
        # Additional analysis
        print("\n" + "="*60)
        print("SAMPLE STREETS BY CATEGORY")
        print("="*60)
        
        for status in ['CONFIDENT', 'NEEDS_AI', 'MISSING']:
            status_df = candidates_df[candidates_df['status'] == status]
            if not status_df.empty:
                print(f"\n{status} ({len(status_df)} streets):")
                # Merge with OSM data to get street names
                merged = status_df.merge(osm_df[['osm_id', 'normalized_name']], on='osm_id')
                if 'osm_name' in osm_df.columns:
                     merged = merged.merge(osm_df[['osm_id', 'osm_name']], on='osm_id', suffixes=('', '_orig'))
                
                # Show top 5 examples
                sample = merged.head(5)
                for _, row in sample.iterrows():
                    osm_name = row.get('osm_name', row['normalized_name'])
                    lamas_name = row.get('best_LAMAS_name', 'N/A')
                    score = row['best_score']
                    print(f"  • OSM: '{osm_name}' → LAMAS: '{lamas_name}' (Score: {score:.2f})")
        
        print("="*60)
        
        # Save results
        output_file = f"data/{city}_threshold_analysis.csv"
        candidates_df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
    except Exception as e:
        print(f"Error analyzing thresholds: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
