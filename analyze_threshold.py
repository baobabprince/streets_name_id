#!/usr/bin/env python3
"""
כלי לניתוח השפעת שינוי סף ה-Fuzzy Matching

שימוש:
  python analyze_threshold.py <city_name>

דוגמה:
  python analyze_threshold.py "אלעד"
"""

import sys
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
    
    # Construct file paths based on city name
    osm_file = f"data/{city}_OSM.csv"
    lamas_file = f"data/{city}_LAMAS.csv"
    
    try:
        results = analyze_threshold_impact(osm_file, lamas_file)
        
        # Save results for further analysis
        output_file = f"data/{city}_threshold_analysis.csv"
        results.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find data files for city '{city}'")
        print(f"Looking for: {osm_file} and {lamas_file}")
        print(f"\nMake sure to run the pipeline first to generate these files.")
        sys.exit(1)
