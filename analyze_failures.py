#!/usr/bin/env python3
"""
Analyze batch processing failures and categorize them by reason.
"""

import json
import pandas as pd
import osmnx as ox
from OSM_streets import fetch_osm_street_data

def analyze_failure_reasons():
    """Analyze all failures from the latest batch report and categorize by reason."""
    
    # Load the latest batch report
    with open('batch_reports/batch_summary_20251212_094457.json', 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    
    failures = {
        'nominatim_not_found': [],
        'osm_no_roads': [],
        'osm_unnamed_roads': [],
        'osm_named_but_no_matches': [],
        'other_errors': []
    }
    
    print("Analyzing failures...")
    
    for settlement in batch_data['results']:
        if settlement['status'] in ['failed_nominatim', 'failed_pipeline']:
            settlement_name = settlement['settlement']
            print(f"Checking: {settlement_name}")
            
            try:
                # Try to find in Nominatim
                location = ox.geocoder.geocode(settlement_name + ', Israel')
                if not location:
                    failures['nominatim_not_found'].append({
                        'name': settlement_name,
                        'reason': 'Not found in Nominatim geocoder'
                    })
                    continue
                
                # Try to get OSM data
                gdf = fetch_osm_street_data(settlement_name + ', Israel')
                if gdf is None or len(gdf) == 0:
                    failures['osm_no_roads'].append({
                        'name': settlement_name,
                        'reason': 'No roads found in OSM'
                    })
                    continue
                
                # Check if roads have names
                named_count = gdf['osm_name'].notna().sum()
                if named_count == 0:
                    failures['osm_unnamed_roads'].append({
                        'name': settlement_name,
                        'reason': f'Has {len(gdf)} roads but all unnamed',
                        'total_roads': len(gdf)
                    })
                else:
                    failures['osm_named_but_no_matches'].append({
                        'name': settlement_name,
                        'reason': f'Has {named_count} named streets but no LAMAS matches',
                        'total_roads': len(gdf),
                        'named_roads': named_count,
                        'street_names': gdf[gdf['osm_name'].notna()]['osm_name'].unique()[:5].tolist()
                    })
                    
            except Exception as e:
                failures['other_errors'].append({
                    'name': settlement_name,
                    'reason': f'Error: {str(e)}'
                })
    
    return failures

def generate_failure_report(failures):
    """Generate a comprehensive failure report."""
    
    report = []
    report.append("=" * 80)
    report.append("BATCH PROCESSING FAILURE ANALYSIS REPORT")
    report.append("=" * 80)
    
    total_failures = sum(len(category) for category in failures.values())
    report.append(f"Total failures analyzed: {total_failures}")
    report.append("")
    
    # Category 1: Not found in Nominatim
    category = failures['nominatim_not_found']
    report.append(f"1. NOT FOUND IN NOMINATIM ({len(category)} settlements)")
    report.append("-" * 50)
    report.append("These settlements cannot be located by the geocoding service:")
    for item in category[:10]:  # Show first 10
        report.append(f"  • {item['name']}")
    if len(category) > 10:
        report.append(f"  ... and {len(category) - 10} more")
    report.append("")
    
    # Category 2: No roads in OSM
    category = failures['osm_no_roads']
    report.append(f"2. NO ROADS IN OSM ({len(category)} settlements)")
    report.append("-" * 50)
    report.append("Found in Nominatim but no road data in OpenStreetMap:")
    for item in category[:10]:
        report.append(f"  • {item['name']}")
    if len(category) > 10:
        report.append(f"  ... and {len(category) - 10} more")
    report.append("")
    
    # Category 3: Unnamed roads
    category = failures['osm_unnamed_roads']
    report.append(f"3. UNNAMED ROADS ONLY ({len(category)} settlements)")
    report.append("-" * 50)
    report.append("Have roads in OSM but no street names:")
    for item in category[:10]:
        report.append(f"  • {item['name']} ({item['total_roads']} unnamed roads)")
    if len(category) > 10:
        report.append(f"  ... and {len(category) - 10} more")
    report.append("")
    
    # Category 4: Named streets but no matches
    category = failures['osm_named_but_no_matches']
    report.append(f"4. NAMED STREETS BUT NO LAMAS MATCHES ({len(category)} settlements)")
    report.append("-" * 50)
    report.append("Have named streets but don't match official LAMAS registry:")
    for item in category[:5]:
        report.append(f"  • {item['name']} ({item['named_roads']} named streets)")
        report.append(f"    Streets: {', '.join(item['street_names'])}")
    if len(category) > 5:
        report.append(f"  ... and {len(category) - 5} more")
    report.append("")
    
    # Category 5: Other errors
    category = failures['other_errors']
    if category:
        report.append(f"5. OTHER ERRORS ({len(category)} settlements)")
        report.append("-" * 50)
        for item in category:
            report.append(f"  • {item['name']}: {item['reason']}")
        report.append("")
    
    # Summary
    report.append("SUMMARY")
    report.append("-" * 50)
    report.append(f"Not found in Nominatim: {len(failures['nominatim_not_found'])} ({len(failures['nominatim_not_found'])/total_failures*100:.1f}%)")
    report.append(f"No roads in OSM: {len(failures['osm_no_roads'])} ({len(failures['osm_no_roads'])/total_failures*100:.1f}%)")
    report.append(f"Unnamed roads only: {len(failures['osm_unnamed_roads'])} ({len(failures['osm_unnamed_roads'])/total_failures*100:.1f}%)")
    report.append(f"Named streets, no matches: {len(failures['osm_named_but_no_matches'])} ({len(failures['osm_named_but_no_matches'])/total_failures*100:.1f}%)")
    if failures['other_errors']:
        report.append(f"Other errors: {len(failures['other_errors'])} ({len(failures['other_errors'])/total_failures*100:.1f}%)")
    
    return "\n".join(report)

if __name__ == "__main__":
    print("Starting failure analysis...")
    failures = analyze_failure_reasons()
    
    report = generate_failure_report(failures)
    
    # Save report
    with open('batch_reports/failure_analysis_report.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    # Also save detailed JSON
    with open('batch_reports/failure_analysis_detailed.json', 'w', encoding='utf-8') as f:
        json.dump(failures, f, ensure_ascii=False, indent=2)
    
    print("\nReport saved to:")
    print("- batch_reports/failure_analysis_report.txt")
    print("- batch_reports/failure_analysis_detailed.json")
    
    print("\n" + "="*50)
    print(report)
