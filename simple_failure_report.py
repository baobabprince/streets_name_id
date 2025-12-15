#!/usr/bin/env python3
"""
Simple failure analysis based on existing batch report data.
"""

import json
import pandas as pd

def analyze_existing_failures():
    """Analyze failures from existing batch report without re-testing."""
    
    # Load the latest batch report
    with open('batch_reports/batch_summary_20251212_094457.json', 'r', encoding='utf-8') as f:
        batch_data = json.load(f)
    
    # Get statistics
    stats = batch_data['statistics']
    
    # Categorize failures
    nominatim_failures = []
    pipeline_failures = []
    
    for settlement in batch_data['results']:
        if settlement['status'] == 'failed_nominatim':
            nominatim_failures.append(settlement['settlement'])
        elif settlement['status'] == 'failed_pipeline':
            pipeline_failures.append(settlement['settlement'])
    
    # Generate report
    report = []
    report.append("=" * 80)
    report.append("BATCH PROCESSING FAILURE ANALYSIS REPORT")
    report.append("=" * 80)
    report.append(f"Generated from batch run: {batch_data['timestamp']}")
    report.append("")
    
    # Overall statistics
    report.append("OVERALL STATISTICS")
    report.append("-" * 50)
    report.append(f"Total settlements processed: {stats['total_settlements']}")
    report.append(f"Successful: {stats['successful']} ({stats['successful']/stats['total_settlements']*100:.1f}%)")
    report.append(f"Failed Nominatim: {stats['failed_nominatim']} ({stats['failed_nominatim']/stats['total_settlements']*100:.1f}%)")
    report.append(f"Failed Pipeline: {stats['failed_pipeline']} ({stats['failed_pipeline']/stats['total_settlements']*100:.1f}%)")
    report.append(f"Success rate: {batch_data.get('success_rate', '0.0%')}")
    report.append("")
    
    # Category 1: Nominatim failures
    report.append(f"1. NOMINATIM FAILURES ({len(nominatim_failures)} settlements)")
    report.append("-" * 50)
    report.append("These settlements could not be found by the geocoding service:")
    report.append("Likely reasons: Misspelled names, tribal settlements, very small/remote places")
    report.append("")
    
    # Show patterns in nominatim failures
    tribal_settlements = [s for s in nominatim_failures if ')שבט(' in s]
    other_failures = [s for s in nominatim_failures if ')שבט(' not in s]
    
    report.append(f"  Tribal settlements (שבט): {len(tribal_settlements)}")
    for settlement in tribal_settlements[:10]:
        report.append(f"    • {settlement}")
    if len(tribal_settlements) > 10:
        report.append(f"    ... and {len(tribal_settlements) - 10} more")
    report.append("")
    
    report.append(f"  Other settlements: {len(other_failures)}")
    for settlement in other_failures[:15]:
        report.append(f"    • {settlement}")
    if len(other_failures) > 15:
        report.append(f"    ... and {len(other_failures) - 15} more")
    report.append("")
    
    # Category 2: Pipeline failures
    report.append(f"2. PIPELINE FAILURES ({len(pipeline_failures)} settlements)")
    report.append("-" * 50)
    report.append("These settlements were found in Nominatim but failed during processing:")
    report.append("Likely reasons: No roads in OSM, unnamed roads only, or no matching street names")
    report.append("")
    
    # Show sample of pipeline failures
    report.append("Sample of pipeline failures:")
    for settlement in pipeline_failures[:20]:
        report.append(f"  • {settlement}")
    if len(pipeline_failures) > 20:
        report.append(f"  ... and {len(pipeline_failures) - 20} more")
    report.append("")
    
    # Analysis of settlement types
    report.append("ANALYSIS BY SETTLEMENT TYPE")
    report.append("-" * 50)
    
    all_failures = nominatim_failures + pipeline_failures
    
    # Count different types
    tribal_count = len([s for s in all_failures if ')שבט(' in s])
    kibbutz_count = len([s for s in all_failures if ')קיבוץ(' in s or 'קיבוץ' in s])
    moshav_count = len([s for s in all_failures if ')מושב(' in s or 'מושב' in s])
    camp_count = len([s for s in all_failures if 'מחנה' in s])
    other_count = len(all_failures) - tribal_count - kibbutz_count - moshav_count - camp_count
    
    report.append(f"Tribal settlements (שבט): {tribal_count} ({tribal_count/len(all_failures)*100:.1f}%)")
    report.append(f"Kibbutzim: {kibbutz_count} ({kibbutz_count/len(all_failures)*100:.1f}%)")
    report.append(f"Moshavim: {moshav_count} ({moshav_count/len(all_failures)*100:.1f}%)")
    report.append(f"Military camps (מחנה): {camp_count} ({camp_count/len(all_failures)*100:.1f}%)")
    report.append(f"Other settlements: {other_count} ({other_count/len(all_failures)*100:.1f}%)")
    report.append("")
    
    # Recommendations
    report.append("RECOMMENDATIONS")
    report.append("-" * 50)
    report.append("1. Tribal settlements (שבט) - Many not in OSM/Nominatim databases")
    report.append("   → Consider alternative geocoding sources or manual coordinate mapping")
    report.append("")
    report.append("2. Pipeline failures - Settlements found but no processable street data")
    report.append("   → These are legitimate cases where OSM has no named streets")
    report.append("   → Consider marking as 'no_street_data' instead of 'failed'")
    report.append("")
    report.append("3. Small settlements - Many have roads but no street names in OSM")
    report.append("   → This is expected for rural/small settlements")
    report.append("   → Current behavior is correct - cannot match unnamed roads")
    
    return "\n".join(report)

if __name__ == "__main__":
    print("Generating failure analysis report...")
    
    report = analyze_existing_failures()
    
    # Save report
    with open('batch_reports/failure_analysis_summary.txt', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("Report saved to: batch_reports/failure_analysis_summary.txt")
    print("\n" + report)
