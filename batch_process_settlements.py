#!/usr/bin/env python3
"""
Batch Settlement Processing Script

Processes all settlements from the LAMAS (CBS) database by:
1. Extracting unique settlement names
2. Finding each settlement in OSM using Nominatim
3. Validating geographic reasonableness
4. Running the pipeline on each successfully matched settlement
5. Generating comprehensive summary reports
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from pathlib import Path

# Import local modules
from settlement_matcher import SettlementMatcher, SettlementMatch
from lamas_streets import fetch_all_LAMAS_data
from pipeline import run_pipeline


# Output directory for reports
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "batch_reports")
os.makedirs(REPORTS_DIR, exist_ok=True)


class BatchProcessor:
    """Orchestrates batch processing of all settlements"""
    
    def __init__(self, use_ai: bool = False, skip_html: bool = False, quiet: bool = False):
        self.use_ai = use_ai
        self.skip_html = skip_html
        self.quiet = quiet
        self.matcher = SettlementMatcher()
        
        # Statistics tracking
        self.stats = {
            'total_settlements': 0,
            'matched': 0,
            'failed_nominatim': 0,
            'failed_validation': 0,
            'failed_pipeline': 0,
            'skipped_already_processed': 0,
            'successful': 0
        }
        
        # Detailed results
        self.results = []
        
        # Load already processed settlements
        self.processed_settlements_file = os.path.join(REPORTS_DIR, "processed_settlements.json")
        self.processed_settlements = self._load_processed_settlements()
    
    def _load_processed_settlements(self) -> set:
        """Load set of already processed settlements"""
        if os.path.exists(self.processed_settlements_file):
            try:
                with open(self.processed_settlements_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('settlements', []))
            except Exception as e:
                print(f"Warning: Failed to load processed settlements: {e}")
        return set()
    
    def _save_processed_settlement(self, settlement_name: str):
        """Add settlement to processed list and save"""
        self.processed_settlements.add(settlement_name)
        try:
            with open(self.processed_settlements_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'settlements': sorted(list(self.processed_settlements)),
                    'last_updated': datetime.now().isoformat()
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save processed settlements: {e}")
    
    def get_unique_settlements(self, lamas_df: pd.DataFrame) -> List[str]:
        """Extract unique settlement names from LAMAS data"""
        if 'city' not in lamas_df.columns:
            print("Error: LAMAS data doesn't have 'city' column")
            return []
        
        # Get unique settlements, sorted alphabetically
        settlements = sorted(lamas_df['city'].dropna().unique().tolist())
        
        print(f"\nFound {len(settlements)} unique settlements in LAMAS data")
        return settlements
    
    def process_settlement(self, settlement_name: str, force: bool = False) -> Dict:
        """
        Process a single settlement through the entire pipeline.
        
        Args:
            settlement_name: Name of the settlement to process
            force: Force reprocessing even if already processed
            
        Returns:
            Dictionary with processing results
        """
        result = {
            'settlement': settlement_name,
            'status': 'unknown',
            'message': '',
            'match': None,
            'pipeline_success': False,
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"\n{'='*70}")
        print(f"Processing: {settlement_name}")
        print(f"{'='*70}")
        
        # Check if already processed
        if not force and settlement_name in self.processed_settlements:
            print(f"  ⏭ Skipping - already processed")
            result['status'] = 'skipped'
            result['message'] = 'Already processed (use --force to reprocess)'
            self.stats['skipped_already_processed'] += 1
            return result
        
        # Step 1: Search Nominatim
        match = self.matcher.search_settlement(settlement_name)
        
        if not match:
            print(f"  ✗ Failed to find valid match in Nominatim")
            result['status'] = 'failed_nominatim'
            result['message'] = 'No valid Nominatim match found'
            self.stats['failed_nominatim'] += 1
            return result
        
        if not match.is_valid:
            print(f"  ✗ Match failed validation: {match.validation_message}")
            result['status'] = 'failed_validation'
            result['message'] = match.validation_message
            result['match'] = {
                'display_name': match.display_name,
                'lat': match.lat,
                'lon': match.lon
            }
            self.stats['failed_validation'] += 1
            return result
        
        print(f"  ✓ Valid match found: {match.display_name}")
        result['match'] = {
            'display_name': match.display_name,
            'lat': match.lat,
            'lon': match.lon,
            'place_type': match.place_type,
            'osm_id': match.osm_id
        }
        self.stats['matched'] += 1
        
        # Step 2: Run the pipeline
        print(f"  → Running pipeline for {settlement_name}...")
        
        try:
            # Construct the place string for OSM query
            # Use the display name from Nominatim for better accuracy
            place_string = match.display_name
            
            # Run the pipeline
            pipeline_success = run_pipeline(
                place=place_string,
                force_refresh=False,
                use_ai=self.use_ai
            )
            
            if pipeline_success is None or pipeline_success:
                print(f"  ✓ Pipeline completed successfully")
                result['status'] = 'success'
                result['message'] = 'Pipeline completed successfully'
                result['pipeline_success'] = True
                self.stats['successful'] += 1
                
                # Mark as processed
                self._save_processed_settlement(settlement_name)
            else:
                print(f"  ✗ Pipeline failed")
                result['status'] = 'failed_pipeline'
                result['message'] = 'Pipeline execution failed'
                self.stats['failed_pipeline'] += 1
                
        except Exception as e:
            print(f"  ✗ Pipeline error: {e}")
            result['status'] = 'failed_pipeline'
            result['message'] = f'Pipeline exception: {str(e)}'
            self.stats['failed_pipeline'] += 1
        
        return result
    
    def generate_summary_report(self, output_file: Optional[str] = None):
        """Generate comprehensive summary report"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(REPORTS_DIR, f"batch_summary_{timestamp}.json")
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'statistics': self.stats,
            'success_rate': f"{(self.stats['successful'] / self.stats['total_settlements'] * 100):.1f}%" if self.stats['total_settlements'] > 0 else "0%",
            'results': self.results
        }
        
        # Save JSON report
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        print(f"\n{'='*70}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"Total settlements: {self.stats['total_settlements']}")
        print(f"Successful: {self.stats['successful']} ({self.stats['successful'] / self.stats['total_settlements'] * 100:.1f}%)" if self.stats['total_settlements'] > 0 else "Successful: 0")
        print(f"Matched in Nominatim: {self.stats['matched']}")
        print(f"Failed Nominatim search: {self.stats['failed_nominatim']}")
        print(f"Failed validation: {self.stats['failed_validation']}")
        print(f"Failed pipeline: {self.stats['failed_pipeline']}")
        print(f"Skipped (already processed): {self.stats['skipped_already_processed']}")
        print(f"\nDetailed report saved to: {output_file}")
        
        # Generate human-readable text report
        text_report_file = output_file.replace('.json', '.txt')
        self._generate_text_report(text_report_file)
        print(f"Text report saved to: {text_report_file}")
        
        return summary
    
    def _generate_text_report(self, output_file: str):
        """Generate human-readable text report"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("BATCH SETTLEMENT PROCESSING REPORT\n")
            f.write("="*70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("STATISTICS\n")
            f.write("-"*70 + "\n")
            f.write(f"Total settlements: {self.stats['total_settlements']}\n")
            f.write(f"Successful: {self.stats['successful']}\n")
            f.write(f"Matched in Nominatim: {self.stats['matched']}\n")
            f.write(f"Failed Nominatim search: {self.stats['failed_nominatim']}\n")
            f.write(f"Failed validation: {self.stats['failed_validation']}\n")
            f.write(f"Failed pipeline: {self.stats['failed_pipeline']}\n")
            f.write(f"Skipped (already processed): {self.stats['skipped_already_processed']}\n\n")
            
            # Group results by status
            by_status = {}
            for result in self.results:
                status = result['status']
                if status not in by_status:
                    by_status[status] = []
                by_status[status].append(result)
            
            # Write successful settlements
            if 'success' in by_status:
                f.write("\nSUCCESSFUL SETTLEMENTS\n")
                f.write("-"*70 + "\n")
                for result in by_status['success']:
                    f.write(f"✓ {result['settlement']}\n")
                    if result.get('match'):
                        f.write(f"  → {result['match']['display_name']}\n")
            
            # Write failed settlements
            for status in ['failed_nominatim', 'failed_validation', 'failed_pipeline']:
                if status in by_status:
                    f.write(f"\n{status.upper().replace('_', ' ')}\n")
                    f.write("-"*70 + "\n")
                    for result in by_status[status]:
                        f.write(f"✗ {result['settlement']}\n")
                        f.write(f"  → {result['message']}\n")
                        if result.get('match'):
                            f.write(f"  → Match: {result['match']['display_name']}\n")
    
    def run_batch(self, settlements: List[str], limit: Optional[int] = None, 
                  force: bool = False, dry_run: bool = False):
        """
        Run batch processing on a list of settlements.
        
        Args:
            settlements: List of settlement names to process
            limit: Maximum number of settlements to process (for testing)
            force: Force reprocessing of already processed settlements
            dry_run: Show what would be processed without actually running
        """
        if limit:
            settlements = settlements[:limit]
        
        self.stats['total_settlements'] = len(settlements)
        
        if dry_run:
            print(f"\n{'='*70}")
            print("DRY RUN MODE - No actual processing will occur")
            print(f"{'='*70}")
            print(f"\nWould process {len(settlements)} settlements:")
            for i, settlement in enumerate(settlements, 1):
                status = "SKIP" if settlement in self.processed_settlements else "PROCESS"
                print(f"  {i:3d}. [{status}] {settlement}")
            return
        
        print(f"\n{'='*70}")
        print(f"Starting batch processing of {len(settlements)} settlements")
        print(f"AI Resolution: {'ENABLED' if self.use_ai else 'DISABLED'}")
        print(f"{'='*70}")
        
        for i, settlement in enumerate(settlements, 1):
            print(f"\n[{i}/{len(settlements)}] ", end='')
            
            result = self.process_settlement(settlement, force=force)
            self.results.append(result)
            
            # Save intermediate results every 10 settlements
            if i % 10 == 0:
                self.generate_summary_report()
        
        # Generate final summary
        self.generate_summary_report()


def main():
    parser = argparse.ArgumentParser(
        description="Batch process all settlements from LAMAS database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all settlements with AI
  python batch_process_settlements.py --use-ai
  
  # Process first 10 settlements without AI (testing)
  python batch_process_settlements.py --limit 10
  
  # Dry run to see what would be processed
  python batch_process_settlements.py --dry-run
  
  # Force reprocess all settlements
  python batch_process_settlements.py --force --use-ai
        """
    )
    
    parser.add_argument('--use-ai', action='store_true',
                       help='Enable AI resolution for ambiguous matches')
    parser.add_argument('--limit', type=int, metavar='N',
                       help='Limit processing to first N settlements (for testing)')
    parser.add_argument('--force', action='store_true',
                       help='Force reprocessing of already processed settlements')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be processed without actually running')
    parser.add_argument('--skip-html', action='store_true',
                       help='Skip HTML generation (faster for batch processing)')
    parser.add_argument('--quiet', action='store_true',
                       help='Reduce console output')
    
    args = parser.parse_args()
    
    # Step 1: Fetch LAMAS data
    print("Fetching LAMAS data...")
    lamas_df = fetch_all_LAMAS_data()
    
    if lamas_df.empty:
        print("Error: Failed to fetch LAMAS data")
        sys.exit(1)
    
    # Step 2: Initialize batch processor
    processor = BatchProcessor(
        use_ai=args.use_ai,
        skip_html=args.skip_html,
        quiet=args.quiet
    )
    
    # Step 3: Get unique settlements
    settlements = processor.get_unique_settlements(lamas_df)
    
    if not settlements:
        print("Error: No settlements found in LAMAS data")
        sys.exit(1)
    
    # Step 4: Run batch processing
    processor.run_batch(
        settlements=settlements,
        limit=args.limit,
        force=args.force,
        dry_run=args.dry_run
    )
    
    print("\n✓ Batch processing complete!")


if __name__ == "__main__":
    main()
