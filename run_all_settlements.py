# run_all_settlements.py
import argparse
import pandas as pd
from pipeline import run_pipeline, load_or_fetch_LAMAS
from OSM_streets import check_place_exists_in_osm

def main():
    """
    Orchestrates the execution of the street mapping pipeline for all unique settlements
    found in the LAMAS dataset.

    This script fetches a complete list of settlements from LAMAS data, then iterates
    through each one, calling the main pipeline function for it. It provides
    command-line arguments to control caching, AI usage, and to limit the number of
    settlements for testing purposes.

    Command-line arguments:
      --no-ai: Disables the AI resolution step in the pipeline.
      --refresh: Forces a re-download of all data, ignoring caches.
    """
    parser = argparse.ArgumentParser(description="Run the street mapping pipeline for all settlements.")
    parser.add_argument("--no-ai", action="store_true", help="Disable AI resolution.")
    parser.add_argument("--refresh", action="store_true", help="Force refresh of all cached data.")
    args = parser.parse_args()

    # --- 1. Fetch and Prepare Settlement List ---
    print("Fetching all LAMAS data to get a list of settlements...")
    lamas_df = load_or_fetch_LAMAS(force_refresh=args.refresh)
    if lamas_df.empty:
        print("Could not retrieve LAMAS data. Exiting.")
        return

    settlements = sorted(list(lamas_df['city'].unique()))
    print(f"Found {len(settlements)} unique settlements.")

    # --- 2. Iterate and Run Pipeline for Each Settlement ---
    for i, settlement in enumerate(settlements):
        print(f"\\n--- Processing settlement {i+1}/{len(settlements)}: {settlement} ---")

        # --- 3. Find a valid OSM place name variant ---
        # Try different variations of the settlement name to find one that exists in OSM
        variants = [
            settlement,
            settlement.replace("-", " "),
            settlement.replace("'", ""),
            settlement.replace("-", " ").replace("'", "")
        ]

        valid_place_name = None
        # Use dict.fromkeys to remove duplicates while preserving order
        for variant in list(dict.fromkeys(variants)):
            if check_place_exists_in_osm(variant):
                valid_place_name = variant
                print(f"Found valid OSM name for '{settlement}': '{valid_place_name}'")
                break

        if not valid_place_name:
            print(f"Skipping '{settlement}' after trying variants: {', '.join(variants)}")
            continue

        # --- 4. Execute Pipeline with the Validated Name ---
        try:
            run_pipeline(
                place=valid_place_name,
                force_refresh=args.refresh,
                use_ai=not args.no_ai
            )
        except Exception as e:
            # Catch exceptions to ensure the script continues to the next settlement
            print(f"ERROR: Pipeline failed for {settlement}. Reason: {e}")
            print("Continuing to the next settlement.")

if __name__ == "__main__":
    main()