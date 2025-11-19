# src/pipeline.py
import os
import pandas as pd
from src.utils.caching import _save_intermediate_df, _safe_place_name, load_or_fetch_LAMAS, load_or_fetch_osm
from src.processing.normalization import normalize_street_name, find_fuzzy_candidates
from src.processing.map_of_adjacents import build_adjacency_map
from src.utils.ai_resolver import get_ai_resolution
from src.visualization.generate_html import create_html_from_gdf
import os

API_KEY = os.getenv("GEMINI_API_KEY") 

def _normalize_city(col: pd.Series) -> pd.Series:
    """Robustly normalizes city names."""
    return (
        col.astype(str)
        .str.replace(r'[\u2010\u2011\u2012\u2013\u2014\-]', ' ', regex=True)  # various dashes -> space
        .str.replace(r'\s+', ' ', regex=True) # multiple spaces -> single space
        .str.replace(r'עיריית', '', regex=False) # remove common suffix/prefix
        .str.strip()
    )

def calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf):
    """Calculates the diagnostic summary statistics."""
    # --- OSM Statistics (based on unique street names) ---
    # Filter out streets with no normalized name, as they aren't processed
    named_osm_gdf = osm_gdf.dropna(subset=['normalized_name'])
    total_osm_streets = named_osm_gdf['normalized_name'].nunique()

    # Calculate matches based on unique names in the diagnostic dataframe
    confident_matches = diagnostic_df_full[diagnostic_df_full['status'] == 'CONFIDENT']['normalized_name'].nunique()
    ai_resolved_matches = diagnostic_df_full[
        (diagnostic_df_full['status'] == 'NEEDS_AI') & (diagnostic_df_full['final_LAMAS_id'].notna())
    ]['normalized_name'].nunique()

    # Total matched is the count of unique OSM names that have a final LAMAS ID
    total_matched = diagnostic_df_full[diagnostic_df_full['final_LAMAS_id'].notna()]['normalized_name'].nunique()
    unmatched_osm_streets = total_osm_streets - total_matched
    
    # Get the list of unmatched OSM street names
    matched_osm_names = set(diagnostic_df_full[diagnostic_df_full['final_LAMAS_id'].notna()]['normalized_name'].unique())
    all_osm_names = set(named_osm_gdf['normalized_name'].unique())
    unmatched_osm_names = sorted(list(all_osm_names - matched_osm_names))

    # --- LAMAS Statistics (based on unique LAMAS IDs) ---
    if not lamas_in_city_df.empty:
        # Filter LAMAS data to include only actual streets (3-digit codes)
        # 4-digit codes represent non-streets: neighborhoods, squares, settlements, etc.
        lamas_streets_only = lamas_in_city_df[
            lamas_in_city_df['LAMAS_id'].astype(str).str.len() == 3
        ]
        
        total_lamas_streets = lamas_streets_only['LAMAS_id'].nunique()

        # Get the set of unique LAMAS IDs that were successfully matched to at least one OSM street
        matched_lamas_ids = set(diagnostic_df_full['final_LAMAS_id'].dropna().astype(str).str.replace(r'\.0$', '', regex=True))

        # Get the set of unique LAMAS IDs that were successfully matched
        matched_lamas_ids = set(diagnostic_df_full['final_LAMAS_id'].dropna().astype(str).str.replace(r'\.0$', '', regex=True))

        # Create a boolean Series that is True for every row where the LAMAS_id was matched
        is_matched = lamas_streets_only['LAMAS_id'].astype(str).isin(matched_lamas_ids)

        # All rows for LAMAS streets that were never matched
        unmatched_lamas_df = lamas_streets_only[~is_matched]

        # From the unmatched streets, get ONE name per unique LAMAS_id (to avoid showing all synonyms)
        # Group by LAMAS_id and take the first name for each ID
        unmatched_lamas_street_names = sorted(
            unmatched_lamas_df.groupby('LAMAS_id')['LAMAS_name'].first().tolist()
        )
        unmatched_lamas_count = unmatched_lamas_df['LAMAS_id'].nunique()
        unmatched_lamas_percentage = (unmatched_lamas_count / total_lamas_streets) * 100 if total_lamas_streets > 0 else 0
    else:
        total_lamas_streets = 0
        unmatched_lamas_street_names = []
        unmatched_lamas_count = 0
        unmatched_lamas_percentage = 0

    diagnostics = {
        "total_osm_streets": total_osm_streets,
        "total_lamas_streets": total_lamas_streets,
        "confident_matches": confident_matches,
        "ai_resolved_matches": ai_resolved_matches,
        "total_matched": total_matched,
        "unmatched_osm_streets": unmatched_osm_streets,
        "unmatched_osm_street_names": unmatched_osm_names,
        "unmatched_lamas_count": unmatched_lamas_count,
        "unmatched_lamas_percentage": f"{unmatched_lamas_percentage:.1f}%",
        "unmatched_lamas_street_names": unmatched_lamas_street_names
    }
    return diagnostics


def run_pipeline(place: str | None = None, force_refresh: bool = False, use_ai: bool = False):
    """
    מארגן את כל ה-pipeline למיפוי מזהי הרחובות.
    """
    print("--- Starting Street Mapping Orchestrator ---")
    if not API_KEY:
        print("\n*** WARNING: GEMINI_API_KEY environment variable not set. AI resolution will be skipped. ***)")

    if not use_ai:
        print("\n*** INFO: AI resolution is disabled for this run (use_ai=False). ***")
    
    # STEP 1: Data Acquisition (with caching)
    try:
        from src.data_management.OSM_streets import place_name as OSM_PLACE_NAME
        chosen_place = place or OSM_PLACE_NAME or "Tel Aviv-Yafo, Israel"
        print(f"Using place: {chosen_place}")
        LAMAS_df = load_or_fetch_LAMAS(force_refresh=force_refresh)
        osm_gdf = load_or_fetch_osm(chosen_place, force_refresh=force_refresh)


        # If OSM doesn't include a 'city' column, populate it from the place string
        if 'city' not in osm_gdf.columns:
            city_label = chosen_place.split(',')[0].strip()
            osm_gdf['city'] = city_label

        # Normalize city name formatting in both dataframes (CRITICAL FIX)
        if 'city' in LAMAS_df.columns:
            LAMAS_df['city'] = _normalize_city(LAMAS_df['city'])
        osm_gdf['city'] = _normalize_city(osm_gdf['city'])
        
        # Save intermediate normalized data
        _save_intermediate_df(LAMAS_df, "step1_lamas_raw", chosen_place)
        _save_intermediate_df(osm_gdf, "step1_osm_raw", chosen_place)
        
    except Exception as e:
        print(f"FATAL ERROR during Data Acquisition: {e}")
        return

    # STEP 2: Preprocessing and Normalization
    print("\n[Step 2/7] Normalizing street names...")
    LAMAS_df['normalized_name'] = LAMAS_df['LAMAS_name'].apply(normalize_street_name)
    
    # Normalize osm_name (which already prioritizes Hebrew from OSM_streets.py)
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
    
    # Also normalize the original name and name:he if they exist for diagnostic purposes
    if 'osm_name_original' in osm_gdf.columns:
        osm_gdf['normalized_original'] = osm_gdf['osm_name_original'].apply(normalize_street_name)
    if 'name:he' in osm_gdf.columns:
        osm_gdf['normalized_name:he'] = osm_gdf['name:he'].apply(normalize_street_name)

    # Drop rows where the final normalized_name is null
    LAMAS_df.dropna(subset=['normalized_name'], inplace=True)
    osm_gdf.dropna(subset=['normalized_name'], inplace=True)

    _save_intermediate_df(LAMAS_df, "step2_lamas_normalized", chosen_place)
    _save_intermediate_df(osm_gdf, "step2_osm_normalized", chosen_place)
    
    # STEP 3: Topology (Adjacency)
    print("\n[Step 3/7] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    # STEP 4: Candidate Matching (CREATES candidates_df)
    print("\n[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
    candidates_df = find_fuzzy_candidates(osm_gdf, LAMAS_df)
    
    _save_intermediate_df(candidates_df, "step4_candidates", chosen_place)

    # STEP 5: AI Resolution (CREATES ai_decisions_df)
    print("\n[Step 5/7] (Optional) Invoking Real AI for ambiguous cases...")
    ai_results = []

    if use_ai and API_KEY:
        # Filter candidates to the city being processed to avoid processing irrelevant streets
        osm_city_label = osm_gdf['city'].iloc[0]
        osm_gdf_in_city = osm_gdf[osm_gdf['city'] == osm_city_label]
        
        ai_candidates_to_process = candidates_df[
            (candidates_df['status'] == 'NEEDS_AI') & 
            (candidates_df['osm_id'].isin(osm_gdf_in_city['osm_id']))
        ].copy()
        
        # Make a set of OSM IDs to check against
        osm_id_set = set(osm_gdf_in_city['osm_id'])
        
        for _, row in ai_candidates_to_process.iterrows():
            osm_id = row['osm_id']

            # Safety check: skip if osm_id is not in the current city (shouldn't happen with filter above)
            if osm_id not in osm_id_set:
                 continue

            # Building the full prompt (including topological context)
            osm_street_name = osm_gdf_in_city[osm_gdf_in_city['osm_id'] == osm_id]['normalized_name'].iloc[0]
            
            # Find adjacent OSM IDs and look up their normalized names
            adjacent_osm_ids = map_of_adjacents.get(osm_id, [])
            adjacent_names = osm_gdf_in_city[osm_gdf_in_city['osm_id'].isin(adjacent_osm_ids)]['normalized_name'].tolist()

            prompt = (f"OSM Street Name: '{osm_street_name}'. "
                      f"Adjacent Streets: {', '.join(adjacent_names) if adjacent_names else 'None'}. "
                      f"LAMAS Candidates: {row['all_candidates']}. Choose the best LAMAS ID (number only or 'None').")

            ai_decision_id = get_ai_resolution(prompt, osm_id)
            ai_results.append({'osm_id': osm_id, 'ai_LAMAS_id': ai_decision_id})
    else:
        # AI disabled or missing API key — produce empty results
        ai_results = []

    # ensure DataFrame has expected columns even if empty
    ai_decisions_df = pd.DataFrame(ai_results, columns=['osm_id', 'ai_LAMAS_id'])
    _save_intermediate_df(ai_decisions_df, "step5_ai_decisions", chosen_place)

    # STEP 6: Final Merge and Mapping
    print("\n[Step 6/7] Merging results to create final diagnostic table...")
    
    # Merge AI decisions back into the candidates table
    ai_decisions_merged = candidates_df.merge(ai_decisions_df, on='osm_id', how='left')
    
    # Convert all ID columns to string for safe merging and final output consistency
    ai_decisions_merged['osm_id'] = ai_decisions_merged['osm_id'].astype(str)
    osm_gdf['osm_id'] = osm_gdf['osm_id'].astype(str)
    
    # 1. Create final_mapping_df (the rows that successfully received a final ID)
    final_mapping_df = ai_decisions_merged[
        (ai_decisions_merged['status'] == 'CONFIDENT') | 
        ((ai_decisions_merged['status'] == 'NEEDS_AI') & (ai_decisions_merged['ai_LAMAS_id'].astype(str) != 'None'))
    ].copy()

    # Determine the final ID source
    final_mapping_df['final_LAMAS_id'] = final_mapping_df.apply(
        lambda row: row['best_LAMAS_id'] if row['status'] == 'CONFIDENT' else row['ai_LAMAS_id'], axis=1
    ).astype(str)

    # Select key diagnostic columns from candidates/AI merge
    diagnostic_cols = ['osm_id', 'status', 'best_score', 'best_LAMAS_name', 'all_candidates', 'ai_LAMAS_id']
    
    # Create the full diagnostic table by merging OSM data with the candidates/AI data
    osm_cols_for_diag = ['osm_id', 'osm_name', 'normalized_name', 'city', 'geometry']
    if 'osm_name_original' in osm_gdf.columns:
        osm_cols_for_diag.insert(2, 'osm_name_original')  # Insert after osm_name
    if 'name:he' in osm_gdf.columns:
        osm_cols_for_diag.insert(2, 'name:he')  # Insert after osm_name

    diagnostic_df_full = osm_gdf[osm_cols_for_diag].merge(
        ai_decisions_merged[diagnostic_cols], on='osm_id', how='left'
    )
    
    # Merge the final LAMAS ID
    diagnostic_df_full = diagnostic_df_full.merge(
        final_mapping_df[['osm_id', 'final_LAMAS_id']], on='osm_id', how='left'
    )

    # Export final diagnostic report to CSV (new comprehensive file)
    _save_intermediate_df(diagnostic_df_full, "diagnostic_report", chosen_place)
    
    # Merge final mapping back into GeoDataFrame for visualization (Step 7)
    osm_gdf_final = osm_gdf.merge(diagnostic_df_full[['osm_id', 'final_LAMAS_id']], on='osm_id', how='left')

    print("\n-----------------------------------------------------")
    print("       _              PIPELINE COMPLETED                  ")
    print("-----------------------------------------------------")
    print(f"Total OSM streets considered: {len(osm_gdf_final)}")
    
    matched_count = final_mapping_df.shape[0]
    unmatched_count = len(osm_gdf_final) - matched_count
    
    print(f"Total successfully matched (Confident + AI): {matched_count}")
    print(f"Total unmatched (Missing/AI rejected): {unmatched_count}")
    print("\n--- Final Mapping Result Sample ---")
    print(osm_gdf_final[osm_gdf_final['final_LAMAS_id'].notna()][['osm_id', 'osm_name', 'final_LAMAS_id']].head(5))

    # STEP 6.5: Calculate Diagnostic Summary
    print("\n[Step 6.5/7] Calculating diagnostic summary...")
    osm_city_label = osm_gdf['city'].iloc[0]
    lamas_in_city_df = LAMAS_df[LAMAS_df['city'] == osm_city_label]

    diagnostics = calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf)
    print(f"-> Diagnostic Summary: {diagnostics}")

    # STEP 7: Generate HTML Visualization
    print("\n[Step 7/7] Generating HTML visualization of all streets...")
    try:
        os.makedirs("HTML", exist_ok=True)
        # Pass the GeoDataFrame and the new diagnostics object
        create_html_from_gdf(diagnostic_df_full, chosen_place, diagnostics)
    except Exception as e:
        print(f"Warning: failed to generate HTML visualization: {e}")
