# diagnose_pipeline.py
"""
Diagnostic script for pipeline matching failures.
Loads cached LAMAS/OSM, shows schemas, normalization, and runs fuzzy candidate generation
prints counts and examples so we can see why final mapping was empty.
"""
from pipeline import (
    load_or_fetch_LAMAS,
    load_or_fetch_osm,
    normalize_street_name,
    find_fuzzy_candidates,
    build_adjacency_map,
)
import pandas as pd

PLACE = "טבריה"

print("Loading cached LAMAS and OSM (no refresh)...")
LAMAS_df = load_or_fetch_LAMAS()
osm_gdf = load_or_fetch_osm(PLACE)

print('\nLAMAS columns:', LAMAS_df.columns.tolist())
print('LAMAS sample (head):')
print(LAMAS_df.head(5))

print('\nOSM columns:', osm_gdf.columns.tolist())
print('OSM sample (head):')
# Ensure city exists on osm_gdf (pipeline usually populates it)
if 'city' not in osm_gdf.columns:
    osm_city_label = PLACE.split(',')[0].strip()
    osm_gdf['city'] = osm_city_label
print(osm_gdf[['osm_id','osm_name','city']].head(10))

# Normalize
print('\nApplying normalization to name columns (sample).')
# Normalize LAMAS columns if they are present in Hebrew/raw format
if 'LAMAS_name' not in LAMAS_df.columns:
    mapping = {}
    if '_id' in LAMAS_df.columns:
        mapping['_id'] = 'LAMAS_id'
    if 'שם_רחוב' in LAMAS_df.columns:
        mapping['שם_רחוב'] = 'LAMAS_name'
    if 'שם_ישוב' in LAMAS_df.columns:
        mapping['שם_ישוב'] = 'city'
    if mapping:
        LAMAS_df = LAMAS_df.rename(columns=mapping)

if 'LAMAS_name' in LAMAS_df.columns:
    LAMAS_df['normalized_name'] = LAMAS_df['LAMAS_name'].apply(normalize_street_name)
else:
    print('No LAMAS_name column found')

if 'osm_name' in osm_gdf.columns:
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
else:
    print('No osm_name column found')

print('\nLAMAS normalized sample:')
print(LAMAS_df[['LAMAS_id','LAMAS_name','normalized_name']].head(10))
print('\nOSM normalized sample:')
print(osm_gdf[['osm_id','osm_name','normalized_name']].head(10))

# How many LAMAS rows per city (few cities)
print('\nTop 10 cities in LAMAS data:')
print(LAMAS_df['city'].value_counts().head(10))

# How many OSM segments per city
print('\nTop 10 cities in OSM data:')
print(osm_gdf['city'].value_counts().head(10))

# Filter LAMAS to the OSM city label
osm_city_label = osm_gdf['city'].iloc[0]
print(f"\nFiltering LAMAS to city label used by OSM: '{osm_city_label}'")
LAMAS_subset = LAMAS_df[LAMAS_df['city'] == osm_city_label]
print('LAMAS in that city:', len(LAMAS_subset))
print(LAMAS_subset[['LAMAS_id','LAMAS_name']].head(10))

# Run fuzzy candidates on a small sample of OSM rows (first 20) to inspect the results
print('\nRunning fuzzy candidate generation for first 20 OSM streets (in-city filter applied)')
mini_osm = osm_gdf.head(20).copy()
# Ensure city exists
mini_osm['city'] = mini_osm['city'].fillna(osm_city_label)

candidates_df = find_fuzzy_candidates(mini_osm, LAMAS_subset if not LAMAS_subset.empty else LAMAS_df)

print('\nCandidates status counts:')
print(candidates_df['status'].value_counts(dropna=False))

print('\nSample NEEDS_AI rows:')
print(candidates_df[candidates_df['status']=='NEEDS_AI'].head(10))

print('\nSample CONFIDENT rows:')
print(candidates_df[candidates_df['status']=='CONFIDENT'].head(10))

print('\nSample MISSING rows:')
print(candidates_df[candidates_df['status']=='MISSING'].head(10))

# Show top candidate names for first few OSM entries for manual inspection
for _, row in candidates_df.head(10).iterrows():
    print('\nOSM:', row['osm_id'], '-', row.get('all_candidates'))

print('\nDone diagnostics.')
