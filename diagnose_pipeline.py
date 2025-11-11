"""
Diagnostic script for pipeline matching failures.
Loads cached LAMS/OSM, shows schemas, normalization, and runs fuzzy candidate generation
prints counts and examples so we can see why final mapping was empty.
"""
from pipeline import (
    load_or_fetch_lams,
    load_or_fetch_osm,
    normalize_street_name,
    find_fuzzy_candidates,
    build_adjacency_map,
)
import pandas as pd

PLACE = "בית שאן, Israel"

print("Loading cached LAMS and OSM (no refresh)...")
lams_df = load_or_fetch_lams()
osm_gdf = load_or_fetch_osm(PLACE)

print('\nLAMS columns:', lams_df.columns.tolist())
print('LAMS sample (head):')
print(lams_df.head(5))

print('\nOSM columns:', osm_gdf.columns.tolist())
print('OSM sample (head):')
# Ensure city exists on osm_gdf (pipeline usually populates it)
if 'city' not in osm_gdf.columns:
    osm_city_label = PLACE.split(',')[0].strip()
    osm_gdf['city'] = osm_city_label
print(osm_gdf[['osm_id','osm_name','city']].head(10))

# Normalize
print('\nApplying normalization to name columns (sample).')
# Normalize LAMS columns if they are present in Hebrew/raw format
if 'lams_name' not in lams_df.columns:
    mapping = {}
    if '_id' in lams_df.columns:
        mapping['_id'] = 'lams_id'
    if 'שם_רחוב' in lams_df.columns:
        mapping['שם_רחוב'] = 'lams_name'
    if 'שם_ישוב' in lams_df.columns:
        mapping['שם_ישוב'] = 'city'
    if mapping:
        lams_df = lams_df.rename(columns=mapping)

if 'lams_name' in lams_df.columns:
    lams_df['normalized_name'] = lams_df['lams_name'].apply(normalize_street_name)
else:
    print('No lams_name column found')

if 'osm_name' in osm_gdf.columns:
    osm_gdf['normalized_name'] = osm_gdf['osm_name'].apply(normalize_street_name)
else:
    print('No osm_name column found')

print('\nLAMS normalized sample:')
print(lams_df[['lams_id','lams_name','normalized_name']].head(10))
print('\nOSM normalized sample:')
print(osm_gdf[['osm_id','osm_name','normalized_name']].head(10))

# How many LAMS rows per city (few cities)
print('\nTop 10 cities in LAMS data:')
print(lams_df['city'].value_counts().head(10))

# How many OSM segments per city
print('\nTop 10 cities in OSM data:')
print(osm_gdf['city'].value_counts().head(10))

# Filter LAMS to the OSM city label
osm_city_label = osm_gdf['city'].iloc[0]
print(f"\nFiltering LAMS to city label used by OSM: '{osm_city_label}'")
lams_subset = lams_df[lams_df['city'] == osm_city_label]
print('LAMS in that city:', len(lams_subset))
print(lams_subset[['lams_id','lams_name']].head(10))

# Run fuzzy candidates on a small sample of OSM rows (first 20) to inspect the results
print('\nRunning fuzzy candidate generation for first 20 OSM streets (in-city filter applied)')
mini_osm = osm_gdf.head(20).copy()
# Ensure city exists
mini_osm['city'] = mini_osm['city'].fillna(osm_city_label)

candidates_df = find_fuzzy_candidates(mini_osm, lams_subset if not lams_subset.empty else lams_df)

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
