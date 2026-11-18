"""Geographic and topology evidence for street-match candidates."""

from __future__ import annotations

from collections import defaultdict

import pandas as pd
from shapely.ops import unary_union


REFERENCE_DISTANCE_METERS = 250.0
EXACT_NAME_SCORE = 100.0


def validate_candidate_geography(
    osm_gdf: pd.DataFrame,
    candidates_df: pd.DataFrame,
    adjacency_map: dict,
) -> pd.DataFrame:
    """Compare candidates with exact-name OSM references for the same LAMAS ID.

    LAMAS records do not include geometry, so this is a conservative OSM-only
    cross-check. It reports evidence without treating missing references as errors.
    """
    result = candidates_df.copy()
    result["geographic_validation"] = "NO_EXACT_NAME_REFERENCE"
    result["geographic_distance_m"] = pd.NA

    if "geometry" not in osm_gdf.columns or osm_gdf.crs is None:
        result["geographic_validation"] = "CRS_UNAVAILABLE"
        return result

    exact_references: dict[str, set[str]] = defaultdict(set)
    for row in result.itertuples(index=False):
        if (
            row.status == "CONFIDENT"
            and float(row.best_score) == EXACT_NAME_SCORE
            and pd.notna(row.best_LAMAS_id)
        ):
            exact_references[str(row.best_LAMAS_id)].add(str(row.osm_id))

    try:
        projected = osm_gdf[["osm_id", "geometry"]].to_crs(epsg=2039)
    except (ValueError, TypeError):
        result["geographic_validation"] = "CRS_UNAVAILABLE"
        return result

    geometry_by_id = {
        str(osm_id): geometry
        for osm_id, geometry in zip(projected["osm_id"], projected.geometry)
    }
    reference_geometry_by_lamas = {
        lamas_id: unary_union(
            [
                geometry_by_id[osm_id]
                for osm_id in osm_ids
                if osm_id in geometry_by_id
                and not geometry_by_id[osm_id].is_empty
            ]
        )
        for lamas_id, osm_ids in exact_references.items()
        if any(
            osm_id in geometry_by_id and not geometry_by_id[osm_id].is_empty
            for osm_id in osm_ids
        )
    }
    adjacent_by_id = {
        str(osm_id): {str(adjacent_id) for adjacent_id in adjacent_ids}
        for osm_id, adjacent_ids in adjacency_map.items()
    }

    updated_details = []
    validations = []
    distances = []

    for row in result.itertuples(index=False):
        osm_id = str(row.osm_id)
        geometry = geometry_by_id.get(osm_id)
        if geometry is None or geometry.is_empty:
            row_validation = "NO_GEOMETRY"
            row_distance = pd.NA
        else:
            row_validation = "NO_EXACT_NAME_REFERENCE"
            row_distance = pd.NA

        details = []
        candidate_records = row.candidate_details
        if not isinstance(candidate_records, list):
            candidate_records = []
        for candidate in candidate_records:
            candidate = dict(candidate)
            lamas_id = str(candidate["lamas_id"])
            references = exact_references.get(lamas_id, set())
            is_exact_anchor = (
                lamas_id == str(row.best_LAMAS_id)
                and row.status == "CONFIDENT"
                and float(row.best_score) == EXACT_NAME_SCORE
            )
            if geometry is None or geometry.is_empty:
                validation = "NO_GEOMETRY"
                distance = pd.NA
            elif is_exact_anchor:
                validation = "EXACT_NAME_ANCHOR"
                distance = 0.0
            else:
                reference_geometry = reference_geometry_by_lamas.get(lamas_id)
                if reference_geometry is None:
                    validation = "NO_EXACT_NAME_REFERENCE"
                    distance = pd.NA
                else:
                    distance = geometry.distance(reference_geometry)
                    connected = bool(
                        adjacent_by_id.get(osm_id, set()) & references
                    )
                    if connected:
                        validation = "TOPOLOGY_CONNECTED"
                    elif distance <= REFERENCE_DISTANCE_METERS:
                        validation = "NEAR_EXACT_REFERENCE"
                    else:
                        validation = "DISTANT_FROM_EXACT_REFERENCE"

            candidate["geographic_validation"] = validation
            candidate["geographic_distance_m"] = (
                round(float(distance), 1) if pd.notna(distance) else None
            )
            details.append(candidate)

        if details:
            row_validation = details[0]["geographic_validation"]
            row_distance = details[0]["geographic_distance_m"]
        updated_details.append(details)
        validations.append(row_validation)
        distances.append(row_distance)

    result["candidate_details"] = updated_details
    result["geographic_validation"] = validations
    result["geographic_distance_m"] = distances
    return result
