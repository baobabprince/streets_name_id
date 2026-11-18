"""
Diagnostic summary statistics for street matching pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_diagnostics(
    lamas_in_city_df: pd.DataFrame,
    diagnostic_df_full: pd.DataFrame,
    osm_gdf: pd.DataFrame,
) -> dict[str, Any]:
    """Calculate diagnostic summary statistics for a completed pipeline run."""
    named_osm_gdf = osm_gdf.dropna(subset=["normalized_name"])
    total_osm_streets = named_osm_gdf["normalized_name"].nunique()

    approved_matches = diagnostic_df_full[
        diagnostic_df_full["final_LAMAS_id"].notna()
    ]
    confident_matches = approved_matches["normalized_name"].nunique()
    ai_proposed_matches = 0
    if "ai_suggested_LAMAS_id" in diagnostic_df_full:
        ai_proposed_matches = diagnostic_df_full[
            diagnostic_df_full["ai_suggested_LAMAS_id"].notna()
        ]["normalized_name"].nunique()
    geographic_review_matches = 0
    if "mapping_status" in diagnostic_df_full:
        geographic_review_matches = diagnostic_df_full[
            diagnostic_df_full["mapping_status"] == "GEOGRAPHIC_REVIEW"
        ]["normalized_name"].nunique()

    total_matched = confident_matches
    unmatched_osm_streets = total_osm_streets - total_matched

    matched_osm_names = set(
        diagnostic_df_full[diagnostic_df_full["final_LAMAS_id"].notna()][
            "normalized_name"
        ].unique()
    )
    all_osm_names = set(named_osm_gdf["normalized_name"].unique())
    unmatched_osm_names = sorted(list(all_osm_names - matched_osm_names))

    unmatched_osm_percentage = (
        (unmatched_osm_streets / total_osm_streets) * 100 if total_osm_streets > 0 else 0
    )

    if not lamas_in_city_df.empty:
        # 3-digit codes = actual streets; 4-digit = neighborhoods etc.
        lamas_streets_only = lamas_in_city_df[
            lamas_in_city_df["LAMAS_id"].astype(str).str.len() == 3
        ]
        total_lamas_streets = lamas_streets_only["LAMAS_id"].nunique()

        matched_lamas_ids = set(
            diagnostic_df_full["final_LAMAS_id"]
            .dropna()
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
        )

        is_matched = lamas_streets_only["LAMAS_id"].astype(str).isin(matched_lamas_ids)
        unmatched_lamas_df = lamas_streets_only[~is_matched]

        unmatched_lamas_street_names = sorted(
            unmatched_lamas_df.groupby("LAMAS_id")["LAMAS_name"].first().tolist()
        )
        unmatched_lamas_count = unmatched_lamas_df["LAMAS_id"].nunique()
        unmatched_lamas_percentage = (
            (unmatched_lamas_count / total_lamas_streets) * 100
            if total_lamas_streets > 0
            else 0
        )
    else:
        total_lamas_streets = 0
        unmatched_lamas_street_names = []
        unmatched_lamas_count = 0
        unmatched_lamas_percentage = 0

    diagnostics = {
        "total_osm_streets": total_osm_streets,
        "total_lamas_streets": total_lamas_streets,
        "confident_matches": confident_matches,
        "ai_proposed_matches": ai_proposed_matches,
        "ai_resolved_matches": ai_proposed_matches,
        "geographic_review_matches": geographic_review_matches,
        "total_matched": total_matched,
        "unmatched_osm_streets": unmatched_osm_streets,
        "unmatched_osm_percentage": f"{unmatched_osm_percentage:.1f}%",
        "unmatched_osm_street_names": unmatched_osm_names,
        "unmatched_lamas_count": unmatched_lamas_count,
        "unmatched_lamas_percentage": f"{unmatched_lamas_percentage:.1f}%",
        "unmatched_lamas_street_names": unmatched_lamas_street_names,
    }
    logger.debug("Diagnostics computed: matched=%s / osm=%s", total_matched, total_osm_streets)
    return diagnostics
