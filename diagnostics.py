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
        # The old "3-digit code = street" heuristic was wrong: official street
        # codes in the data.gov.il synonyms resource can be 1-5 digits, and
        # 4/5-digit codes also appear for synonyms.  Trust the source status.
        if "is_official" in lamas_in_city_df.columns:
            official_column = lamas_in_city_df["is_official"]
            if official_column.dtype == bool:
                official_mask = official_column.fillna(False)
            else:
                official_mask = (
                    official_column.astype("string").str.casefold()
                    .isin(["true", "1", "yes"])
                )
        elif "street_name_status" in lamas_in_city_df.columns:
            official_mask = (
                lamas_in_city_df["street_name_status"]
                .astype("string").str.casefold().eq("official")
            )
        else:
            official_mask = pd.Series(True, index=lamas_in_city_df.index)
        lamas_streets_only = lamas_in_city_df.loc[official_mask].copy()
        if lamas_streets_only.empty:
            # Legacy caches without status metadata: count every unique id.
            lamas_streets_only = lamas_in_city_df.copy()

        # Prefer the globally unique (city_code:official_code) key when the
        # pipeline wrote it; otherwise fall back to the city-local LAMAS_id.
        if (
            "LAMAS_key" in lamas_streets_only.columns
            and "final_LAMAS_key" in diagnostic_df_full.columns
        ):
            lamas_key_col, diag_key_col = "LAMAS_key", "final_LAMAS_key"
        else:
            lamas_key_col, diag_key_col = "LAMAS_id", "final_LAMAS_id"
        total_lamas_streets = lamas_streets_only[lamas_key_col].nunique()

        matched_lamas_ids = set(
            diagnostic_df_full[diag_key_col]
            .dropna()
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
        )
        is_matched = (
            lamas_streets_only[lamas_key_col]
            .astype(str)
            .isin(matched_lamas_ids)
        )
        unmatched_lamas_df = lamas_streets_only[~is_matched]

        unmatched_lamas_street_names = sorted(
            unmatched_lamas_df.groupby(lamas_key_col)["LAMAS_name"].first().tolist()
        )
        unmatched_lamas_count = unmatched_lamas_df[lamas_key_col].nunique()
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
