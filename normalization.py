"""
Street Name Normalization and Fuzzy Matching Module.

Provides:
1. Hebrew street-name normalization (abbreviations, punctuation, whitespace)
2. Grouping all LAMAS display names / synonyms by LAMAS_id
3. Single-threshold fuzzy matching (>= 90% confident; everything below goes to AI)
"""

import logging
import re
from typing import Dict, List

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

DEFAULT_CONFIDENT_THRESHOLD = 90.0


def normalize_street_name(name):
    """Normalize a Hebrew street name (abbreviations, punctuation, whitespace)."""
    if pd.isna(name) or name is None:
        return None

    name = str(name).strip()
    if not name:
        return None

    typo_fixes = [
        (r"\bסמתט\b", "סמטת"),
    ]
    for pattern, replacement in typo_fixes:
        name = re.sub(pattern, replacement, name)

    abbreviations = [
        (r"\bרח\'", "רחוב"),
        (r"\bרח\.", "רחוב"),
        (r"\bשד\'", "שדרות"),
        (r"\bשד\.", "שדרות"),
        (r"\bשד\b", "שדרות"),
        (r"\bכי\'", "כיכר"),
        (r"\bכי\.", "כיכר"),
        (r"\bדר\'", "דרך"),
        (r"\bדר\.", "דרך"),
        (r"\bסמ\'", "סמטה"),
        (r"\bסמ\.", "סמטה"),
        (r"\bסמט\'", "סמטת"),
        (r"\bסמט\.", "סמטת"),
    ]
    for pattern, replacement in abbreviations:
        name = re.sub(pattern, replacement, name)

    name = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\-]", " ", name)
    name = re.sub(r"[.,;:!?\'\"{}[\]]", "", name)
    name = re.sub(r"[()]", " ", name)
    name = re.sub(r"\s+", " ", name)
    name = name.strip()

    return name if name else None


def build_lamas_aliases(lamas_df: pd.DataFrame) -> Dict[str, List[str]]:
    """Group all LAMAS street names (including synonyms) by LAMAS_id."""
    if lamas_df is None or lamas_df.empty:
        return {}
    aliases: Dict[str, List[str]] = {}
    for lamas_id, group in lamas_df.groupby("LAMAS_id"):
        names = sorted(
            {
                str(n).strip()
                for n in group["LAMAS_name"].dropna().tolist()
                if str(n).strip()
            }
        )
        if names:
            aliases[str(lamas_id)] = names
    return aliases


def find_fuzzy_candidates(
    osm_gdf: pd.DataFrame,
    lamas_df: pd.DataFrame,
    confident_threshold: float = DEFAULT_CONFIDENT_THRESHOLD,
) -> pd.DataFrame:
    """Match OSM street names against city LAMAS streets with one threshold.

    Streets scoring >= ``confident_threshold`` (default 90) are marked
    ``CONFIDENT``. Every named OSM street below the threshold is marked
    ``NEEDS_AI`` so the whole city's remainder can be resolved by AI.
    """
    empty_result = lambda: pd.DataFrame(
        {
            "osm_id": osm_gdf["osm_id"].tolist(),
            "status": ["MISSING"] * len(osm_gdf),
            "best_score": [0.0] * len(osm_gdf),
            "best_LAMAS_id": [None] * len(osm_gdf),
            "best_LAMAS_name": [None] * len(osm_gdf),
        }
    )

    unique_osm_streets = osm_gdf[["normalized_name"]].drop_duplicates().dropna()
    if unique_osm_streets.empty:
        return empty_result()

    normalized_lamas_df = lamas_df.dropna(
        subset=["LAMAS_id", "normalized_name"]
    ).copy()
    if normalized_lamas_df.empty:
        return empty_result()

    normalized_lamas_df["normalized_name"] = normalized_lamas_df[
        "normalized_name"
    ].astype(str)
    normalized_lamas_df["LAMAS_id"] = normalized_lamas_df["LAMAS_id"].astype(str)

    unique_name_to_ids: Dict[str, List[str]] = {}
    display_name_by_id: Dict[str, str] = {}
    id_order: Dict[str, int] = {}
    for lamas_id, normalized_name, display_name in normalized_lamas_df[
        ["LAMAS_id", "normalized_name", "LAMAS_name"]
    ].itertuples(index=False, name=None):
        lid = str(lamas_id)
        name = str(normalized_name)
        ids = unique_name_to_ids.setdefault(name, [])
        if lid not in ids:
            ids.append(lid)
        if lid not in display_name_by_id and pd.notna(display_name):
            display_name_by_id[lid] = str(display_name)
        id_order.setdefault(lid, len(id_order))

    lamas_name_candidates = list(unique_name_to_ids)
    osm_names = unique_osm_streets["normalized_name"].astype(str).tolist()

    logger.info(
        "Fuzzy matching %s unique OSM street names against %s LAMAS names (threshold=%.1f)...",
        len(osm_names),
        len(lamas_name_candidates),
        confident_threshold,
    )

    street_name_results: Dict[str, dict] = {}
    batch_size = 128
    for batch_start in range(0, len(osm_names), batch_size):
        batch_names = osm_names[batch_start : batch_start + batch_size]
        score_matrix = process.cdist(
            batch_names,
            lamas_name_candidates,
            scorer=fuzz.ratio,
            dtype=np.float64,
            workers=1,
        )
        score_matrix *= 0.3
        score_matrix += (
            process.cdist(
                batch_names,
                lamas_name_candidates,
                scorer=fuzz.token_sort_ratio,
                dtype=np.float64,
                workers=1,
            )
            * 0.3
        )
        score_matrix += (
            process.cdist(
                batch_names,
                lamas_name_candidates,
                scorer=fuzz.token_set_ratio,
                dtype=np.float64,
                workers=1,
            )
            * 0.4
        )

        for row_index, osm_name in enumerate(batch_names):
            row_scores = score_matrix[row_index]
            best_score = float(row_scores.max())
            best_indices = np.flatnonzero(row_scores == best_score)
            best_lid = min(
                (
                    lid
                    for idx in best_indices
                    for lid in unique_name_to_ids[lamas_name_candidates[idx]]
                ),
                key=id_order.__getitem__,
            )
            best_lamas_name = display_name_by_id.get(
                best_lid, lamas_name_candidates[best_indices[0]]
            )
            status = "CONFIDENT" if best_score >= confident_threshold else "NEEDS_AI"
            street_name_results[osm_name] = {
                "status": status,
                "best_score": best_score,
                "best_LAMAS_id": best_lid,
                "best_LAMAS_name": best_lamas_name,
            }

    results = []
    for osm_id, osm_name in zip(osm_gdf["osm_id"], osm_gdf["normalized_name"]):
        if pd.isna(osm_name) or osm_name not in street_name_results:
            results.append(
                {
                    "osm_id": osm_id,
                    "status": "MISSING",
                    "best_score": 0.0,
                    "best_LAMAS_id": None,
                    "best_LAMAS_name": None,
                }
            )
        else:
            result = street_name_results[osm_name].copy()
            result["osm_id"] = osm_id
            results.append(result)

    candidates_df = pd.DataFrame(results)
    status_counts = candidates_df["status"].value_counts()
    logger.info(
        "Fuzzy matching complete: CONFIDENT=%s, NEEDS_AI=%s, MISSING=%s",
        status_counts.get("CONFIDENT", 0),
        status_counts.get("NEEDS_AI", 0),
        status_counts.get("MISSING", 0),
    )
    return candidates_df
