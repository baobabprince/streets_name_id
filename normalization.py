"""
Street Name Normalization and Fuzzy Matching Module

This module provides functions for:
1. Normalizing Hebrew street names (expanding abbreviations, removing punctuation)
2. Finding fuzzy match candidates between OSM and LAMAS street data
3. Grouping all synonym / optional names per LAMAS_id for matching and AI context
"""

import logging
import re
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


def normalize_street_name(name):
    """
    Normalize a Hebrew street name by:
    1. Expanding common abbreviations (רח' -> רחוב, שד' -> שדרות, etc.)
    2. Removing punctuation and extra whitespace
    3. Standardizing format

    Args:
        name: Street name string (can be None or NaN)

    Returns:
        Normalized street name string, or None if input is invalid
    """
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
    name = re.sub(r"[.,;:!?\'\"(){}[\]]", "", name)
    name = re.sub(r"\s+", " ", name)
    name = name.strip()

    return name if name else None


def build_lamas_aliases(lamas_df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Group all LAMAS street names (including synonyms) by LAMAS_id.

    Returns:
        {lamas_id: [name1, name2, ...]} sorted unique display names.
    """
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


def format_city_lexicon(aliases_by_id: Dict[str, List[str]], max_entries: int = 400) -> str:
    """
    Compact text lexicon of all city LAMAS ids and their names/synonyms for AI context.
    Truncates with a note if the city is very large.
    """
    if not aliases_by_id:
        return '(אין נתוני למ"ס לעיר זו)'
    items = sorted(aliases_by_id.items(), key=lambda kv: kv[0])
    truncated = len(items) > max_entries
    if truncated:
        items = items[:max_entries]
    lines = [f"ID {lid}: {', '.join(names)}" for lid, names in items]
    text = "\n".join(lines)
    if truncated:
        text += f'\n… (קוצץ; סה"כ {len(aliases_by_id)} רחובות בעיר)'
    return text


def find_fuzzy_candidates(
    osm_gdf,
    lamas_df,
    confident_threshold: int = 70,
    needs_ai_threshold: int = 50,
):
    """
    Find fuzzy match candidates between OSM and LAMAS street data.

    Matching is done per LAMAS_id: score is the max over all synonym names
    of that ID. Candidate formatting includes every known alias for the ID.

    RapidFuzz computes the three score matrices in native code; Python then
    visits only name pairs that pass the candidate threshold.
    """
    unique_osm_streets = osm_gdf[["normalized_name"]].drop_duplicates().dropna()
    if unique_osm_streets.empty:
        return pd.DataFrame(
            {
                "osm_id": osm_gdf["osm_id"].tolist(),
                "status": ["MISSING"] * len(osm_gdf),
                "best_score": [0.0] * len(osm_gdf),
                "best_LAMAS_id": [None] * len(osm_gdf),
                "best_LAMAS_name": [None] * len(osm_gdf),
                "all_candidates": [None] * len(osm_gdf),
            }
        )

    aliases_by_id = build_lamas_aliases(lamas_df)
    normalized_lamas_df = lamas_df.dropna(
        subset=["LAMAS_id", "normalized_name"]
    ).copy()

    if normalized_lamas_df.empty:
        return pd.DataFrame(
            {
                "osm_id": osm_gdf["osm_id"].tolist(),
                "status": ["MISSING"] * len(osm_gdf),
                "best_score": [0.0] * len(osm_gdf),
                "best_LAMAS_id": [None] * len(osm_gdf),
                "best_LAMAS_name": [None] * len(osm_gdf),
                "all_candidates": [None] * len(osm_gdf),
            }
        )

    normalized_lamas_df["normalized_name"] = normalized_lamas_df["normalized_name"].astype(str)
    normalized_lamas_df["LAMAS_id"] = normalized_lamas_df["LAMAS_id"].astype(str)

    unique_name_to_ids: Dict[str, List[str]] = {}
    display_name_by_id: Dict[str, str] = {}
    id_order: Dict[str, int] = {}
    lamas_columns = normalized_lamas_df[
        ["LAMAS_id", "normalized_name", "LAMAS_name"]
    ]
    for lamas_id, normalized_name, display_name in lamas_columns.itertuples(
        index=False, name=None
    ):
        lid = str(lamas_id)
        name = str(normalized_name)
        if name not in unique_name_to_ids:
            unique_name_to_ids[name] = []
        if lid not in unique_name_to_ids[name]:
            unique_name_to_ids[name].append(lid)
        if lid not in display_name_by_id and pd.notna(display_name):
            display_name_by_id[lid] = str(display_name)
        id_order.setdefault(lid, len(id_order))

    lamas_name_candidates = list(unique_name_to_ids)
    osm_names = unique_osm_streets["normalized_name"].astype(str).tolist()

    logger.info(
        "Processing %s unique OSM street names against %s LAMAS normalized names (%s LAMAS rows)...",
        len(unique_osm_streets),
        len(lamas_name_candidates),
        len(normalized_lamas_df),
    )

    street_name_results = {}

    def _fmt_candidates(matches):
        lines = []
        for match in matches[:8]:
            alias_str = " | ".join(match.get("aliases") or [match["lamas_name"]])
            lines.append(
                f"ID: {match['lamas_id']}, Names: [{alias_str}], Score: {match['score']:.1f}"
            )
        return "\n".join(lines)

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
        score_matrix += process.cdist(
            batch_names,
            lamas_name_candidates,
            scorer=fuzz.token_sort_ratio,
            dtype=np.float64,
            workers=1,
        ) * 0.3
        score_matrix += process.cdist(
            batch_names,
            lamas_name_candidates,
            scorer=fuzz.token_set_ratio,
            dtype=np.float64,
            workers=1,
        ) * 0.4

        for row_index, osm_name in enumerate(batch_names):
            candidate_matches: Dict[str, dict] = {}
            for name_index in np.flatnonzero(score_matrix[row_index] >= needs_ai_threshold):
                lamas_name = lamas_name_candidates[name_index]
                score = float(score_matrix[row_index, name_index])
                for lid in unique_name_to_ids[lamas_name]:
                    existing = candidate_matches.get(lid)
                    if existing is None or score > existing["score"]:
                        candidate_matches[lid] = {
                            "score": score,
                            "lamas_id": lid,
                            "lamas_name": display_name_by_id.get(lid, lamas_name),
                            "aliases": aliases_by_id.get(
                                lid, [display_name_by_id.get(lid, lamas_name)]
                            ),
                        }

            matches = sorted(
                candidate_matches.values(),
                key=lambda candidate: (
                    -candidate["score"],
                    id_order[candidate["lamas_id"]],
                ),
            )

            if not matches:
                status = "MISSING"
                best_score = 0
                best_lamas_id = None
                best_lamas_name = None
                all_candidates = None
            elif len(matches) == 1 and matches[0]["score"] >= confident_threshold:
                status = "CONFIDENT"
                best_score = matches[0]["score"]
                best_lamas_id = matches[0]["lamas_id"]
                best_lamas_name = matches[0]["lamas_name"]
                all_candidates = _fmt_candidates(matches)
            elif matches[0]["score"] >= confident_threshold and len(matches) > 1:
                if matches[0]["score"] - matches[1]["score"] >= 5:
                    status = "CONFIDENT"
                else:
                    status = "NEEDS_AI"
                best_score = matches[0]["score"]
                best_lamas_id = matches[0]["lamas_id"]
                best_lamas_name = matches[0]["lamas_name"]
                all_candidates = _fmt_candidates(matches)
            else:
                status = "NEEDS_AI"
                best_score = matches[0]["score"]
                best_lamas_id = matches[0]["lamas_id"]
                best_lamas_name = matches[0]["lamas_name"]
                all_candidates = _fmt_candidates(matches)

            street_name_results[osm_name] = {
                "status": status,
                "best_score": best_score,
                "best_LAMAS_id": best_lamas_id,
                "best_LAMAS_name": best_lamas_name,
                "all_candidates": all_candidates,
            }

    results = []
    for osm_id, osm_name in zip(osm_gdf["osm_id"], osm_gdf["normalized_name"]):

        if pd.isna(osm_name) or osm_name not in street_name_results:
            results.append(
                {
                    "osm_id": osm_id,
                    "status": "MISSING",
                    "best_score": 0,
                    "best_LAMAS_id": None,
                    "best_LAMAS_name": None,
                    "all_candidates": None,
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
