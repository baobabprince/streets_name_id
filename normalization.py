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

import pandas as pd
from rapidfuzz import fuzz

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
    """
    unique_osm_streets = osm_gdf[["normalized_name"]].drop_duplicates().dropna()

    aliases_by_id = build_lamas_aliases(lamas_df)

    id_to_norm_names: Dict[str, List[str]] = {}
    id_to_display: Dict[str, str] = {}
    for _, row in lamas_df.iterrows():
        lid = str(row["LAMAS_id"])
        nname = row.get("normalized_name")
        if pd.isna(nname):
            continue
        id_to_norm_names.setdefault(lid, [])
        if nname not in id_to_norm_names[lid]:
            id_to_norm_names[lid].append(nname)
        if lid not in id_to_display and pd.notna(row.get("LAMAS_name")):
            id_to_display[lid] = str(row["LAMAS_name"])

    logger.info(
        "Processing %s unique OSM street names against %s LAMAS ids (%s name rows)...",
        len(unique_osm_streets),
        len(id_to_norm_names),
        len(lamas_df),
    )

    street_name_results = {}

    for _, osm_row in unique_osm_streets.iterrows():
        osm_name = osm_row["normalized_name"]

        matches = []
        for lid, norm_names in id_to_norm_names.items():
            best_for_id = 0.0
            best_name_for_id = id_to_display.get(
                lid, norm_names[0] if norm_names else ""
            )
            for lamas_name in norm_names:
                score_ratio = fuzz.ratio(osm_name, lamas_name)
                score_token_sort = fuzz.token_sort_ratio(osm_name, lamas_name)
                score_token_set = fuzz.token_set_ratio(osm_name, lamas_name)
                score = (
                    (score_ratio * 0.3)
                    + (score_token_sort * 0.3)
                    + (score_token_set * 0.4)
                )
                if score > best_for_id:
                    best_for_id = score
            if best_for_id >= needs_ai_threshold:
                aliases = aliases_by_id.get(lid, [best_name_for_id])
                matches.append(
                    {
                        "score": best_for_id,
                        "lamas_id": lid,
                        "lamas_name": best_name_for_id,
                        "aliases": aliases,
                    }
                )

        matches.sort(key=lambda x: x["score"], reverse=True)

        def _fmt_candidates(ms):
            lines = []
            for m in ms[:8]:
                alias_str = " | ".join(m.get("aliases") or [m["lamas_name"]])
                lines.append(
                    f"ID: {m['lamas_id']}, Names: [{alias_str}], Score: {m['score']:.1f}"
                )
            return "\n".join(lines)

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
    for _, osm_row in osm_gdf.iterrows():
        osm_id = osm_row["osm_id"]
        osm_name = osm_row["normalized_name"]

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
