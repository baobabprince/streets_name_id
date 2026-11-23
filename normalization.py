"""
Street Name Normalization and Fuzzy Matching Module.

Provides:
1. Street-name normalization (Hebrew abbreviations with or without geresh,
   Hebrew/Arabic punctuation, niqqud, Arabic letter/digit folding, whitespace)
2. Grouping all LAMAS display names / synonyms by LAMAS_id
3. Single-threshold fuzzy matching (>= 90% confident; everything below goes to AI),
   boosted by a "core name" comparison that strips street-type prefixes so
   "סמטת אורן" matches "סמ אורן" and "משעול סייפן" matches "סייפן".
"""

import logging
import re
import unicodedata
from typing import Dict, List

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from settlement_names import canonical_key

logger = logging.getLogger(__name__)

DEFAULT_CONFIDENT_THRESHOLD = 90.0

# Arabic letter folding (mirrors settlement_names._ARABIC_LETTERS) so Arabic
# script OSM names compare consistently against LAMAS spellings of the same
# street.  Kept local to avoid coupling the two modules' internals.
_ARABIC_LETTERS = str.maketrans({
    "أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا",
    "ى": "ي", "ؤ": "و", "ئ": "ي", "ة": "ه",
    "\u0640": "",  # tatweel
})

# Arabic-Indic (٠-٩) and Persian (۰-۹) digits fold to ASCII digits so that
# "١٠٠" and "100" are the same street number.
_ARABIC_DIGITS = str.maketrans({
    chr(0x0660 + i): str(i) for i in range(10)
} | {
    chr(0x06F0 + i): str(i) for i in range(10)
})

# Quote-like characters removed entirely: ASCII ' " ` , Hebrew geresh ׳ and
# gershayim ״, curly quotes, modifier apostrophes.  The LAMAS and OSM sources
# mix all of these for the same name ("הב"ח" / "הב״ח", "ר' אליעזר" / "ר׳ אליעזר").
_PUNCTUATION_RE = re.compile(
    r"[.,;:!?'\"`\u05f3\u05f4\u2018\u2019\u201c\u201d\u02bc{}\[\]]"
)

# Every dash variant, including the Hebrew maqaf (א־סלאם), becomes a space.
_HYPHENS_RE = re.compile(r"[\u05be\u2010\u2011\u2012\u2013\u2014\u2015-]")

# Leading generic street-type prefixes, stripped (repeatedly) for core-name
# comparison.  "שכונת" is included because LAMAS records neighborhoods that
# carry a street-like name ("שכ נתיב האבות").
STREET_PREFIX_RE = re.compile(
    r"^(?:רחוב|רח|שדרות|שדרת|שד|דרך|דר|סמטת|סמטה|סמ|משעול|כיכר|ככר|שביל|"
    r"מעלה|מעלות|טיילת|גשר|רחבת|נתיב|מדרגות|צומת|מחלף|מעבר(?:\s+לרחוב|\s+ל)?|"
    r"חניון|פארק|גן|שכונת)\s+"
)

# Abbreviations applied AFTER punctuation removal, so geresh/period variants
# ("סמ'", "סמ.", "שכ'") are covered by the bare-word patterns below.
# "ר" and "דר" are deliberately NOT expanded: they may mean רבי/ד"ר rather
# than רחוב/דרך (e.g. "ר' אליעזר בן יעקב").
_ABBREVIATIONS = [
    (r"\bרח\b", "רחוב"),
    (r"\bשד\b", "שדרות"),
    (r"\bסמט\s+", "סמטת "),
    (r"\bסמ\s+", "סמטת "),
    (r"\bשכ\s+", "שכונת "),
    (r"\bכי\s+", "כיכר "),
]


def strip_street_prefix(name: str) -> str:
    """Strip leading generic Hebrew street-type prefixes for core-name similarity.

    Repeatedly removes a leading street-type word ("דרך נתיב האבות" ->
    "נתיב האבות" -> "האבות").  Callers should run :func:`normalize_street_name`
    first so bare abbreviations ("סמ אורן") are already expanded.
    """
    text = (name or "").strip()
    prev = None
    while prev != text:
        prev = text
        text = STREET_PREFIX_RE.sub("", text).strip()
    return text


def normalize_street_name(name):
    """Normalize a street name (any language) for matching.

    Handles: niqqud/combining-mark removal, Arabic letter and digit folding,
    Hebrew abbreviation expansion with or without geresh, Hebrew geresh and
    gershayim variants, the Hebrew maqaf, and whitespace/punctuation noise.
    """
    if pd.isna(name) or name is None:
        return None

    name = str(name).strip()
    if not name:
        return None

    # 1. Unicode hygiene: drop combining marks (niqqud, Arabic harakat) and
    #    fold Arabic letters/digits so both script variants compare equal.
    name = "".join(
        char
        for char in unicodedata.normalize("NFKD", name)
        if not unicodedata.combining(char)
    )
    name = name.translate(_ARABIC_LETTERS).translate(_ARABIC_DIGITS)

    typo_fixes = [
        (r"\bסמתט\b", "סמטת"),
    ]
    for pattern, replacement in typo_fixes:
        name = re.sub(pattern, replacement, name)

    # 2. Dashes become spaces; quote-like punctuation is removed.  This must
    #    run before abbreviation expansion so "רח'"/"שד." collapse to bare
    #    words first ("אלג׳בל" and "אלג'בל" both become "אלגבל").
    name = _HYPHENS_RE.sub(" ", name)
    name = _PUNCTUATION_RE.sub("", name)

    # 3. Abbreviation expansion (see _ABBREVIATIONS above).
    for pattern, replacement in _ABBREVIATIONS:
        name = re.sub(pattern, replacement, name)

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


def _blend_score_matrix(left: List[str], right: List[str]) -> np.ndarray:
    """The project's standard fuzzy blend over two name lists (cdist)."""
    matrix = (
        process.cdist(left, right, scorer=fuzz.ratio, dtype=np.float64, workers=1)
        * 0.3
    )
    matrix += (
        process.cdist(
            left, right, scorer=fuzz.token_sort_ratio, dtype=np.float64, workers=1
        )
        * 0.3
    )
    matrix += (
        process.cdist(
            left, right, scorer=fuzz.token_set_ratio, dtype=np.float64, workers=1
        )
        * 0.4
    )
    return matrix


def find_fuzzy_candidates(
    osm_gdf: pd.DataFrame,
    lamas_df: pd.DataFrame,
    confident_threshold: float = DEFAULT_CONFIDENT_THRESHOLD,
) -> pd.DataFrame:
    """Match OSM street names against city LAMAS streets with one threshold.

    Streets scoring >= ``confident_threshold`` (default 90) are marked
    ``CONFIDENT``. Every named OSM street below the threshold is marked
    ``NEEDS_AI`` so the whole city's remainder can be resolved by AI.

    The score is ``max(full_name_blend, core_name_blend)`` where the core
    blend compares names after stripping street-type prefixes.  Safeguards:

    * Among tied top scores the candidate with the strongest full-name blend
      wins, so an exact match always beats a core-level match.
    * A top-score tie that only exists after prefix stripping and spans
      several LAMAS ids ("שדרות הרצל" vs "רחוב הרצל") is demoted to NEEDS_AI
      instead of being approved arbitrarily.
    * LAMAS names identical to the settlement's own name never participate in
      the core boost, so a road named "דרך <יישוב>" cannot confidently absorb
      the settlement pseudo-record.
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

    # Settlement own-name records are ineligible for core-level matching.
    settlement_key = ""
    if "city" in osm_gdf.columns and len(osm_gdf):
        settlement_key = canonical_key(osm_gdf["city"].iloc[0])

    osm_core_names = [strip_street_prefix(name) for name in osm_names]
    lamas_core_names: List[str] = []
    for name in lamas_name_candidates:
        if settlement_key and canonical_key(name) == settlement_key:
            lamas_core_names.append("")
        else:
            lamas_core_names.append(strip_street_prefix(name))

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
        batch_cores = osm_core_names[batch_start : batch_start + batch_size]
        score_matrix = _blend_score_matrix(batch_names, lamas_name_candidates)
        if any(batch_cores):
            core_matrix = _blend_score_matrix(batch_cores, lamas_core_names)
        else:
            core_matrix = np.zeros_like(score_matrix)
        final_matrix = np.maximum(score_matrix, core_matrix)

        for row_index, osm_name in enumerate(batch_names):
            row_final = final_matrix[row_index]
            row_full = score_matrix[row_index]
            best_score = float(row_final.max())
            # Ties are compared with a tiny tolerance against float noise.
            tied = np.flatnonzero(row_final >= best_score - 1e-9)
            # Prefer the strongest full-name candidate among tied scores, so
            # "ירושלים" (exact 100) beats "שדרות ירושלים" (core 100).
            tied_full_best = float(row_full[tied].max())
            finalists = tied[row_full[tied] >= tied_full_best - 1e-9]

            best_lid = min(
                (
                    lid
                    for idx in finalists
                    for lid in unique_name_to_ids[lamas_name_candidates[idx]]
                ),
                key=id_order.__getitem__,
            )

            status = "CONFIDENT" if best_score >= confident_threshold else "NEEDS_AI"
            tied_ids = {
                lid
                for idx in tied
                for lid in unique_name_to_ids[lamas_name_candidates[idx]]
            }
            if (
                status == "CONFIDENT"
                and len(tied_ids) > 1
                and tied_full_best < confident_threshold
            ):
                # The tie exists only after prefix stripping and spans several
                # LAMAS ids — ambiguous; defer to AI/review instead of guessing.
                status = "NEEDS_AI"
                logger.debug(
                    "Ambiguous core-level match for '%s' across %d LAMAS ids; "
                    "deferring to AI.",
                    osm_name,
                    len(tied_ids),
                )

            best_lamas_name = display_name_by_id.get(
                best_lid, lamas_name_candidates[finalists[0]]
            )
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
    name_statuses = [entry["status"] for entry in street_name_results.values()]
    logger.info(
        "Fuzzy matching complete: CONFIDENT=%s, NEEDS_AI=%s, MISSING=%s segments "
        "(unique street names: CONFIDENT=%d, NEEDS_AI=%d)",
        status_counts.get("CONFIDENT", 0),
        status_counts.get("NEEDS_AI", 0),
        status_counts.get("MISSING", 0),
        name_statuses.count("CONFIDENT"),
        name_statuses.count("NEEDS_AI"),
    )
    return candidates_df
