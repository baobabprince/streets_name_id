"""
Street Name Normalization and Fuzzy Matching Module.

Provides:
1. Street-name normalization (Hebrew abbreviations with or without geresh,
   Hebrew/Arabic punctuation, niqqud, Arabic letter/digit folding, whitespace)
2. Grouping all LAMAS display names / synonyms by LAMAS_id
3. Layered fuzzy/core and complete-token matching (default threshold 90), with
   review-only Hebrew phonetic suggestions and numeric/ambiguity safety gates.
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
# A phonetic collision is a retrieval hint, never independent approval evidence.
PHONETIC_SUGGESTION_SCORE = 89.0

_HEBREW_PHONETIC_FOLD = str.maketrans({
    "ך": "ק", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ",
    "כ": "ק", "ח": "ק", "ט": "ת", "א": "ע",
})


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


def hebrew_phonetic_key(name: str) -> str:
    """Return a non-truncated Hebrew consonant key, for suggestions only.

    Unlike English Soundex, preserve word boundaries without truncation, fold
    common Hebrew sound/spelling variants and omit non-initial vav/yod (matres
    lectionis). They can also be consonants: this intentionally lossy key MUST
    NOT establish street identity. Short, numeric and mixed-script names are
    excluded; no transliteration or Arabic phonetic matching is attempted.
    """
    name = normalize_street_name(name)
    if not name:
        return ""
    tokens = strip_street_prefix(name).split()
    keys = []
    for token in tokens:
        if len(token) < 3 or not re.fullmatch(r"[א-ת]+", token):
            return ""
        token = token.translate(_HEBREW_PHONETIC_FOLD)
        key = token[0] + re.sub(r"[וי]", "", token[1:])
        if len(key) < 2:
            return ""
        keys.append(key)
    return " ".join(sorted(keys))


def _token_key(name: str) -> str:
    # Sorting a list, not a set, retains repeated words and requires all words.
    return " ".join(sorted(name.split()))


def _equal_key_matrix(left: List[str], right: List[str], score: float) -> np.ndarray:
    """Exact nonempty key matches without a Python all-pairs comparison."""
    positions = {}
    for index, key in enumerate(right):
        if key:
            positions.setdefault(key, []).append(index)
    matrix = np.zeros((len(left), len(right)), dtype=np.float64)
    for index, key in enumerate(left):
        matches = positions.get(key)
        if matches:
            matrix[index, matches] = score
    return matrix


def _number_key(name: str) -> str:
    return "|".join(sorted(re.findall(r"\d+", name)))


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

    Streets scoring >= ``confident_threshold`` (default 90) and passing the
    safety gates are marked ``CONFIDENT``. All other named OSM streets are
    marked ``NEEDS_AI`` so the city's remainder can be resolved by AI/review.

    Full/core fuzzy blends are augmented by complete-token equality (ignoring
    order) and review-only Hebrew phonetic suggestions. Numeric conflicts are
    excluded from scoring. ``match_method`` explains the winning signal and
    ``match_review_reason`` records safety gates; scores are not probabilities.
    The core blend compares names after stripping street-type prefixes. Safeguards:

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
            "match_method": ["missing"] * len(osm_gdf),
            "match_review_reason": [""] * len(osm_gdf),
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

    lamas_token_keys = [_token_key(name) for name in lamas_name_candidates]
    lamas_core_keys = [_token_key(name) for name in lamas_core_names]
    # Reuse the core guard: settlement pseudo-records cannot acquire a
    # phonetic boost, and an empty core must never match another empty core.
    lamas_phonetic_keys = [hebrew_phonetic_key(name) for name in lamas_core_names]
    lamas_numbers = np.array([_number_key(name) for name in lamas_name_candidates])
    phonetic_score = max(0.0, min(PHONETIC_SUGGESTION_SCORE, confident_threshold - 1.0))

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
        lexical_matrix = np.maximum(score_matrix, core_matrix)
        token_matrix = np.maximum(
            _equal_key_matrix([_token_key(n) for n in batch_names], lamas_token_keys, 100.0),
            _equal_key_matrix([_token_key(n) for n in batch_cores], lamas_core_keys, 100.0),
        )
        phonetic_matrix = _equal_key_matrix(
            [hebrew_phonetic_key(n) for n in batch_cores], lamas_phonetic_keys, phonetic_score
        )
        final_matrix = np.maximum(np.maximum(lexical_matrix, token_matrix), phonetic_matrix)
        # A single digit mismatch can otherwise score >90 in a long name.
        # An absent number also conflicts; do not silently drop qualifiers.
        batch_numbers = np.array([_number_key(name) for name in batch_names])
        number_conflicts = batch_numbers[:, None] != lamas_numbers[None, :]
        final_matrix[number_conflicts] = 0.0

        for row_index, osm_name in enumerate(batch_names):
            row_final = final_matrix[row_index]
            row_full = score_matrix[row_index]
            best_score = float(row_final.max())
            if number_conflicts[row_index].all():
                street_name_results[osm_name] = {
                    "status": "NEEDS_AI", "best_score": 0.0,
                    "best_LAMAS_id": None, "best_LAMAS_name": None,
                    "match_method": "none", "match_review_reason": "number_conflict",
                }
                continue
            # Ties are compared with a tiny tolerance against float noise.
            tied = np.flatnonzero(
                (row_final >= best_score - 1e-9) & ~number_conflicts[row_index]
            )
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

            best_index = next(
                idx for idx in finalists
                if best_lid in unique_name_to_ids[lamas_name_candidates[idx]]
            )
            if osm_name == lamas_name_candidates[best_index]:
                method = "exact"
            elif token_matrix[row_index, best_index] > lexical_matrix[row_index, best_index] + 1e-9:
                method = "token_order"
            elif phonetic_matrix[row_index, best_index] > lexical_matrix[row_index, best_index] + 1e-9:
                method = "hebrew_phonetic"
            elif core_matrix[row_index, best_index] > row_full[best_index] + 1e-9:
                method = "core_fuzzy"
            else:
                method = "fuzzy"

            status = "CONFIDENT" if best_score >= confident_threshold else "NEEDS_AI"
            review_reason = "phonetic_only" if method == "hebrew_phonetic" else ""
            tied_ids = {
                lid
                for idx in tied
                for lid in unique_name_to_ids[lamas_name_candidates[idx]]
            }
            finalist_ids = {
                lid for idx in finalists
                for lid in unique_name_to_ids[lamas_name_candidates[idx]]
            }
            if (
                len(finalist_ids) > 1
                or (
                    len(tied_ids) > 1
                    and (tied_full_best < confident_threshold or method in {"token_order", "hebrew_phonetic"})
                )
            ):
                # Multiple IDs remain plausible after the available evidence;
                # do not turn input row order into an automatic approval.
                status = "NEEDS_AI"
                review_reason = "ambiguous_candidates"
                logger.debug(
                    "Ambiguous street match for '%s' across %d LAMAS ids; "
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
                "match_method": method,
                "match_review_reason": review_reason,
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
                    "match_method": "missing",
                    "match_review_reason": "",
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
