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

from arabic_matching import (
    DIGIT_RE as _DIGIT_RE,
    arabic_hebrew_skeleton,
    arabic_long_vowels_compatible,
    arabic_phonetic_skeleton,
    arabic_skeleton_to_hebrew_nouns,
    bare_hebrew_street_noun,
    extract_embedded_street_number,
    has_arabic,
    has_arabic_article_in_hebrew,
    has_hebrew,
    hebrew_arabic_article_form,
    hebrew_noun_to_arabic_skeletons,
    is_arab_style_lamas_city,
    is_latin_only,
    latin_arabic_to_hebrew_skeleton,
    latin_comparison_skeleton,
    strip_arabic_street_prefix,
    transliterate_arabic_to_hebrew,
)
from settlement_names import canonical_key

logger = logging.getLogger(__name__)

DEFAULT_CONFIDENT_THRESHOLD = 90.0
ARABIC_TRANSLIT_CONFIDENT_THRESHOLD = 92.0
LATIN_ARABIC_CONFIDENT_THRESHOLD = 95.0

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
    """Strip leading generic Hebrew or Arabic street-type prefixes for core-name similarity.

    Repeatedly removes a leading street-type word ("דרך נתיב האבות" ->
    "נתיב האבות" -> "האבות"; "شارع 1108" -> "1108").  Callers should run
    :func:`normalize_street_name` first so bare abbreviations ("סמ אורן") are
    already expanded.
    """
    text = (name or "").strip()
    prev = None
    while prev != text:
        prev = text
        text = STREET_PREFIX_RE.sub("", text).strip()
        text = strip_arabic_street_prefix(text)
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

    # Settlement own-name records (and all spelling aliases sharing their LAMAS_id,
    # e.g. 9000 "ג'ולס"/"ג'וליס") are ineligible for core-level / cross-script matching.
    settlement_key = ""
    if "city" in osm_gdf.columns and len(osm_gdf):
        settlement_key = canonical_key(osm_gdf["city"].iloc[0])
    settlement_ids: set[str] = set()
    if settlement_key:
        for name, lids in unique_name_to_ids.items():
            if canonical_key(name) == settlement_key:
                settlement_ids.update(lids)

    osm_core_names = [strip_street_prefix(name) for name in osm_names]
    lamas_core_names: List[str] = []
    for name in lamas_name_candidates:
        lids = unique_name_to_ids[name]
        if settlement_key and (
            canonical_key(name) == settlement_key
            or (settlement_ids and all(lid in settlement_ids for lid in lids))
        ):
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

    # Cross-script Arabic / Latin-Arabic / Hebrew-Arabic-article / embedded-number
    # rescue pass for streets still below confident_threshold.
    unresolved_osm = [
        name for name, res in street_name_results.items() if res["status"] != "CONFIDENT"
    ]
    if unresolved_osm:
        raw_by_norm: Dict[str, str] = {}
        if "osm_name" in osm_gdf.columns:
            for norm_n, raw_n in zip(osm_gdf["normalized_name"], osm_gdf["osm_name"]):
                if pd.notna(norm_n) and pd.notna(raw_n) and str(norm_n) not in raw_by_norm:
                    raw_by_norm[str(norm_n)] = str(raw_n)
        orig_by_norm: Dict[str, str] = {}
        if "osm_name_original" in osm_gdf.columns:
            for norm_n, orig_n in zip(osm_gdf["normalized_name"], osm_gdf["osm_name_original"]):
                if pd.notna(norm_n) and pd.notna(orig_n) and str(norm_n) not in orig_by_norm:
                    orig_by_norm[str(norm_n)] = str(orig_n)

        non_settlement_lamas = [
            (name, core)
            for name, core in zip(lamas_name_candidates, lamas_core_names)
            if core  # settlement own-name records were blanked out above
        ]
        is_arab_city = is_arab_style_lamas_city([name for name, _ in non_settlement_lamas])

        # Precompute LAMAS lookup structures once per city.
        num_to_ids: Dict[str, set[str]] = {}
        skel_entries: Dict[str, List[tuple[str, str, List[str]]]] = {}
        phon_entries: Dict[str, List[tuple[str, str, List[str], bool]]] = {}
        bare_he_entries: Dict[str, List[tuple[str, List[str]]]] = {}
        lat_skel_entries: Dict[str, List[tuple[str, List[str]]]] = {}
        art_strict: Dict[str, List[tuple[str, bool, List[str], str]]] = {}
        art_attached: Dict[str, List[tuple[str, bool, List[str]]]] = {}
        lcores_by_id: Dict[str, List[str]] = {}
        lnames_by_id: Dict[str, List[str]] = {}
        for lname, lcore in non_settlement_lamas:
            lids = unique_name_to_ids[lname]
            l_digs = _DIGIT_RE.findall(lcore)
            for lid in lids:
                lcores_by_id.setdefault(lid, []).append(lcore)
                lnames_by_id.setdefault(lid, []).append(lname)
            if re.fullmatch(r"\d{3,4}", lcore):
                num_to_ids.setdefault(lcore, set()).update(lids)
            l_has_ar_art = has_arabic_article_in_hebrew(
                lcore, allow_attached_al=is_arab_city
            )
            lsk = arabic_hebrew_skeleton(lcore)
            if lsk and len(lsk.replace(" ", "")) >= 3:
                for lid in lids:
                    skel_entries.setdefault(lsk, []).append((lid, lcore, l_digs))
                llsk = latin_comparison_skeleton(lsk)
                if llsk and len(llsk.replace(" ", "")) >= 3:
                    for lid in lids:
                        lat_skel_entries.setdefault(llsk, []).append((lid, l_digs))
            lph = arabic_phonetic_skeleton(lcore)
            if lph and len(lph.replace(" ", "")) >= 3:
                for lid in lids:
                    phon_entries.setdefault(lph, []).append((lid, lcore, l_digs, l_has_ar_art))
            lbare = bare_hebrew_street_noun(lcore)
            if lbare and len(lbare.replace(" ", "")) >= 2:
                for lid in lids:
                    bare_he_entries.setdefault(lbare, []).append((lid, l_digs))
            af_strict = hebrew_arabic_article_form(lcore, strip_attached_al=False)
            if af_strict and len(af_strict.replace(" ", "")) >= 3:
                # Preserve the literal (non-vowel-folded) article-stripped form so
                # mixed cities don't equate unrelated Hebrew words like תקוה vs תקווא.
                lit_lcore = re.sub(r"(?:^|\s)(?:אל|א)(?=\s)", " ", lcore).strip()
                lit_lcore = re.sub(r"\s+", " ", lit_lcore)
                for lid in lids:
                    art_strict.setdefault(af_strict, []).append(
                        (lid, l_has_ar_art, l_digs, lit_lcore)
                    )
            if is_arab_city:
                af_att = hebrew_arabic_article_form(
                    lcore, strip_attached_al=True, strip_he_prefix=True
                )
                if af_att and len(af_att.replace(" ", "")) >= 3:
                    for lid in lids:
                        art_attached.setdefault(af_att, []).append((lid, l_has_ar_art, l_digs))
        skel_items = list(skel_entries.items())
        lat_skel_items = list(lat_skel_entries.items())

        def _pick_best_lid(lids: set[str]) -> str:
            return min(lids, key=id_order.__getitem__)

        def _is_neighborhood_id(lid: str) -> bool:
            disp = str(display_name_by_id.get(lid, "")).strip()
            return bool(re.match(r"^(?:שכ|שכונת)\b", disp))

        def _is_square_id(lid: str) -> bool:
            return any(
                re.match(r"^(?:כיכר|ככר)\b", nm.strip())
                for nm in lnames_by_id.get(lid, [])
            )

        def _disambiguate_arabic_tie(
            raw_ar: str, he_tr: str, tied_lids: set[str]
        ) -> set[str]:
            """Break cross-ID skeleton ties using street-type and exact-transliteration signals."""
            cands = set(tied_lids)
            raw_words = re.sub(r"\s+", " ", raw_ar.strip()).split()
            first_ar = raw_words[0].replace("ة", "ه") if raw_words else ""

            # 1. Square/roundabout prefix in Arabic prefers כיכר/ככר in LAMAS.
            if first_ar in {"ساحه", "الساحه", "ميدان", "الميدان", "دوار", "الدوار"}:
                sq = {lid for lid in cands if _is_square_id(lid)}
                if sq and len(sq) < len(cands):
                    cands = sq

            # 2. Roads/streets (not حي/حارة) prefer street records over neighborhood (שכ) records.
            if len(cands) > 1 and first_ar not in {"حي", "الحي", "حاره", "الحاره"}:
                non_nb = {lid for lid in cands if not _is_neighborhood_id(lid)}
                if non_nb and len(non_nb) < len(cands):
                    cands = non_nb

            # 3. Prefer a unique candidate with clearly higher un-stripped Hebrew fidelity
            #    (preserves vowels and צ vs ס so e.g. אלסהל beats צה"ל, אלסעדיה beats סעידה).
            if len(cands) > 1:
                tr_form = hebrew_arabic_article_form(he_tr, strip_attached_al=True)
                if tr_form:
                    lid_scores: List[tuple[float, str]] = []
                    for lid in cands:
                        best_f = max(
                            (
                                float(
                                    fuzz.ratio(
                                        tr_form,
                                        hebrew_arabic_article_form(
                                            lc, strip_attached_al=True
                                        ),
                                    )
                                )
                                for lc in lcores_by_id.get(lid, [])
                            ),
                            default=0.0,
                        )
                        lid_scores.append((best_f, lid))
                    lid_scores.sort(key=lambda x: x[0], reverse=True)
                    if (
                        len(lid_scores) >= 2
                        and lid_scores[0][0] >= 95.0
                        and lid_scores[0][0] - lid_scores[1][0] >= 6.0
                    ):
                        return {lid_scores[0][1]}
            return cands

        def _match_arabic_raw(raw_ar: str) -> dict | None:
            """Match an Arabic-script street name against LAMAS Hebrew candidates."""
            he_translit = transliterate_arabic_to_hebrew(raw_ar, strip_prefix=True)
            sk = arabic_hebrew_skeleton(he_translit)
            t_digs = _DIGIT_RE.findall(he_translit)
            best_sc = 0.0
            best_lids: set[str] = set()
            if sk and len(sk.replace(" ", "")) >= 3:
                for lsk, entries in skel_items:
                    sc = float(fuzz.ratio(sk, lsk))
                    if sc < max(best_sc - 1e-9, 75.0):
                        continue
                    valid_lids = {
                        lid for lid, lcore, l_digs in entries
                        if l_digs == t_digs
                        and arabic_long_vowels_compatible(he_translit, lcore)
                    }
                    if not valid_lids:
                        continue
                    if sc > best_sc + 1e-9:
                        best_sc = sc
                        best_lids = valid_lids
                    elif abs(sc - best_sc) <= 1e-9:
                        best_lids |= valid_lids
                if len(best_lids) > 1 and best_sc >= ARABIC_TRANSLIT_CONFIDENT_THRESHOLD:
                    best_lids = _disambiguate_arabic_tie(raw_ar, he_translit, best_lids)
                if best_lids and best_sc >= ARABIC_TRANSLIT_CONFIDENT_THRESHOLD and len(best_lids) == 1:
                    lid = _pick_best_lid(best_lids)
                    return {
                        "status": "CONFIDENT",
                        "best_score": best_sc,
                        "best_LAMAS_id": lid,
                        "best_LAMAS_name": display_name_by_id.get(lid, ""),
                    }

            # 2b. Exact Arabic-to-Hebrew phonetic skeleton match (ט<->ת, ח<->כ, interior א),
            #     plus trailing feminine -ה (e.g. نتاف -> נטאף, البيرة -> אלביארה, الزيتونة -> אלזיתון).
            ph = arabic_phonetic_skeleton(he_translit)
            if ph and len(ph.replace(" ", "")) >= 3:
                ph_lids = {
                    lid
                    for lid, lcore, l_digs, _ in phon_entries.get(ph, [])
                    if l_digs == t_digs
                    and arabic_long_vowels_compatible(he_translit, lcore)
                }
                if len(ph_lids) > 1:
                    ph_lids = _disambiguate_arabic_tie(raw_ar, he_translit, ph_lids)
                if len(ph_lids) == 1:
                    lid = _pick_best_lid(ph_lids)
                    return {
                        "status": "CONFIDENT",
                        "best_score": 100.0,
                        "best_LAMAS_id": lid,
                        "best_LAMAS_name": display_name_by_id.get(lid, ""),
                    }
                if len(ph) >= 4 and ph.endswith("ה"):
                    fem_lids = {
                        lid
                        for lid, lcore, l_digs, _ in phon_entries.get(ph[:-1], [])
                        if l_digs == t_digs
                        and arabic_long_vowels_compatible(he_translit, lcore)
                    }
                    if len(fem_lids) > 1:
                        fem_lids = _disambiguate_arabic_tie(raw_ar, he_translit, fem_lids)
                    if len(fem_lids) == 1:
                        lid = _pick_best_lid(fem_lids)
                        return {
                            "status": "CONFIDENT",
                            "best_score": 95.0,
                            "best_LAMAS_id": lid,
                            "best_LAMAS_name": display_name_by_id.get(lid, ""),
                        }

                # 2c. Curated Arabic -> Hebrew street-noun translation (e.g. شارع السلام -> דרך השלום).
                he_nouns = arabic_skeleton_to_hebrew_nouns(ph)
                if he_nouns and not t_digs:
                    gloss_lids = {
                        lid
                        for he_n in he_nouns
                        for lid, l_digs in bare_he_entries.get(he_n, [])
                        if l_digs == t_digs
                    }
                    if len(gloss_lids) > 1:
                        gloss_lids = _disambiguate_arabic_tie(raw_ar, he_translit, gloss_lids)
                    if len(gloss_lids) == 1:
                        lid = _pick_best_lid(gloss_lids)
                        return {
                            "status": "CONFIDENT",
                            "best_score": 100.0,
                            "best_LAMAS_id": lid,
                            "best_LAMAS_name": display_name_by_id.get(lid, ""),
                        }

            if best_lids:
                lid = _pick_best_lid(best_lids)
                return {
                    "status": "NEEDS_AI",
                    "best_score": best_sc,
                    "best_LAMAS_id": lid,
                    "best_LAMAS_name": display_name_by_id.get(lid, ""),
                }
            return None

        def _match_latin_raw(raw_lat: str, digs: List[str]) -> dict | None:
            """Match a Latin-transliterated Arabic street name against LAMAS Hebrew."""
            sk = latin_arabic_to_hebrew_skeleton(raw_lat)
            if not sk or len(sk.replace(" ", "")) < 3:
                return None
            best_sc = 0.0
            best_lids: set[str] = set()
            for lsk, entries in skel_items:
                sc = float(fuzz.ratio(sk, lsk))
                if sc < max(best_sc - 1e-9, 80.0):
                    continue
                valid_lids = {
                    lid for lid, _lcore, l_digs in entries if l_digs == digs
                }
                if not valid_lids:
                    continue
                if sc > best_sc + 1e-9:
                    best_sc = sc
                    best_lids = valid_lids
                elif abs(sc - best_sc) <= 1e-9:
                    best_lids |= valid_lids

            # Also compare after folding Latin-indistinguishable ע<->א / ט<->ת / ח<->כ
            # (e.g. "Utman ben afan" -> "עותמאן בן עפאן").
            lsk_q = latin_comparison_skeleton(sk)
            if lsk_q and len(lsk_q.replace(" ", "")) >= 4:
                for llsk, entries in lat_skel_items:
                    sc = float(fuzz.ratio(lsk_q, llsk))
                    if sc < max(best_sc - 1e-9, LATIN_ARABIC_CONFIDENT_THRESHOLD):
                        continue
                    valid_lids = {lid for lid, l_digs in entries if l_digs == digs}
                    if not valid_lids:
                        continue
                    if sc > best_sc + 1e-9:
                        best_sc = sc
                        best_lids = valid_lids
                    elif abs(sc - best_sc) <= 1e-9:
                        best_lids |= valid_lids

            if not best_lids:
                return None
            lid = _pick_best_lid(best_lids)
            is_conf = (
                best_sc >= LATIN_ARABIC_CONFIDENT_THRESHOLD
                and len(best_lids) == 1
            )
            return {
                "status": "CONFIDENT" if is_conf else "NEEDS_AI",
                "best_score": best_sc,
                "best_LAMAS_id": lid,
                "best_LAMAS_name": display_name_by_id.get(lid, ""),
            }

        for osm_name in unresolved_osm:
            raw_name = raw_by_norm.get(osm_name, osm_name)
            orig_name = orig_by_norm.get(osm_name, "")
            osm_core = strip_street_prefix(osm_name)
            osm_digs = _DIGIT_RE.findall(osm_name)

            # 1. Unique embedded 3-4 digit street number (e.g. "المدينة القديمة - 6171" -> "רח 6171").
            embedded_num = extract_embedded_street_number(raw_name)
            if embedded_num and embedded_num in num_to_ids:
                cand_ids = num_to_ids[embedded_num]
                if len(cand_ids) == 1:
                    lid = next(iter(cand_ids))
                    street_name_results[osm_name] = {
                        "status": "CONFIDENT",
                        "best_score": 100.0,
                        "best_LAMAS_id": lid,
                        "best_LAMAS_name": display_name_by_id.get(lid, embedded_num),
                    }
                    continue

            # 2. Arabic-script OSM -> LAMAS Hebrew via Semitic consonantal / phonetic skeleton.
            if has_arabic(raw_name):
                ar_res = _match_arabic_raw(raw_name)
                if ar_res and (
                    ar_res["status"] == "CONFIDENT"
                    or ar_res["best_score"] > street_name_results[osm_name]["best_score"]
                ):
                    street_name_results[osm_name] = ar_res
                continue

            # 3. Latin-transliterated Arabic OSM -> LAMAS Hebrew.
            if is_latin_only(raw_name):
                lat_res = _match_latin_raw(raw_name, osm_digs)
                if lat_res and (
                    lat_res["status"] == "CONFIDENT"
                    or lat_res["best_score"] > street_name_results[osm_name]["best_score"]
                ):
                    street_name_results[osm_name] = lat_res
                continue

            # 4. Hebrew-script OSM rescue pass:
            #    4a/4b: Arabic definite-article normalization
            #    4c: fallback to Arabic/Latin osm_name_original (e.g. name:he="כביש אלבלסם",
            #        original="شارع البلسم" -> LAMAS "אלבלסם")
            #    4d: Arab-city Hebrew phonetic equivalence (אלבטיכה -> אלבטיח'ה, אלביירה -> אלביארה)
            #    4e: Arab-city bilingual street-noun glossary (הנמל -> אלמינא, האלמוגים -> אלמורג'אן)
            if has_hebrew(osm_core):
                osm_has_ar_art = has_arabic_article_in_hebrew(
                    osm_core, allow_attached_al=is_arab_city
                )
                matched_lids: set[str] = set()
                # 4a. Spaced Arabic article normalization (works in every city, incl. mixed cities).
                af_s = hebrew_arabic_article_form(osm_core, strip_attached_al=False)
                if af_s and len(af_s.replace(" ", "")) >= 3 and af_s in art_strict:
                    lit_osm = re.sub(r"(?:^|\s)(?:אל|א)(?=\s)", " ", osm_core).strip()
                    lit_osm = re.sub(r"\s+", " ", lit_osm)
                    for lid, l_has_ar, l_digs, lit_lcore in art_strict[af_s]:
                        if l_digs != osm_digs or not (osm_has_ar_art or l_has_ar):
                            continue
                        # In mixed/Jewish cities, if OSM has no Arabic article (e.g. "דרך תקוה"),
                        # require literal match after article removal so it won't fold into "אלתקווא".
                        if not is_arab_city and not osm_has_ar_art and lit_osm != lit_lcore:
                            continue
                        matched_lids.add(lid)
                # 4b. Attached אל / Hebrew ה<->אל article normalization in Arab-style cities.
                if not matched_lids and is_arab_city:
                    af_a = hebrew_arabic_article_form(
                        osm_core, strip_attached_al=True, strip_he_prefix=True
                    )
                    if af_a and len(af_a.replace(" ", "")) >= 3 and af_a in art_attached:
                        matched_lids.update(
                            lid for lid, l_has_ar, l_digs in art_attached[af_a]
                            if l_digs == osm_digs and (osm_has_ar_art or l_has_ar)
                        )
                if len(matched_lids) == 1:
                    lid = next(iter(matched_lids))
                    street_name_results[osm_name] = {
                        "status": "CONFIDENT",
                        "best_score": 100.0,
                        "best_LAMAS_id": lid,
                        "best_LAMAS_name": display_name_by_id.get(lid, ""),
                    }
                    continue

                # 4c. Original Arabic/Latin OSM `name` fallback when `name:he` did not match.
                if orig_name and orig_name != raw_name:
                    if has_arabic(orig_name):
                        orig_res = _match_arabic_raw(orig_name)
                        if orig_res and orig_res["status"] == "CONFIDENT":
                            street_name_results[osm_name] = orig_res
                            continue
                    elif is_latin_only(orig_name):
                        orig_lat = _match_latin_raw(orig_name, _DIGIT_RE.findall(orig_name))
                        if orig_lat and orig_lat["status"] == "CONFIDENT":
                            street_name_results[osm_name] = orig_lat
                            continue

                # 4d & 4e: Arab-style cities only (never touch highways like כביש החוף).
                if is_arab_city and not re.match(r"^\s*כביש\b", raw_name):
                    # 4d. Exact Arabic-transliterated Hebrew phonetic match (requires Arabic article).
                    ph_he = arabic_phonetic_skeleton(osm_core)
                    if ph_he and len(ph_he.replace(" ", "")) >= 3 and ph_he in phon_entries:
                        ph_lids = {
                            lid
                            for lid, _lcore, l_digs, l_has_ar in phon_entries[ph_he]
                            if l_digs == osm_digs and (osm_has_ar_art or l_has_ar)
                        }
                        if len(ph_lids) == 1:
                            lid = next(iter(ph_lids))
                            street_name_results[osm_name] = {
                                "status": "CONFIDENT",
                                "best_score": 100.0,
                                "best_LAMAS_id": lid,
                                "best_LAMAS_name": display_name_by_id.get(lid, ""),
                            }
                            continue

                    # 4e. Bilingual Hebrew-translation -> Arabic-transliteration street noun.
                    ar_targets = hebrew_noun_to_arabic_skeletons(osm_core)
                    if ar_targets and not osm_digs:
                        gloss_lids = {
                            lid
                            for target_ph in ar_targets
                            for lid, _lcore, l_digs, _ in phon_entries.get(target_ph, [])
                            if l_digs == osm_digs
                        }
                        if len(gloss_lids) == 1:
                            lid = next(iter(gloss_lids))
                            street_name_results[osm_name] = {
                                "status": "CONFIDENT",
                                "best_score": 100.0,
                                "best_LAMAS_id": lid,
                                "best_LAMAS_name": display_name_by_id.get(lid, ""),
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
