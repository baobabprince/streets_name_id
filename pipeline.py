"""
Street name mapping pipeline orchestrator.

Maps OpenStreetMap street names to official LAMAS (CBS) street IDs using
normalization, 90% fuzzy matching, and optional whole-city Laya AI resolution
for everything below 90%.
"""

from __future__ import annotations

import argparse
import inspect
import logging
import os
import re
import sys
import traceback
from enum import Enum

import pandas as pd
from rapidfuzz import fuzz

from arabic_matching import (
    arabic_hebrew_skeleton,
    arabic_long_vowels_compatible,
    arabic_phonetic_skeleton,
    arabic_skeleton_to_hebrew_nouns,
    bare_hebrew_street_noun,
    cross_script_comparison_skeleton,
    has_arabic,
    has_arabic_article_in_hebrew,
    has_hebrew,
    hebrew_arabic_article_form,
    hebrew_noun_to_arabic_skeletons,
    is_latin_only,
    latin_arabic_to_hebrew_skeleton,
    latin_comparison_skeleton,
    latin_to_hebrew_skeleton,
    transliterate_arabic_to_hebrew,
)
from data_loader import (
    city_key,
    load_or_fetch_LAMAS,
    load_or_fetch_osm,
    normalize_city,
    safe_place_name,
    save_intermediate_df,
)
from diagnostics import calculate_diagnostics
from laya_ai_resolver import LayaAIResolver
from openrouter_ai_resolver import HybridAIResolver
from normalization import (
    DEFAULT_CONFIDENT_THRESHOLD,
    NON_STREET_FEATURE_PREFIX_RE,
    _HONORIFIC_RE,
    build_lamas_aliases,
    find_fuzzy_candidates,
    normalize_street_name,
    strip_street_prefix,
)
from OSM_streets import place_name as OSM_PLACE_NAME
from settlement_names import canonical_key, settlement_identity_key

logger = logging.getLogger(__name__)

ISRAEL_BOUNDS = (29.0, 34.0, 33.7, 36.0)
_LAYA_RESOLVER: LayaAIResolver | None = None
_AI_RESOLVER: object | None = None
_AI_RESOLVER_KEY: tuple[str, str | None] | None = None
ESSENTIAL_STEPS = {"diagnostic_report"}

# Conservative AI approval guardrails (Option 3: ~95-96% precision, zero magnets)
AI_MIN_CONFIDENCE = 0.70
AI_MIN_CORE_WRATIO = 75.0
AI_MAX_RAW_HITS_PER_ID = 10
AI_MAX_APPROVED_HITS_PER_ID = 2

# The AI may replace the fuzzy top-1 candidate (an "override"), but only with
# stronger evidence than an agreement with the fuzzy candidate requires.
AI_OVERRIDE_MIN_CONFIDENCE = 0.80
AI_OVERRIDE_MIN_CORE_WRATIO = 80.0

# High-confidence OpenRouter Decisions thresholds for genuine cross-script
# translations (Arabic/Latin -> Hebrew) and semantic/acronym equivalences.
OPENROUTER_CROSS_SCRIPT_MIN_CONFIDENCE = 0.90
OPENROUTER_CROSS_SCRIPT_MIN_CORE_WRATIO = 50.0
OPENROUTER_SEMANTIC_MIN_CONFIDENCE = 0.92
OPENROUTER_SEMANTIC_MIN_CORE_WRATIO = 60.0

_DIGIT_RE = re.compile(r"\d+")

_FEATURE_FAMILY = {
    "כיכר": "square",
    "ככר": "square",
    "פארק": "park",
    "גן": "park",
    "גשר": "bridge",
    "טיילת": "promenade",
    "חניון": "parking",
    "צומת": "junction",
    "מחלף": "junction",
}

# Common Hebrew given / biblical / tribe names that frequently appear as
# standalone LAMAS street names ("נפתלי", "שמואל", "יחזקאל", "אבשלום", "ראובן")
# and must not be matched by AI to a full person name ("נפתלי הרץ אימבר",
# "יחזקאל קויפמן", "אבשלום פיינברג") solely via the first word.
_COMMON_HEBREW_GIVEN_NAMES = frozenset({
    "אברהם", "יצחק", "יעקב", "משה", "אהרן", "אהרון", "דוד", "שלמה",
    "שמואל", "נפתלי", "יחזקאל", "אבשלום", "ראובן", "שמעון", "לוי",
    "יהודה", "דן", "גד", "אשר", "זבולון", "יששכר", "יוסף", "בנימין",
    "אליהו", "יונה", "ישעיהו", "ירמיהו", "דניאל", "חיים", "מנחם",
    "רחל", "שרה", "רבקה", "לאה", "מרים", "אסתר", "דבורה", "רות",
})


_ARABIC_SQUARE_PREFIX_RE = re.compile(r"^\s*(?:دوار|الدوار)\b")


def _feature_family(name: str) -> str | None:
    m = NON_STREET_FEATURE_PREFIX_RE.match(name or "")
    if m:
        return _FEATURE_FAMILY.get(m.group(1))
    if _ARABIC_SQUARE_PREFIX_RE.match(name or ""):
        return "square"
    return None


def _feature_type_compatible(osm_label: str, lamas_candidates: list[str]) -> bool:
    """Require squares, bridges, parks, etc. in OSM to match the same feature family."""
    osm_fam = _feature_family(osm_label)
    if not osm_fam:
        return True
    return any(_feature_family(cand) == osm_fam for cand in lamas_candidates if cand)


def _is_given_name_only_collision(osm_label: str, lamas_candidates: list[str]) -> bool:
    """Reject AI matches that collapse a full person name to an unrelated first-name street."""
    osm_core = strip_street_prefix(normalize_street_name(osm_label) or "")
    o_words = osm_core.split()
    if len(o_words) < 2 or o_words[1] == "בן":
        return False
    first_word, last_word = o_words[0], o_words[-1]
    if first_word == last_word or first_word not in _COMMON_HEBREW_GIVEN_NAMES:
        return False
    lam_cores = [
        strip_street_prefix(normalize_street_name(c) or "")
        for c in lamas_candidates
        if c
    ]
    lam_cores = [lc for lc in lam_cores if lc]
    if not lam_cores or not all(len(lc.split()) == 1 for lc in lam_cores):
        return False
    return any(lc == first_word for lc in lam_cores) and not any(lc == last_word for lc in lam_cores)


def _save_step(
    df: pd.DataFrame, step_name: str, identifier: str, save_intermediates: bool
) -> None:
    """Skip bulky intermediate pickles unless explicitly requested."""
    if not save_intermediates and step_name not in ESSENTIAL_STEPS:
        return
    save_intermediate_df(df, step_name, identifier)


def _filter_lamas_by_city(
    LAMAS_df: pd.DataFrame,
    target_key: str,
    target_identity_key: str | None = None,
) -> pd.DataFrame:
    """Join LAMAS streets to a settlement without merging colliding twin names.

    Matches on ``city_identity_key`` first (keeping ``גת )קיבוץ(`` separate from
    ``ג'ת``, ``טמרה )יזרעאל(`` separate from ``טמרה``, ``איחוד`` from ``מאוחד``),
    falling back to ``city_key`` (canonical key) only when no identity-key row
    matches.
    """
    if target_identity_key and "city_identity_key" in LAMAS_df.columns:
        exact_rows = LAMAS_df[LAMAS_df["city_identity_key"] == target_identity_key]
        if not exact_rows.empty:
            return exact_rows
    rows = LAMAS_df[LAMAS_df["city_key"] == target_key]
    if not rows.empty or not target_key:
        return rows
    try:
        from rapidfuzz import fuzz, process

        keys = sorted(set(LAMAS_df["city_key"].dropna().tolist()))
        best = process.extractOne(
            target_key, keys, scorer=fuzz.ratio, score_cutoff=92
        )
        if best:
            logger.info(
                "Fuzzy city key match: '%s' -> '%s' (%.1f)",
                target_key,
                best[0],
                best[1],
            )
            return LAMAS_df[LAMAS_df["city_key"] == best[0]]
    except Exception as exc:
        logger.debug("Fuzzy city fallback unavailable: %s", exc)
    return rows


class PipelineStatus(Enum):
    SUCCESS = "SUCCESS"
    NO_DATA = "NO_DATA"
    NO_LAMAS_STREETS = "NO_LAMAS_STREETS"
    FAILURE = "FAILURE"


# Pipeline outcomes that completed correctly but have nothing to map.
NO_DATA_STATUSES = (PipelineStatus.NO_DATA, PipelineStatus.NO_LAMAS_STREETS)


def _setup_logging(level: int = logging.INFO) -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        )


def _get_laya_resolver() -> LayaAIResolver | None:
    """Reuse one initialized Laya model across settlements in this process."""
    global _LAYA_RESOLVER
    if _LAYA_RESOLVER is not None:
        return _LAYA_RESOLVER

    resolver = LayaAIResolver()
    if resolver.is_available():
        _LAYA_RESOLVER = resolver
        return resolver
    return None


def _get_ai_resolver(backend: str = "auto", model: str | None = None):
    """Return the configured AI resolver (OpenRouter+fallback by default, or local Laya)."""
    global _AI_RESOLVER, _AI_RESOLVER_KEY
    selected = (backend or os.environ.get("AI_BACKEND", "auto") or "auto").strip().lower()
    if selected in {"local", "laya"}:
        return _get_laya_resolver()

    has_key = bool(os.environ.get("OPENROUTER_API_KEY", "").strip())
    if selected == "auto" and not has_key:
        logger.warning(
            "OPENROUTER_API_KEY not set; falling back automatically to local Laya resolver."
        )
        return _get_laya_resolver()

    cache_key = (selected, model)
    if _AI_RESOLVER is not None and _AI_RESOLVER_KEY == cache_key:
        return _AI_RESOLVER

    if selected == "openrouter" and not has_key:
        logger.warning(
            "AI backend 'openrouter' requested without OPENROUTER_API_KEY; "
            "using HybridAIResolver with automatic fallback to local Laya."
        )

    resolver = HybridAIResolver(local_factory=_get_laya_resolver, model=model)
    if resolver.is_available():
        _AI_RESOLVER = resolver
        _AI_RESOLVER_KEY = cache_key
        return resolver
    return None


def _clean_id(value: object) -> str:
    """Normalize an ID value to a clean string (handling None/NaN and float '.0')."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "nan", "<na>"}:
        return ""
    if text.endswith(".0") and text[:-2].lstrip("-").isdigit():
        text = text[:-2]
    return text


def _digits_compatible(osm_name: str, lamas_name: str) -> bool:
    """Reject numeric mismatches (e.g. 'גדוד 21' vs 'גדוד 22' or '5083' vs 'רח 7083')."""
    da = _DIGIT_RE.findall(osm_name or "")
    db = _DIGIT_RE.findall(lamas_name or "")
    if not da and not db:
        return True
    return da == db


def _core_wratio(osm_name: str, lamas_name: str) -> float:
    """Compute WRatio between normalized prefix-stripped OSM and LAMAS names.

    Cross-script Arabic or Latin-Arabic OSM names are also compared via Hebrew
    transliteration / Semitic consonantal skeletons so valid multilingual Laya
    matches are not rejected with 0.0 script-mismatch scores.
    """
    norm_osm = normalize_street_name(osm_name) or ""
    norm_lamas = normalize_street_name(lamas_name) or ""
    core_osm = strip_street_prefix(norm_osm)
    core_lamas = strip_street_prefix(norm_lamas)
    if not core_osm or not core_lamas:
        return 0.0
    score = float(fuzz.WRatio(core_osm, core_lamas))

    if has_arabic(osm_name):
        he_translit = transliterate_arabic_to_hebrew(osm_name, strip_prefix=True)
        if he_translit:
            score = max(score, float(fuzz.WRatio(he_translit, core_lamas)))
            if arabic_long_vowels_compatible(he_translit, core_lamas):
                sk_osm = arabic_hebrew_skeleton(he_translit)
                sk_lam = arabic_hebrew_skeleton(core_lamas)
                if len(sk_osm.replace(" ", "")) >= 3 and len(sk_lam.replace(" ", "")) >= 3:
                    score = max(score, float(fuzz.ratio(sk_osm, sk_lam)))
                ph_osm = arabic_phonetic_skeleton(he_translit)
                ph_lam = arabic_phonetic_skeleton(core_lamas)
                if len(ph_osm.replace(" ", "")) >= 3 and ph_osm == ph_lam:
                    score = max(score, 100.0)
                if (
                    len(ph_osm) >= 4
                    and ph_osm.endswith("ה")
                    and ph_osm[:-1] == ph_lam
                ):
                    score = max(score, 95.0)
                if bare_hebrew_street_noun(core_lamas) in arabic_skeleton_to_hebrew_nouns(ph_osm):
                    score = max(score, 100.0)
    elif is_latin_only(osm_name):
        sk_osm = latin_arabic_to_hebrew_skeleton(osm_name)
        sk_lam = arabic_hebrew_skeleton(core_lamas)
        if len(sk_osm.replace(" ", "")) >= 3 and len(sk_lam.replace(" ", "")) >= 3:
            score = max(score, float(fuzz.ratio(sk_osm, sk_lam)))
            lsk_osm = latin_comparison_skeleton(sk_osm)
            lsk_lam = latin_comparison_skeleton(sk_lam)
            if len(lsk_osm.replace(" ", "")) >= 3 and len(lsk_lam.replace(" ", "")) >= 3:
                score = max(score, float(fuzz.ratio(lsk_osm, lsk_lam)))
        else:
            sk_gen = latin_to_hebrew_skeleton(osm_name)
            csk_osm = cross_script_comparison_skeleton(sk_gen)
            csk_lam = cross_script_comparison_skeleton(sk_lam)
            if len(csk_osm.replace(" ", "")) >= 3 and len(csk_lam.replace(" ", "")) >= 3:
                score = max(
                    score,
                    float(fuzz.ratio(csk_osm, csk_lam)),
                    float(fuzz.WRatio(csk_osm, csk_lam)),
                )
    elif has_hebrew(core_osm):
        if has_hebrew(core_lamas):
            hon_osm = " ".join(_HONORIFIC_RE.sub("", core_osm).split())
            hon_lam = " ".join(_HONORIFIC_RE.sub("", core_lamas).split())
            if (
                (hon_osm != core_osm or hon_lam != core_lamas)
                and len(hon_osm.split()) >= 2
                and len(hon_lam.split()) >= 2
            ):
                score = max(score, float(fuzz.WRatio(hon_osm, hon_lam)))
        if (
            has_arabic_article_in_hebrew(core_osm, allow_attached_al=True)
            or has_arabic_article_in_hebrew(core_lamas, allow_attached_al=True)
        ):
            af_osm = hebrew_arabic_article_form(core_osm, strip_attached_al=True, strip_he_prefix=True)
            af_lam = hebrew_arabic_article_form(core_lamas, strip_attached_al=True, strip_he_prefix=True)
            if len(af_osm.replace(" ", "")) >= 3 and len(af_lam.replace(" ", "")) >= 3:
                af_ratio = float(fuzz.ratio(af_osm, af_lam))
                if af_ratio >= 80.0:
                    score = max(score, af_ratio)
        ph_lam = arabic_phonetic_skeleton(core_lamas)
        if ph_lam and ph_lam in hebrew_noun_to_arabic_skeletons(core_osm):
            score = max(score, 100.0)

    return score


def _finalize_mappings(
    candidates_df: pd.DataFrame,
    ai_decisions_df: pd.DataFrame,
    lamas_name_by_id: dict[str, str] | None = None,
    city_name: str | None = None,
    osm_names_by_id: dict[str, str] | None = None,
    min_ai_confidence: float = AI_MIN_CONFIDENCE,
    min_core_wratio: float = AI_MIN_CORE_WRATIO,
    max_raw_hits_per_id: int = AI_MAX_RAW_HITS_PER_ID,
    max_approved_hits_per_id: int = AI_MAX_APPROVED_HITS_PER_ID,
    min_override_confidence: float = AI_OVERRIDE_MIN_CONFIDENCE,
    min_override_core_wratio: float = AI_OVERRIDE_MIN_CORE_WRATIO,
    lamas_aliases_by_id: dict[str, list[str]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Approve >=90% fuzzy matches and strictly verified Laya AI matches.

    Every unmatched street is resolved by Laya (see :func:`run_pipeline`); the
    gates below all apply to the AI *output*.  An AI suggestion is approved
    into ``final_LAMAS_id`` only if ALL hold:

    1. ``ai_confidence`` passes the threshold — 0.70 when Laya agreed with the
       top-1 fuzzy candidate (``ai_suggested_LAMAS_id == best_LAMAS_id``),
       0.80 when Laya proposes a different candidate ("override").
    2. Prefix-stripped ``WRatio`` passes the matching threshold — 75.0 for an
       agreement, 80.0 for an override — scored against the best of all
       LAMAS aliases known for the suggested id.
    3. Any digits in the OSM and LAMAS names match identically.
    4. The matched LAMAS street name is not the settlement's own name.
    5. Magnet protection (counted per unique normalized street name):
       raw AI hits on that LAMAS ID <= ``max_raw_hits_per_id`` (10) AND
       post-filter AI hits on that LAMAS ID <= ``max_approved_hits_per_id`` (2).
    """
    left = candidates_df.copy()
    right = ai_decisions_df.copy()
    left["osm_id"] = left["osm_id"].astype(str)
    if not right.empty:
        right["osm_id"] = right["osm_id"].astype(str)
    merged = left.merge(right, on="osm_id", how="left")

    is_fuzzy = merged["status"] == "CONFIDENT"
    ai_ids = merged["ai_suggested_LAMAS_id"].map(_clean_id)
    best_ids = merged["best_LAMAS_id"].map(_clean_id)
    has_ai = (~is_fuzzy) & (ai_ids != "")

    if "normalized_name" in merged.columns:
        street_keys = merged["normalized_name"].fillna("").astype(str)
    elif osm_names_by_id:
        street_keys = merged["osm_id"].map(osm_names_by_id).fillna("").astype(str)
    else:
        street_keys = pd.Series("", index=merged.index, dtype=str)
    # Fallback to osm_id if a row has no street key so grouping never collapses unrelated blanks
    street_keys_for_group = street_keys.where(street_keys != "", merged["osm_id"])

    # Count raw unique OSM street names pointing to each AI-suggested LAMAS ID
    raw_hits_by_id: dict[str, int] = {}
    if has_ai.any():
        raw_hits_by_id = (
            pd.DataFrame({"ai_id": ai_ids[has_ai], "street": street_keys_for_group[has_ai]})
            .groupby("ai_id")["street"]
            .nunique()
            .to_dict()
        )

    target_city_key = canonical_key(city_name) if city_name else ""
    confs = pd.to_numeric(merged["ai_confidence"], errors="coerce").fillna(0.0)
    ai_methods = (
        merged["ai_method"].fillna("").astype(str)
        if "ai_method" in merged.columns
        else pd.Series("", index=merged.index, dtype=str)
    )
    lamas_lookup = {_clean_id(k): v for k, v in (lamas_name_by_id or {}).items()}
    if lamas_aliases_by_id:
        aliases_lookup = {
            _clean_id(k): [str(a) for a in v if str(a).strip()]
            for k, v in lamas_aliases_by_id.items()
        }
    else:
        aliases_lookup = {k: [v] for k, v in lamas_lookup.items() if v}

    def _best_core_score(osm_label: str, lamas_id: str, display_name: str) -> float:
        """Best prefix-stripped similarity across all aliases of the LAMAS id."""
        candidates = [display_name, *aliases_lookup.get(lamas_id, [])]
        return max((_core_wratio(osm_label, name) for name in candidates), default=0.0)

    candidate_ai = pd.Series(False, index=merged.index, dtype=bool)
    reject_reasons: dict[int, str] = {}

    for idx in merged.index[has_ai]:
        ai_id = ai_ids.at[idx]
        best_id = best_ids.at[idx]
        conf = float(confs.at[idx])
        osm_label = street_keys.at[idx]
        matched_lamas_name = lamas_lookup.get(ai_id) or str(merged.at[idx, "best_LAMAS_name"] or "")
        is_openrouter = ai_methods.at[idx].startswith("openrouter:")
        is_cross_script = bool(osm_label) and (has_arabic(osm_label) or is_latin_only(osm_label))

        agrees = bool(best_id) and ai_id == best_id
        conf_threshold = min_ai_confidence if agrees else min_override_confidence
        wratio_threshold = min_core_wratio if agrees else min_override_core_wratio

        if not best_id and not (
            is_openrouter and is_cross_script and conf >= OPENROUTER_SEMANTIC_MIN_CONFIDENCE
        ):
            reject_reasons[idx] = "no_fuzzy_candidate"
        elif conf < conf_threshold:
            kind = "" if agrees else "override_"
            reject_reasons[idx] = f"{kind}low_confidence({conf:.2f}<{conf_threshold:.2f})"
        elif raw_hits_by_id.get(ai_id, 0) > max_raw_hits_per_id:
            reject_reasons[idx] = f"magnet_raw_hits({raw_hits_by_id[ai_id]}>{max_raw_hits_per_id})"
        elif target_city_key and canonical_key(matched_lamas_name) == target_city_key:
            reject_reasons[idx] = "settlement_name_street"
        elif osm_label and not _digits_compatible(osm_label, matched_lamas_name):
            reject_reasons[idx] = "digit_mismatch"
        elif osm_label and not _feature_type_compatible(
            osm_label, [matched_lamas_name, *aliases_lookup.get(ai_id, [])]
        ):
            reject_reasons[idx] = "feature_type_mismatch"
        elif osm_label and _is_given_name_only_collision(
            osm_label, [matched_lamas_name, *aliases_lookup.get(ai_id, [])]
        ):
            reject_reasons[idx] = "given_name_only_match"
        elif osm_label:
            score = _best_core_score(osm_label, ai_id, matched_lamas_name)
            allow_openrouter_semantic = is_openrouter and (
                (
                    is_cross_script
                    and (
                        conf >= OPENROUTER_SEMANTIC_MIN_CONFIDENCE
                        or (
                            conf >= OPENROUTER_CROSS_SCRIPT_MIN_CONFIDENCE
                            and score >= OPENROUTER_CROSS_SCRIPT_MIN_CORE_WRATIO
                        )
                    )
                )
                or (
                    not is_cross_script
                    and conf >= OPENROUTER_SEMANTIC_MIN_CONFIDENCE
                    and score >= OPENROUTER_SEMANTIC_MIN_CORE_WRATIO
                )
            )
            if score < wratio_threshold and not allow_openrouter_semantic:
                kind = "" if agrees else "override_"
                reject_reasons[idx] = f"{kind}low_core_wratio({score:.1f}<{wratio_threshold:.1f})"
            else:
                candidate_ai.at[idx] = True
        else:
            candidate_ai.at[idx] = True

    # Post-filter magnet check: reject any LAMAS ID still claimed by > max_approved_hits_per_id unique streets
    is_ai = candidate_ai.copy()
    if candidate_ai.any():
        post_hits_by_id = (
            pd.DataFrame(
                {"ai_id": ai_ids[candidate_ai], "street": street_keys_for_group[candidate_ai]}
            )
            .groupby("ai_id")["street"]
            .nunique()
            .to_dict()
        )
        for idx in merged.index[candidate_ai]:
            ai_id = ai_ids.at[idx]
            if post_hits_by_id.get(ai_id, 0) > max_approved_hits_per_id:
                is_ai.at[idx] = False
                reject_reasons[idx] = (
                    f"magnet_post_hits({post_hits_by_id[ai_id]}>{max_approved_hits_per_id})"
                )

    if reject_reasons and "ai_reasoning" in merged.columns:
        for idx, reason in reject_reasons.items():
            prev = merged.at[idx, "ai_reasoning"]
            prev_text = "" if pd.isna(prev) else str(prev).strip()
            tag = f"[filtered: {reason}]"
            if tag not in prev_text:
                merged.at[idx, "ai_reasoning"] = f"{prev_text} {tag}".strip()

    merged["mapping_status"] = "UNMATCHED"
    merged.loc[is_ai, "mapping_status"] = "APPROVED_AI"
    merged.loc[is_fuzzy, "mapping_status"] = "APPROVED_FUZZY"

    if lamas_lookup and is_ai.any():
        matched_names = ai_ids.loc[is_ai].map(lamas_lookup)
        merged.loc[is_ai, "best_LAMAS_name"] = matched_names.where(
            matched_names.notna(), merged.loc[is_ai, "best_LAMAS_name"]
        )

    approved_mask = is_fuzzy | is_ai
    approved = merged[approved_mask].copy()
    approved["final_LAMAS_id"] = (
        best_ids.loc[approved_mask]
        .where(approved["mapping_status"] == "APPROVED_FUZZY", ai_ids.loc[approved_mask])
        .astype(str)
    )
    return merged, approved


def _finalize_no_lamas_streets(
    osm_gdf: pd.DataFrame,
    city_label: str,
    file_identifier: str,
    source_provenance: dict,
    reason: str,
    save_intermediates: bool,
    skip_html: bool,
) -> PipelineStatus:
    """Complete a run when the settlement has no street-level LAMAS records.

    Writes a marked diagnostic report (``status`` column = NO_LAMAS_STREETS)
    so the settlement stays auditable, and returns the dedicated
    NO_LAMAS_STREETS status instead of a misleading "100% unmatched" run.

    No HTML page is produced: with no CBS streets there is nothing to match
    against, so a page would only show a misleading 0% coverage. These
    settlements are surfaced instead in the "settlements without results"
    dashboard (``build_no_results``), grouped under "no CBS street list". The
    single special case — a settlement whose only CBS record is its own name —
    gets a dedicated info page from ``generate_single_street_pages``.
    """
    marked_diagnostic_cols = {
        "status": PipelineStatus.NO_LAMAS_STREETS.value,
        "mapping_status": "UNMATCHED",
        "best_score": 0.0,
        "best_LAMAS_id": None,
        "best_LAMAS_name": None,
        "ai_suggested_LAMAS_id": None,
        "ai_confidence": None,
        "ai_reasoning": reason,
        "ai_method": None,
        "final_LAMAS_id": None,
    }
    osm_cols = ["osm_id", "osm_name", "normalized_name", "city", "geometry"]
    if "osm_name_original" in osm_gdf.columns:
        osm_cols.insert(2, "osm_name_original")
    if "name:he" in osm_gdf.columns:
        osm_cols.insert(2, "name:he")
    diagnostic_df = osm_gdf[[column for column in osm_cols if column in osm_gdf.columns]].copy()
    for column, value in marked_diagnostic_cols.items():
        diagnostic_df[column] = value
    _save_step(diagnostic_df, "diagnostic_report", file_identifier, save_intermediates)

    named_osm_count = int(diagnostic_df["normalized_name"].nunique())
    unmatched_names = sorted(diagnostic_df["normalized_name"].dropna().unique().tolist())
    logger.info(
        "PIPELINE COMPLETED | No street-level LAMAS records for %s — nothing to "
        "match (%d named OSM streets remain unmatched by design)",
        city_label,
        named_osm_count,
    )

    diagnostics = {
        "total_osm_streets": named_osm_count,
        "total_lamas_streets": 0,
        "confident_matches": 0,
        "ai_proposed_matches": 0,
        "ai_resolved_matches": 0,
        "geographic_review_matches": 0,
        "total_matched": 0,
        "unmatched_osm_streets": named_osm_count,
        "unmatched_osm_percentage": f"{100.0 if named_osm_count else 0.0:.1f}%",
        "unmatched_osm_street_names": unmatched_names,
        "unmatched_lamas_count": 0,
        "unmatched_lamas_percentage": "0.0%",
        "unmatched_lamas_street_names": [],
        "pipeline_status": PipelineStatus.NO_LAMAS_STREETS.value,
        "no_lamas_reason": reason,
    }

    if not skip_html:
        # Intentionally do NOT render an HTML page here. With no CBS street
        # records there is nothing to match against, so a page would only show a
        # misleading 0% coverage. These settlements are reported via
        # ``build_no_results`` (category "no_lamas_streets" / "single_street")
        # and — for the single-name case — get a dedicated info page from
        # ``generate_single_street_pages``.
        logger.info(
            "Skipping HTML page for %s: no CBS street records to map.", city_label
        )

    # Remove any stale street page left over from a previous run, so this
    # settlement does not linger in the dashboard as a misleading 0% page
    # (the single-name case gets a dedicated info page from
    # ``generate_single_street_pages`` instead).
    try:
        stale = os.path.join("HTML", f"{safe_place_name(city_label)}_roads.html")
        if os.path.exists(stale):
            os.remove(stale)
            logger.info("Removed stale page %s", stale)
    except Exception as exc:  # cleanup must never fail the run
        logger.warning("Could not remove stale page for %s: %s", city_label, exc)

    return PipelineStatus.NO_LAMAS_STREETS


def run_pipeline(
    place: str | dict | None = None,
    force_refresh: bool = False,
    use_ai: bool = False,
    skip_html: bool = False,
    output_name: str | None = None,
    use_pbf: bool = True,
    lamas_df: pd.DataFrame | None = None,
    lamas_city: str | None = None,
    save_intermediates: bool = True,
    confident_threshold: float = DEFAULT_CONFIDENT_THRESHOLD,
    ai_backend: str = "auto",
    ai_model: str | None = None,
) -> PipelineStatus:
    """Run the street-mapping pipeline for a settlement."""
    _setup_logging()
    logger.info("--- Starting Street Mapping Orchestrator ---")

    if not use_ai:
        logger.info("AI resolution is disabled for this run (use_ai=False).")

    try:
        place_obj_for_osm = place
        if isinstance(place, dict):
            chosen_place_str = place.get("display_name", "unknown_place")
        elif isinstance(place, str):
            chosen_place_str = place
            lower_place = chosen_place_str.lower()
            if (
                "israel" not in lower_place
                and "palestine" not in lower_place
                and "ישראל" not in chosen_place_str
                and "פלסטין" not in chosen_place_str
            ):
                chosen_place_str = f"{chosen_place_str}, Israel"
            place_obj_for_osm = chosen_place_str
        else:
            chosen_place_str = OSM_PLACE_NAME or "Tel Aviv-Yafo, Israel"
            place_obj_for_osm = chosen_place_str

        logger.info("Using place: %s", chosen_place_str)
        file_identifier = output_name if output_name else chosen_place_str

        LAMAS_df = (
            lamas_df.copy()
            if lamas_df is not None
            else load_or_fetch_LAMAS(force_refresh=force_refresh)
        )
        osm_gdf = load_or_fetch_osm(
            place_obj_for_osm, force_refresh=force_refresh, use_pbf=use_pbf
        )

        if osm_gdf is None or len(osm_gdf) == 0:
            logger.warning("No OSM data available for %s", chosen_place_str)
            return PipelineStatus.NO_DATA

        # Captured before any transformation: when each source was obtained.
        source_provenance = {
            "osm": dict(osm_gdf.attrs.get("provenance") or {}) or None,
            "lamas": dict(LAMAS_df.attrs.get("provenance") or {}) or None,
        }

        try:
            total_bounds = osm_gdf.total_bounds
            avg_lon = (total_bounds[0] + total_bounds[2]) / 2
            avg_lat = (total_bounds[1] + total_bounds[3]) / 2
            min_lat, min_lon, max_lat, max_lon = ISRAEL_BOUNDS
            if not (min_lat <= avg_lat <= max_lat and min_lon <= avg_lon <= max_lon):
                logger.error(
                    "Downloaded data appears outside Israel/Palestine "
                    "(avg lat=%.4f, lon=%.4f). Check settlement name.",
                    avg_lat,
                    avg_lon,
                )
                return PipelineStatus.FAILURE
        except Exception as e:
            logger.warning("Could not validate geographic location: %s", e)

        if len(osm_gdf) > 50000:
            logger.warning(
                "Downloaded %s street segments — suspiciously large for a single city.",
                len(osm_gdf),
            )

        if "city" not in osm_gdf.columns:
            osm_gdf["city"] = chosen_place_str.split(",")[0].strip()

        if "city" in LAMAS_df.columns:
            LAMAS_df["city"] = normalize_city(LAMAS_df["city"])
        osm_gdf["city"] = normalize_city(osm_gdf["city"])
        if lamas_city:
            osm_gdf["city"] = lamas_city
        LAMAS_df["city_key"] = city_key(LAMAS_df["city"])
        LAMAS_df["city_identity_key"] = LAMAS_df["city"].map(settlement_identity_key)
        target_city_raw = lamas_city or osm_gdf["city"].iloc[0]
        target_city_key = canonical_key(target_city_raw)
        target_city_identity_key = settlement_identity_key(target_city_raw)

        _save_step(LAMAS_df, "step1_lamas_raw", file_identifier, save_intermediates)
        _save_step(osm_gdf, "step1_osm_raw", file_identifier, save_intermediates)

    except Exception as e:
        logger.error("FATAL ERROR during Data Acquisition: %s", e)
        logger.debug(traceback.format_exc())
        return PipelineStatus.FAILURE

    logger.info("[Step 2/5] Normalizing street names...")
    LAMAS_df["normalized_name"] = LAMAS_df["LAMAS_name"].apply(normalize_street_name)
    osm_gdf["normalized_name"] = osm_gdf["osm_name"].apply(normalize_street_name)

    if "osm_name_original" in osm_gdf.columns:
        osm_gdf["normalized_original"] = osm_gdf["osm_name_original"].apply(
            normalize_street_name
        )
    if "name:he" in osm_gdf.columns:
        osm_gdf["normalized_name:he"] = osm_gdf["name:he"].apply(normalize_street_name)

    LAMAS_df = LAMAS_df.dropna(subset=["normalized_name"]).copy()
    osm_gdf = osm_gdf.dropna(subset=["normalized_name"]).copy()

    if osm_gdf.empty:
        logger.warning(
            "OSM data for %s has no named streets after normalization.",
            chosen_place_str,
        )
        return PipelineStatus.NO_DATA

    _save_step(LAMAS_df, "step2_lamas_normalized", file_identifier, save_intermediates)
    _save_step(osm_gdf, "step2_osm_normalized", file_identifier, save_intermediates)

    logger.info("[Step 3/5] Running Fuzzy Matching (threshold=%.1f%%)...", confident_threshold)
    osm_city_label = osm_gdf["city"].iloc[0]
    lamas_city_df = _filter_lamas_by_city(
        LAMAS_df, target_city_key, target_identity_key=target_city_identity_key
    ).copy()
    settlement_record_mask = lamas_city_df["LAMAS_name"].map(
        lambda name: bool(target_city_key)
        and pd.notna(name)
        and canonical_key(name) == target_city_key
    )
    settlement_lamas_ids = set(
        lamas_city_df.loc[settlement_record_mask, "LAMAS_id"].astype(str)
    )
    lamas_street_df = lamas_city_df[
        ~lamas_city_df["LAMAS_id"].astype(str).isin(settlement_lamas_ids)
    ]

    if lamas_street_df.empty:
        # Nothing street-level to match against: the settlement is either
        # absent from LAMAS or present only as its own-name pseudo-record.
        # Running the fuzzy/AI stages would just report a misleading
        # "100% unmatched", so bail out with a dedicated status instead.
        if lamas_city_df.empty:
            no_lamas_reason = (
                f"LAMAS contains no records for settlement '{osm_city_label}'"
            )
        else:
            no_lamas_reason = (
                f"LAMAS contains only the settlement-name record for "
                f"'{osm_city_label}' (no street-level records)"
            )
        logger.warning("%s. Nothing to match against.", no_lamas_reason)
        return _finalize_no_lamas_streets(
            osm_gdf,
            osm_city_label,
            file_identifier,
            source_provenance,
            no_lamas_reason,
            save_intermediates,
            skip_html,
        )

    logger.info(
        "Matched '%s' to %d LAMAS street records (%d street-level, canonical city key '%s')",
        osm_city_label,
        len(lamas_city_df),
        len(lamas_street_df),
        target_city_key,
    )

    candidates_df = find_fuzzy_candidates(
        osm_gdf, lamas_city_df, confident_threshold=confident_threshold
    )
    _save_step(candidates_df, "step3_candidates", file_identifier, save_intermediates)

    logger.info(
        "[Step 4/5] (Optional) Running AI street matching (%s) for <%.0f%% streets...",
        ai_backend,
        confident_threshold,
    )
    ai_results: list[dict] = []
    lamas_aliases = build_lamas_aliases(lamas_city_df)
    laya_lamas_aliases = build_lamas_aliases(lamas_street_df)
    lamas_name_by_id = {lid: names[0] for lid, names in lamas_aliases.items() if names}

    if use_ai and laya_lamas_aliases:
        osm_gdf_in_city = osm_gdf[osm_gdf["city"] == osm_city_label]
        unmatched_rows = candidates_df[
            (candidates_df["status"] != "CONFIDENT")
            & (candidates_df["osm_id"].isin(osm_gdf_in_city["osm_id"]))
        ].merge(osm_gdf_in_city[["osm_id", "normalized_name"]], on="osm_id")
        unique_unmatched = unmatched_rows.dropna(
            subset=["normalized_name"]
        ).drop_duplicates(subset=["normalized_name"])
        # Every unmatched street goes to AI: the hardest cases are exactly
        # the ones the fuzzy stage cannot propose a usable candidate for, so
        # pre-filtering by similarity would hide them from the AI forever.
        # Precision is protected by the output gates in _finalize_mappings.
        unmatched_names = [
            row.normalized_name
            for row in unique_unmatched.itertuples(index=False)
        ]
        preferred_ids = {
            str(row.normalized_name): _clean_id(getattr(row, "best_LAMAS_id", ""))
            for row in unique_unmatched.itertuples(index=False)
            if _clean_id(getattr(row, "best_LAMAS_id", ""))
        }

        if unmatched_names:
            try:
                resolver = _get_ai_resolver(backend=ai_backend, model=ai_model)
            except Exception as exc:
                logger.warning("Failed to initialize AI resolver (%s): %s", ai_backend, exc)
                resolver = None

            if resolver is not None:
                logger.info(
                    "Sending %d unmatched OSM street names (%d named streets in city) "
                    "and %d city LAMAS street ids to AI (%s)...",
                    len(unmatched_names),
                    int(osm_gdf_in_city["normalized_name"].nunique()),
                    len(laya_lamas_aliases),
                    type(resolver).__name__,
                )
                try:
                    sig = inspect.signature(resolver.resolve_city)
                    supports_pref = "preferred_ids" in sig.parameters
                except (TypeError, ValueError):
                    supports_pref = False
                if supports_pref:
                    city_decisions = resolver.resolve_city(
                        osm_city_label,
                        unmatched_names,
                        laya_lamas_aliases,
                        preferred_ids=preferred_ids,
                    )
                else:
                    city_decisions = resolver.resolve_city(
                        osm_city_label, unmatched_names, laya_lamas_aliases
                    )
                valid_ids = set(laya_lamas_aliases)
                for row in unmatched_rows.itertuples(index=False):
                    decision = city_decisions.get(row.normalized_name)
                    if not decision:
                        continue
                    lamas_id = decision.get("lamas_id")
                    reasoning = decision.get("reasoning", "")
                    if lamas_id is not None:
                        lamas_id = str(lamas_id).strip()
                        if lamas_id.lower() in {"", "none", "null", "nan"}:
                            lamas_id = None
                        elif lamas_id not in valid_ids:
                            logger.warning(
                                "AI returned LAMAS ID %s not in city '%s'; discarding.",
                                lamas_id,
                                osm_city_label,
                            )
                            reasoning = "AI returned an ID outside the city LAMAS list"
                            lamas_id = None
                    ai_results.append(
                        {
                            "osm_id": row.osm_id,
                            "ai_suggested_LAMAS_id": lamas_id,
                            "ai_confidence": decision.get("confidence"),
                            "ai_reasoning": reasoning,
                            "ai_method": decision.get("method", "local_laya"),
                        }
                    )

    ai_decisions_df = pd.DataFrame(
        ai_results,
        columns=[
            "osm_id",
            "ai_suggested_LAMAS_id",
            "ai_confidence",
            "ai_reasoning",
            "ai_method",
        ],
    )
    _save_step(ai_decisions_df, "step4_ai_decisions", file_identifier, save_intermediates)

    logger.info("[Step 5/5] Finalizing diagnostic table and outputs...")
    osm_names_by_id = dict(
        zip(osm_gdf["osm_id"].astype(str), osm_gdf["normalized_name"].fillna("").astype(str))
    )
    merged_df, final_mapping_df = _finalize_mappings(
        candidates_df,
        ai_decisions_df,
        lamas_name_by_id=lamas_name_by_id,
        city_name=osm_city_label,
        osm_names_by_id=osm_names_by_id,
        lamas_aliases_by_id=lamas_aliases,
    )
    merged_df["osm_id"] = merged_df["osm_id"].astype(str)
    osm_gdf["osm_id"] = osm_gdf["osm_id"].astype(str)

    diagnostic_cols = [
        "osm_id",
        "status",
        "mapping_status",
        "best_score",
        "best_LAMAS_id",
        "best_LAMAS_name",
        "ai_suggested_LAMAS_id",
        "ai_confidence",
        "ai_reasoning",
        "ai_method",
    ]
    osm_cols_for_diag = ["osm_id", "osm_name", "normalized_name", "city", "geometry"]
    if "osm_name_original" in osm_gdf.columns:
        osm_cols_for_diag.insert(2, "osm_name_original")
    if "name:he" in osm_gdf.columns:
        osm_cols_for_diag.insert(2, "name:he")

    diagnostic_df_full = osm_gdf[osm_cols_for_diag].merge(
        merged_df[diagnostic_cols], on="osm_id", how="left"
    )
    diagnostic_df_full = diagnostic_df_full.merge(
        final_mapping_df[["osm_id", "final_LAMAS_id"]], on="osm_id", how="left"
    )

    metadata_columns = [
        column
        for column in (
            "LAMAS_key",
            "city_code",
            "official_code",
            "street_code",
            "street_name_status",
            "is_official",
            "id_source",
        )
        if column in lamas_city_df.columns
    ]
    if "LAMAS_id" in lamas_city_df.columns and metadata_columns:
        meta = lamas_city_df.drop_duplicates(subset=["LAMAS_id"])[
            ["LAMAS_id", *metadata_columns]
        ].copy()
        meta["LAMAS_id"] = meta["LAMAS_id"].astype("string")
        meta = meta.rename(
            columns={
                "LAMAS_id": "final_LAMAS_id",
                "LAMAS_key": "final_LAMAS_key",
                "city_code": "final_city_code",
                "official_code": "final_official_code",
                "street_code": "final_street_code",
                "street_name_status": "final_street_name_status",
                "is_official": "final_is_official",
                "id_source": "final_id_source",
            }
        )
        diagnostic_df_full["final_LAMAS_id"] = diagnostic_df_full[
            "final_LAMAS_id"
        ].astype("string")
        overlap = [
            column
            for column in meta.columns
            if column != "final_LAMAS_id" and column in diagnostic_df_full.columns
        ]
        diagnostic_df_full = diagnostic_df_full.drop(columns=overlap).merge(
            meta, on="final_LAMAS_id", how="left"
        )

    # Optional offline authority metadata: shared commemorative entity, not a
    # replacement for the settlement-scoped physical street / official code.
    from mazal import enrich_from_registry

    diagnostic_df_full = enrich_from_registry(diagnostic_df_full)
    _save_step(diagnostic_df_full, "diagnostic_report", file_identifier, save_intermediates)

    named_osm_count = diagnostic_df_full["normalized_name"].nunique()
    fuzzy_matched_count = diagnostic_df_full.loc[
        diagnostic_df_full["mapping_status"] == "APPROVED_FUZZY", "normalized_name"
    ].nunique()
    ai_matched_count = diagnostic_df_full.loc[
        diagnostic_df_full["mapping_status"] == "APPROVED_AI", "normalized_name"
    ].nunique()
    matched_count = diagnostic_df_full.loc[
        diagnostic_df_full["final_LAMAS_id"].notna(), "normalized_name"
    ].nunique()
    unmatched_count = named_osm_count - matched_count
    logger.info(
        "PIPELINE COMPLETED | Fuzzy (>=%.0f%%): %s | AI: %s | Total matched: %s | Unmatched: %s",
        confident_threshold,
        fuzzy_matched_count,
        ai_matched_count,
        matched_count,
        unmatched_count,
    )

    diagnostics = calculate_diagnostics(lamas_city_df, diagnostic_df_full, osm_gdf)
    logger.info("Diagnostic Summary: %s", diagnostics)

    if not skip_html:
        try:
            from generate_html import create_html_from_gdf

            os.makedirs("HTML", exist_ok=True)
            create_html_from_gdf(
                diagnostic_df_full,
                file_identifier,
                diagnostics,
                provenance=source_provenance,
            )
        except Exception as e:
            logger.warning("Failed to generate HTML visualization: %s", e)

    return PipelineStatus.SUCCESS


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map OSM street names to LAMAS IDs.")
    parser.add_argument("place", nargs="?", help="Settlement name")
    parser.add_argument("--refresh", action="store_true", help="Refresh downloaded data")
    parser.add_argument("--no-ai", action="store_true", help="Disable AI street resolution")
    parser.add_argument(
        "--ai-backend",
        choices=["auto", "openrouter", "local"],
        default="auto",
        help="AI resolver backend (default: auto = OpenRouter with local Laya fallback)",
    )
    parser.add_argument(
        "--ai-model",
        default=None,
        help="Override OpenRouter Decisions model ID (default: perplexity/pplx-decider-v1.3:free)",
    )
    parser.add_argument(
        "--no-pbf",
        action="store_true",
        help="Disable local PBF extraction and use live Overpass API",
    )
    parser.add_argument(
        "--output-name",
        help="Name outputs separately, e.g. for comparing runs",
    )
    parser.add_argument(
        "--lamas-city",
        help="Authoritative LAMAS settlement name used to filter street records",
    )
    parser.add_argument(
        "--no-intermediates",
        action="store_true",
        help="Skip bulky intermediate pickles (diagnostic CSV is still saved)",
    )
    args = parser.parse_args()

    status = run_pipeline(
        place=args.place,
        force_refresh=args.refresh,
        use_ai=not args.no_ai,
        use_pbf=not args.no_pbf,
        output_name=args.output_name,
        lamas_city=args.lamas_city,
        save_intermediates=not args.no_intermediates,
        ai_backend=args.ai_backend,
        ai_model=args.ai_model,
    )
    # "Nothing to map" outcomes completed correctly — not failures.
    sys.exit(0 if status in (PipelineStatus.SUCCESS, *NO_DATA_STATUSES) else 1)
