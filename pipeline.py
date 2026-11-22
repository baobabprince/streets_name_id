"""
Street name mapping pipeline orchestrator.

Maps OpenStreetMap street names to official LAMAS (CBS) street IDs using
normalization, 90% fuzzy matching, and optional whole-city Laya AI resolution
for everything below 90%.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import traceback
from enum import Enum

import pandas as pd
from rapidfuzz import fuzz

from data_loader import (
    city_key,
    load_or_fetch_LAMAS,
    load_or_fetch_osm,
    normalize_city,
    save_intermediate_df,
)
from diagnostics import calculate_diagnostics
from laya_ai_resolver import LayaAIResolver
from normalization import (
    DEFAULT_CONFIDENT_THRESHOLD,
    build_lamas_aliases,
    find_fuzzy_candidates,
    normalize_street_name,
)
from OSM_streets import place_name as OSM_PLACE_NAME
from settlement_names import canonical_key

logger = logging.getLogger(__name__)

ISRAEL_BOUNDS = (29.0, 34.0, 33.7, 36.0)
_LAYA_RESOLVER: LayaAIResolver | None = None
ESSENTIAL_STEPS = {"diagnostic_report"}

# Conservative AI approval guardrails (Option 3: ~95-96% precision, zero magnets)
AI_MIN_CONFIDENCE = 0.70
AI_MIN_CORE_WRATIO = 75.0
AI_MAX_RAW_HITS_PER_ID = 10
AI_MAX_APPROVED_HITS_PER_ID = 2

_STREET_PREFIX_RE = re.compile(
    r"^(?:רחוב|רח|שדרות|שדרת|שד|דרך|דר|סמטת|סמטה|סמ|משעול|כיכר|ככר|שביל|"
    r"מעלה|מעלות|טיילת|גשר|רחבת|נתיב|מדרגות|צומת|מחלף|מעבר(?:\s+לרחוב|\s+ל)?|"
    r"חניון|פארק|גן)\s+"
)
_DIGIT_RE = re.compile(r"\d+")


def _save_step(
    df: pd.DataFrame, step_name: str, identifier: str, save_intermediates: bool
) -> None:
    """Skip bulky intermediate pickles unless explicitly requested."""
    if not save_intermediates and step_name not in ESSENTIAL_STEPS:
        return
    save_intermediate_df(df, step_name, identifier)


def _filter_lamas_by_city(LAMAS_df: pd.DataFrame, target_key: str) -> pd.DataFrame:
    """Join LAMAS streets to a settlement using canonical city keys."""
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
    FAILURE = "FAILURE"


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


def _strip_street_prefix(name: str) -> str:
    """Strip leading generic Hebrew street-type prefixes for core-name similarity."""
    text = (name or "").strip()
    prev = None
    while prev != text:
        prev = text
        text = _STREET_PREFIX_RE.sub("", text).strip()
    return text


def _digits_compatible(osm_name: str, lamas_name: str) -> bool:
    """Reject numeric mismatches (e.g. 'גדוד 21' vs 'גדוד 22' or '5083' vs 'רח 7083')."""
    da = _DIGIT_RE.findall(osm_name or "")
    db = _DIGIT_RE.findall(lamas_name or "")
    if not da and not db:
        return True
    return da == db


def _core_wratio(osm_name: str, lamas_name: str) -> float:
    """Compute WRatio between normalized prefix-stripped OSM and LAMAS names."""
    norm_osm = normalize_street_name(osm_name) or ""
    norm_lamas = normalize_street_name(lamas_name) or ""
    core_osm = _strip_street_prefix(norm_osm)
    core_lamas = _strip_street_prefix(norm_lamas)
    if not core_osm or not core_lamas:
        return 0.0
    return float(fuzz.WRatio(core_osm, core_lamas))


def _is_eligible_for_option3_ai(
    osm_name: str,
    best_lamas_id: object,
    best_lamas_name: object,
    target_city_key: str,
    min_core_wratio: float = AI_MIN_CORE_WRATIO,
) -> bool:
    """True only when a <90% street's top-1 fuzzy candidate could pass Option 3."""
    if not _clean_id(best_lamas_id):
        return False
    lamas_label = "" if pd.isna(best_lamas_name) else str(best_lamas_name)
    if not lamas_label.strip():
        return False
    if target_city_key and canonical_key(lamas_label) == target_city_key:
        return False
    if not _digits_compatible(osm_name, lamas_label):
        return False
    return _core_wratio(osm_name, lamas_label) >= min_core_wratio


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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Approve >=90% fuzzy matches and strictly verified Laya AI matches.

    An AI suggestion is approved into ``final_LAMAS_id`` only if ALL hold:
    1. Laya agreed with the top-1 fuzzy candidate (``ai_suggested_LAMAS_id == best_LAMAS_id``).
    2. ``ai_confidence >= min_ai_confidence`` (default 0.70).
    3. Prefix-stripped ``WRatio >= min_core_wratio`` (default 75.0).
    4. Any digits in the OSM and LAMAS names match identically.
    5. The matched LAMAS street name is not the settlement's own name.
    6. Magnet protection (counted per unique normalized street name):
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
    lamas_lookup = {_clean_id(k): v for k, v in (lamas_name_by_id or {}).items()}

    candidate_ai = pd.Series(False, index=merged.index, dtype=bool)
    reject_reasons: dict[int, str] = {}

    for idx in merged.index[has_ai]:
        ai_id = ai_ids.at[idx]
        best_id = best_ids.at[idx]
        conf = float(confs.at[idx])
        osm_label = street_keys.at[idx]
        matched_lamas_name = lamas_lookup.get(ai_id) or str(merged.at[idx, "best_LAMAS_name"] or "")

        if not best_id or ai_id != best_id:
            reject_reasons[idx] = "disagreed_with_fuzzy_top1"
        elif conf < min_ai_confidence:
            reject_reasons[idx] = f"low_confidence({conf:.2f}<{min_ai_confidence:.2f})"
        elif raw_hits_by_id.get(ai_id, 0) > max_raw_hits_per_id:
            reject_reasons[idx] = f"magnet_raw_hits({raw_hits_by_id[ai_id]}>{max_raw_hits_per_id})"
        elif target_city_key and canonical_key(matched_lamas_name) == target_city_key:
            reject_reasons[idx] = "settlement_name_street"
        elif osm_label and not _digits_compatible(osm_label, matched_lamas_name):
            reject_reasons[idx] = "digit_mismatch"
        elif osm_label and _core_wratio(osm_label, matched_lamas_name) < min_core_wratio:
            score = _core_wratio(osm_label, matched_lamas_name)
            reject_reasons[idx] = f"low_core_wratio({score:.1f}<{min_core_wratio:.1f})"
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
        target_city_key = canonical_key(lamas_city or osm_gdf["city"].iloc[0])

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
    lamas_city_df = _filter_lamas_by_city(LAMAS_df, target_city_key).copy()
    if lamas_city_df.empty:
        logger.warning(
            "No LAMAS data found for city '%s'. Fuzzy matching will yield MISSING.",
            osm_city_label,
        )
    else:
        logger.info(
            "Matched '%s' to %d LAMAS street records (canonical city key '%s')",
            osm_city_label,
            len(lamas_city_df),
            target_city_key,
        )

    candidates_df = find_fuzzy_candidates(
        osm_gdf, lamas_city_df, confident_threshold=confident_threshold
    )
    _save_step(candidates_df, "step3_candidates", file_identifier, save_intermediates)

    logger.info("[Step 4/5] (Optional) Running whole-city Laya AI matching for <%.0f%% streets...", confident_threshold)
    ai_results: list[dict] = []
    lamas_aliases = build_lamas_aliases(lamas_city_df)
    lamas_name_by_id = {lid: names[0] for lid, names in lamas_aliases.items() if names}

    if use_ai and lamas_aliases:
        osm_gdf_in_city = osm_gdf[osm_gdf["city"] == osm_city_label]
        unmatched_rows = candidates_df[
            (candidates_df["status"] != "CONFIDENT")
            & (candidates_df["osm_id"].isin(osm_gdf_in_city["osm_id"]))
        ].merge(osm_gdf_in_city[["osm_id", "normalized_name"]], on="osm_id")
        unique_unmatched = unmatched_rows.dropna(
            subset=["normalized_name"]
        ).drop_duplicates(subset=["normalized_name"])
        unmatched_names = [
            row.normalized_name
            for row in unique_unmatched.itertuples(index=False)
            if _is_eligible_for_option3_ai(
                row.normalized_name,
                row.best_LAMAS_id,
                row.best_LAMAS_name,
                target_city_key,
            )
        ]

        if unmatched_names:
            try:
                resolver = _get_laya_resolver()
            except Exception as exc:
                logger.warning("Failed to initialize Laya AI: %s", exc)
                resolver = None

            if resolver is not None:
                logger.info(
                    "Sending %d unmatched OSM streets and %d city LAMAS streets to Laya...",
                    len(unmatched_names),
                    len(lamas_aliases),
                )
                city_decisions = resolver.resolve_city(
                    osm_city_label, unmatched_names, lamas_aliases
                )
                valid_ids = set(lamas_aliases)
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
                                "Laya returned LAMAS ID %s not in city '%s'; discarding.",
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
        "PIPELINE COMPLETED | Fuzzy (>=%.0f%%): %s | Laya AI: %s | Total matched: %s | Unmatched: %s",
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
            create_html_from_gdf(diagnostic_df_full, file_identifier, diagnostics)
        except Exception as e:
            logger.warning("Failed to generate HTML visualization: %s", e)

    return PipelineStatus.SUCCESS


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Map OSM street names to LAMAS IDs.")
    parser.add_argument("place", nargs="?", help="Settlement name")
    parser.add_argument("--refresh", action="store_true", help="Refresh downloaded data")
    parser.add_argument("--no-ai", action="store_true", help="Disable Laya AI resolution")
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
    )
    sys.exit(0 if status == PipelineStatus.SUCCESS else 1)
