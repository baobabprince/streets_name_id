"""
Street name mapping pipeline orchestrator.

Maps OpenStreetMap street names to official LAMAS (CBS) street IDs using
normalization, fuzzy matching, topology, and optional AI resolution.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from enum import Enum

import pandas as pd

from data_loader import (
    city_key,
    load_or_fetch_LAMAS,
    load_or_fetch_osm,
    normalize_city,
    save_intermediate_df,
)
from diagnostics import calculate_diagnostics
from gemini_client import API_KEY, get_ai_resolution
from geospatial_validation import validate_candidate_geography
from laya_ai_resolver import LayaAIResolver
from local_ai_resolver import LocalAIResolver
from map_of_adjacents import build_adjacency_map
from normalization import (
    find_fuzzy_candidates,
    normalize_street_name,
    select_ai_candidates,
)
from OSM_streets import place_name as OSM_PLACE_NAME
from settlement_names import canonical_key

logger = logging.getLogger(__name__)

ISRAEL_BOUNDS = (29.0, 34.0, 33.7, 36.0)
_LOCAL_RESOLVER_CACHE: dict[str, LayaAIResolver | LocalAIResolver] = {}
# CSV audit outputs are always kept; bulky pickle intermediates are optional.
ESSENTIAL_STEPS = {"diagnostic_report", "step4_candidate_scores"}


def _save_step(
    df: pd.DataFrame, step_name: str, identifier: str, save_intermediates: bool
) -> None:
    """Skip bulky intermediate pickles unless they were explicitly requested."""
    if not save_intermediates and step_name not in ESSENTIAL_STEPS:
        return
    save_intermediate_df(df, step_name, identifier)


def _filter_lamas_by_city(LAMAS_df: pd.DataFrame, target_key: str) -> pd.DataFrame:
    """Join LAMAS streets to a settlement using canonical city keys.

    OSM and LAMAS spell city names differently; falling back to a tight fuzzy
    key match prevents a whole city from silently producing zero LAMAS rows.
    """
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
                target_key, best[0], best[1],
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


def _get_local_resolver(local_ai_model: str):
    """Reuse the initialized model across settlements handled by this process."""
    resolver = _LOCAL_RESOLVER_CACHE.get(local_ai_model)
    if resolver is not None:
        return resolver

    resolver_class = LayaAIResolver if local_ai_model == "laya" else LocalAIResolver
    resolver = resolver_class()
    if resolver.is_available():
        _LOCAL_RESOLVER_CACHE[local_ai_model] = resolver
        return resolver
    return None


def _build_candidate_score_report(
    candidates_df: pd.DataFrame, osm_gdf: pd.DataFrame
) -> pd.DataFrame:
    """Flatten every above-threshold fuzzy candidate for quality review."""
    osm_columns = ["osm_id", "osm_name", "normalized_name", "city"]
    available_osm_columns = [column for column in osm_columns if column in osm_gdf]
    osm_by_id = {
        str(row["osm_id"]): row
        for row in osm_gdf[available_osm_columns].to_dict("records")
    }

    records = []
    for row in candidates_df.to_dict("records"):
        osm_info = osm_by_id.get(str(row["osm_id"]), {})
        candidate_details = row.get("candidate_details")
        if not isinstance(candidate_details, list):
            continue
        for rank, candidate in enumerate(candidate_details, start=1):
            records.append(
                {
                    "osm_id": row["osm_id"],
                    "osm_name": osm_info.get("osm_name"),
                    "normalized_name": osm_info.get("normalized_name"),
                    "city": osm_info.get("city"),
                    "fuzzy_status": row.get("status"),
                    "best_score": row.get("best_score"),
                    "candidate_rank": rank,
                    "lamas_id": candidate["lamas_id"],
                    "lamas_name": candidate["lamas_name"],
                    "matched_alias": candidate.get("matched_alias"),
                    "fuzzy_score": candidate["score"],
                    "aliases_json": json.dumps(
                        candidate.get("aliases", []), ensure_ascii=False
                    ),
                    "geographic_validation": candidate.get(
                        "geographic_validation"
                    ),
                    "geographic_distance_m": candidate.get(
                        "geographic_distance_m"
                    ),
                }
            )

    columns = [
        "osm_id",
        "osm_name",
        "normalized_name",
        "city",
        "fuzzy_status",
        "best_score",
        "candidate_rank",
        "lamas_id",
        "lamas_name",
        "matched_alias",
        "fuzzy_score",
        "aliases_json",
        "geographic_validation",
        "geographic_distance_m",
    ]
    return pd.DataFrame(records, columns=columns)


def _merge_ai_suggestions(
    candidates_df: pd.DataFrame, ai_decisions_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep AI outputs as proposals and create approved mappings from fuzzy matches only."""
    merged = candidates_df.merge(ai_decisions_df, on="osm_id", how="left")
    merged["osm_id"] = merged["osm_id"].astype(str)
    has_ai_suggestion = merged["ai_suggested_LAMAS_id"].notna()
    has_geographic_conflict = (
        merged["geographic_validation"] == "DISTANT_FROM_EXACT_REFERENCE"
    )
    merged["mapping_status"] = "UNMATCHED"
    merged.loc[merged["status"] == "NEEDS_AI", "mapping_status"] = "NEEDS_REVIEW"
    merged.loc[has_ai_suggestion, "mapping_status"] = "AI_PROPOSED"
    merged.loc[has_geographic_conflict, "mapping_status"] = "GEOGRAPHIC_REVIEW"
    merged.loc[
        (merged["status"] == "CONFIDENT") & ~has_geographic_conflict,
        "mapping_status",
    ] = "APPROVED_FUZZY"

    approved = merged[merged["mapping_status"] == "APPROVED_FUZZY"].copy()
    approved["final_LAMAS_id"] = approved["best_LAMAS_id"].astype(str)
    return merged, approved


def run_pipeline(
    place: str | dict | None = None,
    force_refresh: bool = False,
    use_ai: bool = False,
    use_local_ai: bool = True,
    local_ai_model: str = "qwen",
    use_gemini: bool = True,
    skip_html: bool = False,
    output_name: str | None = None,
    use_pbf: bool = True,
    lamas_df: pd.DataFrame | None = None,
    lamas_city: str | None = None,
    save_intermediates: bool = True,
) -> PipelineStatus:
    """Run the full street-mapping pipeline for a settlement."""
    _setup_logging()
    logger.info("--- Starting Street Mapping Orchestrator ---")

    if local_ai_model not in {"qwen", "laya"}:
        raise ValueError("local_ai_model must be 'qwen' or 'laya'")

    if use_gemini and not API_KEY:
        logger.warning(
            "GEMINI_API_KEY environment variable not set. Cloud AI resolution will be skipped."
        )
    elif not use_gemini:
        logger.info("Gemini fallback is disabled for this run.")
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

        # A batch already fetched the national LAMAS table once; reuse it
        # instead of re-reading the pickle for every settlement.
        LAMAS_df = (
            lamas_df.copy()
            if lamas_df is not None
            else load_or_fetch_LAMAS(force_refresh=force_refresh)
        )
        osm_gdf = load_or_fetch_osm(
            place_obj_for_osm, force_refresh=force_refresh, use_pbf=use_pbf
        )

        if osm_gdf is None:
            logger.warning("No OSM data available for %s", chosen_place_str)
            return PipelineStatus.NO_DATA

        if len(osm_gdf) == 0:
            logger.warning("OSM data for %s is empty (no streets found)", chosen_place_str)
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
            city_label = chosen_place_str.split(",")[0].strip()
            osm_gdf["city"] = city_label

        if "city" in LAMAS_df.columns:
            LAMAS_df["city"] = normalize_city(LAMAS_df["city"])
        osm_gdf["city"] = normalize_city(osm_gdf["city"])
        if lamas_city:
            # Keep the authoritative LAMAS settlement label in every output.
            osm_gdf["city"] = lamas_city
        LAMAS_df["city_key"] = city_key(LAMAS_df["city"])
        target_city_key = canonical_key(lamas_city or osm_gdf["city"].iloc[0])

        _save_step(LAMAS_df, "step1_lamas_raw", file_identifier, save_intermediates)
        _save_step(osm_gdf, "step1_osm_raw", file_identifier, save_intermediates)

    except Exception as e:
        logger.error("FATAL ERROR during Data Acquisition: %s", e)
        logger.debug(traceback.format_exc())
        return PipelineStatus.FAILURE

    logger.info("[Step 2/7] Normalizing street names...")
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

    _save_step(LAMAS_df, "step2_lamas_normalized", file_identifier, save_intermediates)
    _save_step(osm_gdf, "step2_osm_normalized", file_identifier, save_intermediates)

    logger.info("[Step 3/7] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    logger.info("[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
    osm_city_label = osm_gdf["city"].iloc[0]
    logger.info("Filtering LAMAS data for city: '%s'", osm_city_label)

    lamas_city_df = _filter_lamas_by_city(LAMAS_df, target_city_key).copy()
    if lamas_city_df.empty:
        logger.warning(
            "No LAMAS data found for city '%s'. Fuzzy matching will yield MISSING.",
            osm_city_label,
        )
    else:
        logger.info(
            "Matched '%s' to %d LAMAS street records (canonical city key '%s')",
            osm_city_label, len(lamas_city_df), target_city_key,
        )

    candidates_df = find_fuzzy_candidates(osm_gdf, lamas_city_df)
    candidates_df = validate_candidate_geography(
        osm_gdf, candidates_df, map_of_adjacents
    )
    _save_step(candidates_df, "step4_candidates", file_identifier, save_intermediates)
    candidate_scores_df = _build_candidate_score_report(candidates_df, osm_gdf)
    _save_step(
        candidate_scores_df, "step4_candidate_scores", file_identifier, save_intermediates
    )

    logger.info("[Step 5/7] (Optional) Invoking AI for ambiguous cases...")
    ai_results: list[dict] = []

    if use_ai:
        local_resolver = None
        if use_local_ai:
            try:
                logger.info("Initializing local %s AI resolver...", local_ai_model)
                local_resolver = _get_local_resolver(local_ai_model)
                if local_resolver is None:
                    logger.info(
                        "Local AI unavailable; the configured cloud fallback will be used if enabled."
                    )
                else:
                    logger.info("Local AI initialized successfully.")
            except Exception as e:
                logger.warning("Failed to initialize Local AI: %s", e)
                local_resolver = None

        osm_gdf_in_city = osm_gdf[osm_gdf["city"] == osm_city_label]

        ai_candidates_to_process = candidates_df[
            (
                (candidates_df["status"] == "NEEDS_AI")
                | (
                    candidates_df["geographic_validation"]
                    == "DISTANT_FROM_EXACT_REFERENCE"
                )
            )
            & (candidates_df["osm_id"].isin(osm_gdf_in_city["osm_id"]))
        ].merge(osm_gdf_in_city[["osm_id", "normalized_name"]], on="osm_id")

        unique_ai_streets = ai_candidates_to_process.drop_duplicates(
            subset=["normalized_name"]
        )
        osm_ids_by_name = (
            osm_gdf_in_city.groupby("normalized_name", sort=False)["osm_id"]
            .agg(list)
            .to_dict()
        )
        osm_name_by_id = dict(
            zip(osm_gdf_in_city["osm_id"], osm_gdf_in_city["normalized_name"])
        )
        osm_name_order = {
            name: order
            for order, name in enumerate(
                osm_gdf_in_city["normalized_name"].dropna().drop_duplicates()
            )
        }
        logger.info(
            "Found %s unique street names requiring AI resolution.",
            len(unique_ai_streets),
        )

        ai_decision_cache: dict[str, dict] = {}

        for _, row in unique_ai_streets.iterrows():
            normalized_street_name = row["normalized_name"]
            representative_osm_id = row["osm_id"]
            lamas_candidates = select_ai_candidates(
                row["candidate_details"], normalized_street_name
            )

            all_adjacent_ids: set = set()
            for osm_id in osm_ids_by_name.get(normalized_street_name, []):
                all_adjacent_ids.update(map_of_adjacents.get(osm_id, []))

            adjacent_names = sorted(
                {
                    osm_name_by_id[osm_id]
                    for osm_id in all_adjacent_ids
                    if osm_id in osm_name_by_id
                    and pd.notna(osm_name_by_id[osm_id])
                },
                key=osm_name_order.get,
            )

            city_name = osm_gdf_in_city["city"].iloc[0]

            ai_decision_id = None
            confidence = None
            reasoning = ""
            method = "none"

            if local_resolver:
                logger.info(
                    "Consulting Local AI for street: '%s'...", normalized_street_name
                )
                local_result = local_resolver.resolve_street(
                    representative_osm_id,
                    normalized_street_name,
                    city_name,
                    lamas_candidates,
                    adjacent_names[:8],
                )

                ai_decision_id = local_result.get("lamas_id")
                confidence = local_result.get("confidence", 0.0)
                reasoning = local_result.get("reasoning", "")
                method = local_result.get("method", "local_ai")

                if ai_decision_id is None:
                    logger.info(
                        "Local AI found no match for '%s'. Confidence: %s",
                        normalized_street_name,
                        confidence,
                    )
                else:
                    logger.info(
                        "Local AI matched '%s' to %s. Confidence: %s",
                        normalized_street_name,
                        ai_decision_id,
                        confidence,
                    )

            if (
                (ai_decision_id is None or method == "none")
                and use_gemini
                and API_KEY
            ):
                if method.startswith("local_"):
                    logger.info(
                        "Falling back to Gemini API for '%s'...",
                        normalized_street_name,
                    )

                prompt = (
                    f"OSM Street Name: '{normalized_street_name}'. "
                    f"City: '{city_name}'. "
                    f"Adjacent Streets: {', '.join(adjacent_names[:8]) if adjacent_names else 'None'}. "
                    "LAMAS candidates ranked by fuzzy score (at most 8; up to 3 "
                    "relevant aliases each): "
                    f"{json.dumps(lamas_candidates, ensure_ascii=False)}. "
                    "Choose a candidate ID or None. Return a suggestion only; "
                    "the result will not be automatically approved."
                )
                ai_decision_id = get_ai_resolution(prompt, representative_osm_id)
                method = "gemini"
                confidence = None

            if ai_decision_id is not None:
                ai_decision_id = str(ai_decision_id).strip()
                if ai_decision_id.lower() in {"", "none", "null", "nan"}:
                    ai_decision_id = None
                elif ai_decision_id not in {
                    candidate["id"] for candidate in lamas_candidates
                }:
                    logger.warning(
                        "AI returned LAMAS ID %s outside the candidate list for '%s'; "
                        "discarding the suggestion.",
                        ai_decision_id,
                        normalized_street_name,
                    )
                    reasoning = "AI returned an ID outside the candidate list"
                    ai_decision_id = None

            ai_decision_cache[normalized_street_name] = {
                "ai_suggested_LAMAS_id": ai_decision_id,
                "ai_confidence": confidence,
                "ai_reasoning": reasoning,
                "ai_method": method,
            }

        for _, row in ai_candidates_to_process.iterrows():
            street_name = row["normalized_name"]
            cached_result = ai_decision_cache.get(street_name)
            if cached_result:
                ai_results.append({"osm_id": row["osm_id"], **cached_result})

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
    _save_step(ai_decisions_df, "step5_ai_decisions", file_identifier, save_intermediates)

    logger.info("[Step 6/7] Merging results to create final diagnostic table...")

    ai_decisions_merged, final_mapping_df = _merge_ai_suggestions(
        candidates_df, ai_decisions_df
    )
    ai_decisions_merged["osm_id"] = ai_decisions_merged["osm_id"].astype(str)
    osm_gdf["osm_id"] = osm_gdf["osm_id"].astype(str)

    diagnostic_cols = [
        "osm_id",
        "status",
        "mapping_status",
        "best_score",
        "best_LAMAS_id",
        "best_LAMAS_name",
        "all_candidates",
        "geographic_validation",
        "geographic_distance_m",
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
        ai_decisions_merged[diagnostic_cols], on="osm_id", how="left"
    )
    diagnostic_df_full = diagnostic_df_full.merge(
        final_mapping_df[["osm_id", "final_LAMAS_id"]], on="osm_id", how="left"
    )

    # Attach the CBS metadata behind the matched id.  ``official_code`` is only
    # unique inside a city, so ``final_LAMAS_key`` (city_code:official_code) is
    # the unambiguous identifier; ``street_code``/status distinguish the
    # official row from synonym rows.
    metadata_columns = [
        column for column in (
            "LAMAS_key", "city_code", "official_code", "street_code",
            "street_name_status", "is_official", "id_source",
        )
        if column in lamas_city_df.columns
    ]
    if "LAMAS_id" in lamas_city_df.columns and metadata_columns:
        meta = lamas_city_df.drop_duplicates(subset=["LAMAS_id"])[
            ["LAMAS_id", *metadata_columns]
        ].copy()
        meta["LAMAS_id"] = meta["LAMAS_id"].astype("string")
        meta = meta.rename(columns={
            "LAMAS_id": "final_LAMAS_id",
            "LAMAS_key": "final_LAMAS_key",
            "city_code": "final_city_code",
            "official_code": "final_official_code",
            "street_code": "final_street_code",
            "street_name_status": "final_street_name_status",
            "is_official": "final_is_official",
            "id_source": "final_id_source",
        })
        diagnostic_df_full["final_LAMAS_id"] = (
            diagnostic_df_full["final_LAMAS_id"].astype("string")
        )
        overlap = [
            column for column in meta.columns
            if column != "final_LAMAS_id" and column in diagnostic_df_full.columns
        ]
        diagnostic_df_full = diagnostic_df_full.drop(columns=overlap).merge(
            meta, on="final_LAMAS_id", how="left"
        )

    _save_step(diagnostic_df_full, "diagnostic_report", file_identifier, save_intermediates)

    logger.info("PIPELINE COMPLETED")
    named_osm_count = diagnostic_df_full["normalized_name"].nunique()
    matched_count = diagnostic_df_full.loc[
        diagnostic_df_full["final_LAMAS_id"].notna(), "normalized_name"
    ].nunique()
    ai_proposal_count = diagnostic_df_full.loc[
        diagnostic_df_full["ai_suggested_LAMAS_id"].notna(), "normalized_name"
    ].nunique()
    geographic_review_count = diagnostic_df_full.loc[
        diagnostic_df_full["mapping_status"] == "GEOGRAPHIC_REVIEW",
        "normalized_name",
    ].nunique()
    unmatched_count = named_osm_count - matched_count
    logger.info(
        "Approved fuzzy matches: %s | AI proposals (not approved): %s | "
        "Geographic reviews: %s | Without approved mapping: %s",
        matched_count,
        ai_proposal_count,
        geographic_review_count,
        unmatched_count,
    )

    logger.info("[Step 6.5/7] Calculating diagnostic summary...")
    lamas_in_city_df = _filter_lamas_by_city(LAMAS_df, target_city_key)
    diagnostics = calculate_diagnostics(lamas_in_city_df, diagnostic_df_full, osm_gdf)
    logger.info("Diagnostic Summary: %s", diagnostics)

    if not skip_html:
        logger.info("[Step 7/7] Generating HTML visualization...")
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
    parser.add_argument("--no-ai", action="store_true", help="Disable AI resolution")
    parser.add_argument(
        "--no-local-ai", action="store_true", help="Skip local model resolution"
    )
    parser.add_argument(
        "--no-pbf",
        action="store_true",
        help="Disable local PBF extraction and use live Overpass API",
    )
    parser.add_argument(
        "--local-ai-model",
        choices=("qwen", "laya"),
        default="qwen",
        help="Local model to use (default: qwen)",
    )
    parser.add_argument(
        "--no-gemini", action="store_true", help="Disable Gemini cloud fallback"
    )
    parser.add_argument(
        "--output-name",
        help="Name outputs separately, e.g. for comparing model runs",
    )
    parser.add_argument(
        "--lamas-city",
        help="Authoritative LAMAS settlement name used to filter street records",
    )
    parser.add_argument(
        "--no-intermediates",
        action="store_true",
        help="Skip bulky intermediate pickles (CSV audit reports are still saved)",
    )
    args = parser.parse_args()

    status = run_pipeline(
        place=args.place,
        force_refresh=args.refresh,
        use_ai=not args.no_ai,
        use_local_ai=not args.no_local_ai,
        use_pbf=not args.no_pbf,
        local_ai_model=args.local_ai_model,
        use_gemini=not args.no_gemini,
        output_name=args.output_name,
        lamas_city=args.lamas_city,
        save_intermediates=not args.no_intermediates,
    )
    sys.exit(0 if status == PipelineStatus.SUCCESS else 1)
