"""
Street name mapping pipeline orchestrator.

Maps OpenStreetMap street names to official LAMAS (CBS) street IDs using
normalization, fuzzy matching, topology, and optional AI resolution.
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

from data_loader import (
    load_or_fetch_LAMAS,
    load_or_fetch_osm,
    normalize_city,
    save_intermediate_df,
)
from diagnostics import calculate_diagnostics
from gemini_client import API_KEY, get_ai_resolution
from laya_ai_resolver import LayaAIResolver
from local_ai_resolver import LocalAIResolver
from map_of_adjacents import build_adjacency_map
from normalization import (
    build_lamas_aliases,
    find_fuzzy_candidates,
    format_city_lexicon,
    normalize_street_name,
)
from OSM_streets import place_name as OSM_PLACE_NAME

logger = logging.getLogger(__name__)

ISRAEL_BOUNDS = (29.0, 34.0, 33.7, 36.0)


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

        LAMAS_df = load_or_fetch_LAMAS(force_refresh=force_refresh)
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

        save_intermediate_df(LAMAS_df, "step1_lamas_raw", file_identifier)
        save_intermediate_df(osm_gdf, "step1_osm_raw", file_identifier)

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

    LAMAS_df.dropna(subset=["normalized_name"], inplace=True)
    osm_gdf.dropna(subset=["normalized_name"], inplace=True)

    save_intermediate_df(LAMAS_df, "step2_lamas_normalized", file_identifier)
    save_intermediate_df(osm_gdf, "step2_osm_normalized", file_identifier)

    logger.info("[Step 3/7] Building Adjacency Map (Topology)...")
    map_of_adjacents = build_adjacency_map(osm_gdf)

    logger.info("[Step 4/7] Running Fuzzy Matching and Candidate Creation...")
    osm_city_label = osm_gdf["city"].iloc[0]
    logger.info("Filtering LAMAS data for city: '%s'", osm_city_label)

    lamas_city_df = LAMAS_df[LAMAS_df["city"] == osm_city_label].copy()
    if lamas_city_df.empty:
        logger.warning(
            "No LAMAS data found for city '%s'. Fuzzy matching will yield MISSING.",
            osm_city_label,
        )

    aliases_by_id = build_lamas_aliases(lamas_city_df)
    city_lexicon = format_city_lexicon(aliases_by_id)
    logger.info(
        "City LAMAS lexicon: %s unique street IDs for '%s'",
        len(aliases_by_id),
        osm_city_label,
    )

    candidates_df = find_fuzzy_candidates(osm_gdf, lamas_city_df)
    save_intermediate_df(candidates_df, "step4_candidates", file_identifier)

    logger.info("[Step 5/7] (Optional) Invoking AI for ambiguous cases...")
    ai_results: list[dict] = []

    if use_ai:
        local_resolver = None
        if use_local_ai:
            try:
                logger.info("Initializing local %s AI resolver...", local_ai_model)
                resolver_class = (
                    LayaAIResolver if local_ai_model == "laya" else LocalAIResolver
                )
                local_resolver = resolver_class()
                if not local_resolver.is_available():
                    logger.info(
                        "Local AI unavailable; the configured cloud fallback will be used if enabled."
                    )
                    local_resolver = None
                else:
                    logger.info("Local AI initialized successfully.")
            except Exception as e:
                logger.warning("Failed to initialize Local AI: %s", e)
                local_resolver = None

        osm_gdf_in_city = osm_gdf[osm_gdf["city"] == osm_city_label]

        ai_candidates_to_process = candidates_df[
            (candidates_df["status"] == "NEEDS_AI")
            & (candidates_df["osm_id"].isin(osm_gdf_in_city["osm_id"]))
        ].merge(osm_gdf_in_city[["osm_id", "normalized_name"]], on="osm_id")

        unique_ai_streets = ai_candidates_to_process.drop_duplicates(
            subset=["normalized_name"]
        )
        logger.info(
            "Found %s unique street names requiring AI resolution.",
            len(unique_ai_streets),
        )

        ai_decision_cache: dict[str, dict] = {}

        for _, row in unique_ai_streets.iterrows():
            normalized_street_name = row["normalized_name"]
            representative_osm_id = row["osm_id"]

            all_osm_ids_for_street = osm_gdf_in_city[
                osm_gdf_in_city["normalized_name"] == normalized_street_name
            ]["osm_id"].tolist()

            all_adjacent_ids: set = set()
            for osm_id in all_osm_ids_for_street:
                all_adjacent_ids.update(map_of_adjacents.get(osm_id, []))

            adjacent_names = (
                osm_gdf_in_city[osm_gdf_in_city["osm_id"].isin(all_adjacent_ids)][
                    "normalized_name"
                ]
                .unique()
                .tolist()
            )

            lamas_candidates_str = row["all_candidates"]
            city_name = osm_gdf_in_city["city"].iloc[0]

            ai_decision_id = "None"
            confidence = 0.0
            reasoning = ""
            method = "none"

            if local_resolver:
                logger.info(
                    "Consulting Local AI for street: '%s'...", normalized_street_name
                )
                local_lamas_candidates = []
                if pd.notna(lamas_candidates_str):
                    for line in lamas_candidates_str.split("\n"):
                        id_match = re.search(r"ID:\s*(\d+)", line)
                        names_bracket = re.search(r"Names:\s*\[([^\]]+)\]", line)
                        name_match = re.search(r"Name:\s*['\"]([^'\"]+)['\"]", line)
                        score_match = re.search(r"Score:\s*([\d.]+)", line)
                        if not id_match:
                            continue
                        lid = id_match.group(1)
                        if lid in aliases_by_id:
                            names = aliases_by_id[lid]
                        elif names_bracket:
                            names = [
                                n.strip()
                                for n in names_bracket.group(1).split("|")
                                if n.strip()
                            ]
                        elif name_match:
                            names = [name_match.group(1)]
                        else:
                            names = []
                        local_lamas_candidates.append(
                            {
                                "id": lid,
                                "name": names[0] if names else "",
                                "names": names,
                                "score": float(score_match.group(1))
                                if score_match
                                else 0.0,
                            }
                        )

                local_result = local_resolver.resolve_street(
                    representative_osm_id,
                    normalized_street_name,
                    city_name,
                    local_lamas_candidates,
                    adjacent_names,
                    city_lexicon=city_lexicon,
                )

                ai_decision_id = local_result.get("lamas_id")
                confidence = local_result.get("confidence", 0.0)
                reasoning = local_result.get("reasoning", "")
                method = local_result.get("method", "local_ai")

                if str(ai_decision_id) == "None":
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
                (str(ai_decision_id) == "None" or method == "none")
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
                    f"Adjacent Streets: {', '.join(adjacent_names) if adjacent_names else 'None'}. "
                    f"LAMAS Candidates (each ID may list multiple synonym names): {lamas_candidates_str}. "
                    f"Full city LAMAS lexicon (id → all names/synonyms):\n{city_lexicon}\n"
                    f"Choose the best LAMAS ID (number only or 'None'). "
                    f"Match against any synonym of an ID, not only the primary name."
                )
                ai_decision_id = get_ai_resolution(prompt, representative_osm_id)
                method = "gemini"
                confidence = 0.0

            ai_decision_cache[normalized_street_name] = {
                "ai_LAMAS_id": ai_decision_id,
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
        columns=["osm_id", "ai_LAMAS_id", "ai_confidence", "ai_reasoning", "ai_method"],
    )
    save_intermediate_df(ai_decisions_df, "step5_ai_decisions", file_identifier)

    logger.info("[Step 6/7] Merging results to create final diagnostic table...")

    ai_decisions_merged = candidates_df.merge(ai_decisions_df, on="osm_id", how="left")
    ai_decisions_merged["osm_id"] = ai_decisions_merged["osm_id"].astype(str)
    osm_gdf["osm_id"] = osm_gdf["osm_id"].astype(str)

    final_mapping_df = ai_decisions_merged[
        (ai_decisions_merged["status"] == "CONFIDENT")
        | (
            (ai_decisions_merged["status"] == "NEEDS_AI")
            & (ai_decisions_merged["ai_LAMAS_id"].astype(str) != "None")
            & (ai_decisions_merged["ai_LAMAS_id"].astype(str) != "nan")
        )
    ].copy()

    final_mapping_df["final_LAMAS_id"] = final_mapping_df.apply(
        lambda row: row["best_LAMAS_id"]
        if row["status"] == "CONFIDENT"
        else row["ai_LAMAS_id"],
        axis=1,
    ).astype(str)

    diagnostic_cols = [
        "osm_id",
        "status",
        "best_score",
        "best_LAMAS_name",
        "all_candidates",
        "ai_LAMAS_id",
        "ai_reasoning",
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

    save_intermediate_df(diagnostic_df_full, "diagnostic_report", file_identifier)
    osm_gdf_final = osm_gdf.merge(
        diagnostic_df_full[["osm_id", "final_LAMAS_id"]], on="osm_id", how="left"
    )

    logger.info("PIPELINE COMPLETED")
    logger.info("Total OSM streets considered: %s", len(osm_gdf_final))
    matched_count = final_mapping_df.shape[0]
    unmatched_count = len(osm_gdf_final) - matched_count
    logger.info(
        "Successfully matched (Confident + AI): %s | Unmatched: %s",
        matched_count,
        unmatched_count,
    )

    logger.info("[Step 6.5/7] Calculating diagnostic summary...")
    osm_city_label = osm_gdf["city"].iloc[0]
    lamas_in_city_df = LAMAS_df[LAMAS_df["city"] == osm_city_label]
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
    )
    sys.exit(0 if status == PipelineStatus.SUCCESS else 1)
