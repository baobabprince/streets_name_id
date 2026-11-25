#!/usr/bin/env python3
"""Batch settlement processing and one-shot orchestration.

The whole project is run in a single pass:

1. **Acquire once** — LAMAS street table (cached) and the national OSM PBF
   caches (streets / boundaries / places).
2. **Match once** — every LAMAS settlement is resolved against the local OSM
   name index with geographic validation; only unresolved names go to
   Nominatim.  Results are persisted in one consolidated JSON store.
3. **Process** — street pipelines run per settlement (optionally in parallel),
   reusing the resolution from step 2. Already processed settlements are
   skipped, and state is written to disk once at the end.
4. **Report** — one batch summary (JSON + text), one settlement-match CSV and
   one dashboard refresh.

Run `python run_all.py` for the complete flow, or this module directly with
the same flags.
"""

import argparse
import concurrent.futures
import json
import os
import sys
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from data_loader import load_or_fetch_LAMAS, normalize_city
from settlement_index import (
    REPORT_PATH as MATCH_REPORT_PATH,
    SettlementMatchStore,
    SettlementResolver,
    write_match_report,
)
from settlement_matcher import SettlementMatcher
from settlement_names import canonical_key, settlement_identity_key
from pipeline import NO_DATA_STATUSES, PipelineStatus, run_pipeline

REPORTS_DIR = os.path.join(os.path.dirname(__file__), "batch_reports")
OFFICIAL_CITIES_FILE = os.path.join(
    os.path.dirname(__file__), "config", "israel_83_cities_lamas.json"
)
NO_DATA_REPORT_FILE = os.path.join(REPORTS_DIR, "no_data_settlements.txt")
os.makedirs(REPORTS_DIR, exist_ok=True)


def filter_to_official_cities(settlements: List[str]) -> List[str]:
    """Select the configured 83 cities and fail if the LAMAS data is incomplete."""
    with open(OFFICIAL_CITIES_FILE, "r", encoding="utf-8") as file:
        city_list = json.load(file)

    expected_count = city_list.get("count")
    target_cities = city_list.get("cities")
    if (
        expected_count != 83
        or not isinstance(target_cities, list)
        or len(target_cities) != expected_count
        or len(set(target_cities)) != expected_count
    ):
        raise ValueError(f"Invalid official city list in {OFFICIAL_CITIES_FILE}")

    normalized_targets = set(normalize_city(pd.Series(target_cities)).tolist())
    normalized_settlements = normalize_city(pd.Series(settlements)).tolist()
    available_targets = set(normalized_settlements)
    missing = sorted(normalized_targets - available_targets)
    if missing:
        raise ValueError(
            "LAMAS data is missing official city names: " + ", ".join(missing)
        )

    filtered = [
        settlement
        for settlement, normalized in zip(settlements, normalized_settlements)
        if normalized in normalized_targets
    ]
    if len(filtered) != expected_count:
        raise ValueError(
            f"Expected {expected_count} unique official cities in LAMAS data, "
            f"found {len(filtered)}"
        )
    return filtered


def worker_wrapper(
    settlement_name: str,
    place: str | Dict[str, Any],
    use_ai: bool = False,
    skip_html: bool = False,
    use_pbf: bool = True,
    save_intermediates: bool = True,
    lamas_df: Optional[pd.DataFrame] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Run one settlement pipeline and return a structured result record."""
    start_time = time.time()
    result = {
        "settlement": settlement_name,
        "status": "unknown",
        "message": "",
        "pipeline_success": False,
        "duration_seconds": 0.0,
        "error_type": None,
        "error_details": None,
    }

    try:
        print(f"  [Worker] Starting pipeline for {settlement_name}...")
        pipeline_result = run_pipeline(
            place=place,
            force_refresh=force_refresh,
            use_ai=use_ai,
            skip_html=skip_html,
            output_name=settlement_name,
            use_pbf=use_pbf,
            lamas_df=lamas_df,
            lamas_city=settlement_name,
            save_intermediates=save_intermediates,
        )

        result["pipeline_success"] = pipeline_result == PipelineStatus.SUCCESS
        if result["pipeline_success"]:
            result["status"] = "success"
            result["message"] = "Pipeline completed successfully"
        elif pipeline_result in NO_DATA_STATUSES:
            # Completed correctly, but there is nothing to map (no named OSM
            # streets / no street-level LAMAS records) — not a failure.
            result["pipeline_success"] = True
            result["status"] = "no_data"
            result["message"] = (
                f"Pipeline returned {pipeline_result.value}: nothing to map"
            )
        else:
            result["status"] = "failed_pipeline"
            pipeline_status = getattr(pipeline_result, "value", pipeline_result)
            result["message"] = f"Pipeline returned {pipeline_status}"
            result["error_type"] = "PipelineReturnedNonSuccess"
    except Exception as e:
        error_trace = traceback.format_exc()
        result.update({
            "status": "failed_pipeline",
            "message": f"Pipeline exception: {e}",
            "error_type": type(e).__name__,
            "error_details": error_trace,
            "pipeline_success": False,
        })
        print(f"  [Worker] ERROR in {settlement_name}: {type(e).__name__}: {e}")

    result["duration_seconds"] = time.time() - start_time
    return result


class BatchProcessor:
    """Process settlements using a pre-computed settlement resolution map."""

    def __init__(
        self,
        use_ai: bool = False,
        skip_html: bool = False,
        quiet: bool = False,
        workers: int = 1,
        use_pbf: bool = True,
        save_intermediates: bool = True,
        matches: Optional[Dict[str, Dict[str, Any]]] = None,
        unresolved: Optional[Dict[str, str]] = None,
        matching_stats: Optional[Dict[str, Any]] = None,
        lamas_df: Optional[pd.DataFrame] = None,
        force_refresh: bool = False,
    ):
        self.use_ai = use_ai
        self.skip_html = skip_html
        self.quiet = quiet
        self.workers = max(1, workers)
        self.use_pbf = use_pbf
        self.save_intermediates = save_intermediates
        self.matches = matches or {}
        self.unresolved = unresolved or {}
        self.matching_stats = matching_stats or {}
        self.lamas_df = lamas_df
        self.force_refresh = force_refresh
        self.matcher = SettlementMatcher()

        self.stats = {
            "total_settlements": 0,
            "matched": 0,
            "failed_nominatim": 0,
            "failed_validation": 0,
            "failed_pipeline": 0,
            "no_data": 0,
            "skipped_already_processed": 0,
            "successful": 0,
        }
        self.results: List[Dict[str, Any]] = []
        self.processed_settlements_file = os.path.join(
            REPORTS_DIR, "processed_settlements.json"
        )
        self.processed_settlements = self._load_processed_settlements()

    def _load_processed_settlements(self) -> set:
        if os.path.exists(self.processed_settlements_file):
            try:
                with open(self.processed_settlements_file, "r", encoding="utf-8") as f:
                    return set(json.load(f).get("settlements", []))
            except Exception as e:
                print(f"Warning: Failed to load processed settlements: {e}")
        return set()

    def _save_processed_settlement(self, settlement_name: str) -> None:
        """Update state in memory; :meth:`flush` writes the file once per run."""
        self.processed_settlements.add(settlement_name)

    def flush_processed_settlements(self) -> None:
        try:
            with open(self.processed_settlements_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "settlements": sorted(self.processed_settlements),
                        "last_updated": datetime.now().isoformat(),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
        except Exception as e:
            print(f"Warning: Failed to save processed settlements: {e}")

    def get_unique_settlements(self, lamas_df: pd.DataFrame) -> List[str]:
        """Extract unique settlement names from LAMAS data."""
        if "city" not in lamas_df.columns:
            print("Error: LAMAS data doesn't have 'city' column")
            return []
        settlements = sorted(lamas_df["city"].dropna().unique().tolist())
        print(f"\nFound {len(settlements)} unique settlements in LAMAS data")
        return settlements

    def resolve_settlement(self, settlement_name: str) -> Dict[str, Any]:
        """Resolve one name (kept for interactive use / backward compatibility)."""
        result = {
            "settlement": settlement_name,
            "status": "unknown",
            "message": "",
            "match": None,
            "timestamp": datetime.now().isoformat(),
        }
        place = self.matches.get(settlement_name)
        if place:
            result["status"] = "ready_for_pipeline"
            result["match"] = place
            result["message"] = f"Settlement store ({place.get('match_source', 'cached')})"
            return result
        if settlement_name in self.unresolved:
            result["status"] = "failed_nominatim"
            result["message"] = self.unresolved[settlement_name]
            return result

        # Only reached when the caller did not pre-resolve the whole list.
        match = self.matcher.search_settlement(settlement_name)
        if not match:
            result["status"] = "failed_nominatim"
            result["message"] = "No valid Nominatim match found"
            return result
        if not match.is_valid:
            result["status"] = "failed_validation"
            result["message"] = match.validation_message
            result["match"] = {
                "display_name": match.display_name,
                "lat": match.lat,
                "lon": match.lon,
            }
            return result
        print(f"  ✓ Valid match found for '{settlement_name}': {match.display_name}")
        result["status"] = "ready_for_pipeline"
        result["match"] = match.to_place_dict()
        return result

    def _resolution(self, settlement: str) -> Dict[str, Any]:
        result = self.resolve_settlement(settlement)
        source = (result.get("match") or {}).get("match_source", "unknown")
        result["match_source"] = source
        return result

    def _worker_kwargs(self) -> Dict[str, Any]:
        return dict(
            use_ai=self.use_ai,
            skip_html=self.skip_html,
            use_pbf=self.use_pbf,
            save_intermediates=self.save_intermediates,
            force_refresh=self.force_refresh,
        )

    def _update_no_data_file(
        self, settlements: List[str], path: Optional[str] = None
    ) -> Optional[str]:
        """Auto-document settlements with nothing to map (union with existing entries).

        ``build_no_results.py`` reads ``batch_reports/no_data_settlements.txt``
        to categorize settlements without a page as "no named streets" instead
        of "pending".  Existing (manual) entries are preserved.
        """
        if not settlements:
            return None
        if path is None:
            path = NO_DATA_REPORT_FILE
        existing = set()
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    existing = {
                        line.strip()
                        for line in handle
                        if line.strip() and not line.startswith("##")
                    }
            except OSError:
                existing = set()
        merged = sorted(existing | set(settlements))
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "## Auto-generated by batch_process_settlements: settlements whose\n"
                    "## pipeline ended with NO_DATA / NO_LAMAS_STREETS (nothing to map).\n"
                    f"## Updated: {datetime.now().isoformat()}\n"
                )
                for name in merged:
                    handle.write(f"{name}\n")
        except OSError as exc:
            print(f"Warning: failed to update the no-data report: {exc}")
            return None
        return path

    def generate_summary_report(self, output_file: Optional[str] = None):
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(REPORTS_DIR, f"batch_summary_{timestamp}.json")

        no_data_settlements = [
            result["settlement"]
            for result in self.results
            if result.get("status") == "no_data"
        ]
        no_data_file = self._update_no_data_file(no_data_settlements)

        summary = {
            "timestamp": datetime.now().isoformat(),
            "statistics": self.stats,
            "settlement_matching": self.matching_stats,
            "no_data_settlements": no_data_settlements,
            "no_data_report": no_data_file,
            "success_rate": (
                f"{(self.stats['successful'] / self.stats['total_settlements'] * 100):.1f}%"
                if self.stats["total_settlements"] > 0
                else "0%"
            ),
            "results": self.results,
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(f"\n{'=' * 70}")
        print("BATCH PROCESSING SUMMARY")
        print(f"{'=' * 70}")
        print(f"Total settlements: {self.stats['total_settlements']}")
        print(
            f"Successful: {self.stats['successful']} "
            f"({self.stats['successful'] / self.stats['total_settlements'] * 100:.1f}%)"
            if self.stats["total_settlements"] > 0
            else "Successful: 0"
        )
        print(f"Matched in OSM: {self.stats['matched']}")
        print(f"Failed settlement matching: {self.stats['failed_nominatim']}")
        print(f"Failed validation: {self.stats['failed_validation']}")
        print(f"Failed pipeline: {self.stats['failed_pipeline']}")
        print(f"No data (nothing to map): {self.stats['no_data']}")
        if no_data_file:
            print(f"No-data report updated: {no_data_file}")
        print(f"Skipped (already processed): {self.stats['skipped_already_processed']}")
        if self.matching_stats:
            print(
                "Settlement sources: "
                f"{self.matching_stats.get('matched_offline', 0)} offline / "
                f"{self.matching_stats.get('matched_nominatim', 0)} Nominatim / "
                f"{self.matching_stats.get('reused_from_store', 0)} cached"
            )
        print(f"\nDetailed report saved to: {output_file}")

        text_report_file = output_file.replace(".json", ".txt")
        self._generate_text_report(text_report_file)
        print(f"Text report saved to: {text_report_file}")
        return summary

    def _generate_text_report(self, output_file: str) -> None:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("BATCH SETTLEMENT PROCESSING REPORT\n")
            f.write("=" * 70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("STATISTICS\n" + "-" * 70 + "\n")
            for key, value in self.stats.items():
                f.write(f"{key}: {value}\n")
            if self.matching_stats:
                f.write("\nSETTLEMENT MATCHING\n" + "-" * 70 + "\n")
                for key, value in self.matching_stats.items():
                    f.write(f"{key}: {value}\n")

            by_status: Dict[str, List[Dict[str, Any]]] = {}
            for result in self.results:
                by_status.setdefault(result["status"], []).append(result)
            if "success" in by_status:
                f.write("\nSUCCESSFUL SETTLEMENTS\n" + "-" * 70 + "\n")
                for result in by_status["success"]:
                    f.write(f"✓ {result['settlement']}\n")
                    if result.get("match"):
                        f.write(f"  → {result['match']['display_name']}\n")
            if "no_data" in by_status:
                f.write(
                    "\nNO DATA SETTLEMENTS (completed; nothing to map)\n"
                    + "-" * 70
                    + "\n"
                )
                for result in by_status["no_data"]:
                    f.write(f"○ {result['settlement']}\n")
                    f.write(f"  → {result.get('message', '')}\n")
            for status in ("failed_nominatim", "failed_validation", "failed_pipeline"):
                if status in by_status:
                    f.write(f"\n{status.upper().replace('_', ' ')}\n" + "-" * 70 + "\n")
                    for result in by_status[status]:
                        f.write(f"✗ {result['settlement']}\n")
                        f.write(f"  → {result['message']}\n")
                        if result.get("match"):
                            f.write(f"  → Match: {result['match']['display_name']}\n")

    def run_batch(
        self,
        settlements: List[str],
        limit: Optional[int] = None,
        force: bool = False,
        dry_run: bool = False,
    ):
        """Resolve once, then run pipelines (optionally in parallel)."""
        if limit:
            settlements = settlements[:limit]
        self.stats["total_settlements"] = len(settlements)

        if dry_run:
            print(f"\n{'=' * 70}")
            print("DRY RUN MODE - No actual processing will occur")
            print(f"{'=' * 70}")
            print(f"\nWould process {len(settlements)} settlements:")
            for i, settlement in enumerate(settlements, 1):
                if settlement in self.unresolved:
                    status = "UNMATCHED"
                elif settlement in self.processed_settlements and not force:
                    status = "SKIP"
                else:
                    status = "PROCESS"
                print(f"  {i:3d}. [{status}] {settlement}")
            return

        print(f"\n{'=' * 70}")
        print(f"Starting batch processing of {len(settlements)} settlements")
        print(f"Workers: {self.workers}")
        print(f"AI Resolution: {'ENABLED' if self.use_ai else 'DISABLED'}")
        print(f"{'=' * 70}")

        tasks: List[tuple[str, Dict[str, Any]]] = []
        for i, settlement in enumerate(settlements, 1):
            print(f"\n[{i}/{len(settlements)}] Processing: {settlement}")
            if not force and settlement in self.processed_settlements:
                print("  ⏭ Skipping - already processed")
                self.stats["skipped_already_processed"] += 1
                self.results.append({
                    "settlement": settlement,
                    "status": "skipped",
                    "message": "Already processed",
                })
                continue

            resolve_result = self._resolution(settlement)
            if resolve_result["status"] != "ready_for_pipeline":
                self.results.append(resolve_result)
                if resolve_result["status"] == "failed_nominatim":
                    self.stats["failed_nominatim"] += 1
                elif resolve_result["status"] == "failed_validation":
                    self.stats["failed_validation"] += 1
                continue

            self.stats["matched"] += 1
            tasks.append((settlement, resolve_result))

        def execute(settlement: str, resolve_result: Dict[str, Any]) -> Dict[str, Any]:
            kwargs = self._worker_kwargs()
            if self.workers == 1:
                # Reuse the already-fetched LAMAS table in this process.
                kwargs["lamas_df"] = self.lamas_df
                return worker_wrapper(
                    settlement, resolve_result["match"], **kwargs
                )
            return worker_wrapper(settlement, resolve_result["match"], **kwargs)

        results_by_settlement: Dict[str, Dict[str, Any]] = {}
        try:
            if self.workers == 1:
                for settlement, resolve_result in tasks:
                    results_by_settlement[settlement] = execute(
                        settlement, resolve_result
                    )
            else:
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=self.workers
                ) as executor:
                    future_map = {
                        executor.submit(
                            worker_wrapper,
                            settlement,
                            resolve_result["match"],
                            **self._worker_kwargs(),
                        ): settlement
                        for settlement, resolve_result in tasks
                    }
                    print(f"\n{'=' * 70}\nWaiting for pending pipeline tasks...\n{'=' * 70}")
                    for future in concurrent.futures.as_completed(future_map):
                        settlement = future_map[future]
                        try:
                            results_by_settlement[settlement] = future.result()
                        except Exception as e:
                            results_by_settlement[settlement] = {
                                "settlement": settlement,
                                "status": "failed_pipeline",
                                "message": f"Worker exception: {e}",
                                "pipeline_success": False,
                                "duration_seconds": 0.0,
                                "error_type": type(e).__name__,
                                "error_details": traceback.format_exc(),
                            }

            for settlement, resolve_result in tasks:
                worker_result = results_by_settlement.get(settlement)
                if worker_result is None:
                    continue
                final_result = resolve_result.copy()
                final_result.update(worker_result)
                if worker_result["status"] == "success":
                    print(f"  ✓ Task completed: {settlement} (Success)")
                    self.stats["successful"] += 1
                    self._save_processed_settlement(settlement)
                elif worker_result["status"] == "no_data":
                    print(
                        f"  ✓ Task completed: {settlement} "
                        f"(No data: {worker_result['message']})"
                    )
                    self.stats["no_data"] += 1
                    # Completed correctly — documented in the no-data report,
                    # so later runs skip it (use --force to re-check).
                    self._save_processed_settlement(settlement)
                else:
                    print(
                        f"  ✗ Task completed: {settlement} "
                        f"(Failed: {worker_result['message']})"
                    )
                    self.stats["failed_pipeline"] += 1
                self.results.append(final_result)
        finally:
            # One write instead of one write per settlement.
            self.flush_processed_settlements()

        return self.generate_summary_report()


def build_cli_parser(**kwargs) -> argparse.ArgumentParser:
    kwargs.setdefault(
        "description", "Fetch, match and process every LAMAS settlement in one run"
    )
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all.py                         # everything, one command
  python run_all.py --cities --workers 4    # the 83 official cities
  python run_all.py --non-cities --match-only  # audit all other settlements
  python run_all.py --only-file batch_reports/settlement_match_retry.txt --force
  python run_all.py --dry-run --limit 10    # preview without processing
""",
        **kwargs,
    )
    parser.add_argument("--use-ai", action="store_true",
                        help="Enable Laya AI resolution for streets below 90%% fuzzy match")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="Limit processing to first N settlements (for testing)")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess settlements even if already processed")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without running pipelines")
    parser.add_argument("--skip-html", action="store_true",
                        help="Skip per-settlement HTML generation")
    parser.add_argument("--skip-dashboard", action="store_true",
                        help="Do not rebuild index.html at the end")
    parser.add_argument("--dashboard-only", action="store_true",
                        help="Only rebuild index.html")
    parser.add_argument("--match-only", action="store_true",
                        help="Only rebuild the consolidated settlement match map")
    parser.add_argument("--quiet", action="store_true",
                        help="Reduce console output")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    parser.add_argument("--cities", action="store_true",
                        help="Process only the 83 cities in config/israel_83_cities_lamas.json")
    parser.add_argument("--non-cities", action="store_true",
                        help="Process every LAMAS settlement except the 83 official cities")
    parser.add_argument("--only", metavar="NAMES",
                        help="Comma-separated settlement names to process")
    parser.add_argument("--only-file", metavar="PATH",
                        help="File with settlement names (one per line or comma-separated)")
    parser.add_argument("--no-pbf", action="store_true",
                        help="Disable local OSM PBF extraction and use live Overpass API")
    parser.add_argument("--refresh", action="store_true",
                        help="Refresh LAMAS, national OSM caches and settlement matches")
    parser.add_argument("--refresh-matches", action="store_true",
                        help="Ignore the settlement match store and re-resolve all names")
    parser.add_argument("--no-nominatim", action="store_true",
                        help="Never call Nominatim; rely on the local OSM index only")
    parser.add_argument("--no-intermediates", action="store_true",
                        help="Skip bulky intermediate pickles (CSV reports are kept)")
    return parser


def _official_city_keys() -> set[str]:
    """Identity keys of the 83 official cities, used by --non-cities."""
    with open(OFFICIAL_CITIES_FILE, "r", encoding="utf-8") as file:
        payload = json.load(file)
    cities = payload.get("cities", [])
    return {settlement_identity_key(city) for city in cities if city}


def _requested_name_keys(args) -> set[str]:
    """Union of --only and --only-file names, using settlement identity keys."""
    parts: List[str] = []
    only = getattr(args, "only", None)
    if only:
        parts.extend(only.split(","))
    path = getattr(args, "only_file", None)
    if path:
        with open(path, "r", encoding="utf-8-sig") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts.extend(line.split(","))
    return {
        settlement_identity_key(part) for part in parts if part and part.strip()
    }


def _select_settlements(lamas_df: pd.DataFrame, args) -> List[str]:
    settlements = sorted(lamas_df["city"].dropna().unique().tolist())
    cities = bool(getattr(args, "cities", False))
    non_cities = bool(getattr(args, "non_cities", False))
    if cities and non_cities:
        raise ValueError("--cities and --non-cities are mutually exclusive")
    if cities:
        settlements = filter_to_official_cities(settlements)
        print(f"Filtered to {len(settlements)} configured cities of Israel.")
    elif non_cities:
        before = len(settlements)
        official = _official_city_keys()
        settlements = [
            name for name in settlements if settlement_identity_key(name) not in official
        ]
        print(
            f"Excluded {before - len(settlements)} official cities; "
            f"{len(settlements)} other LAMAS settlements remain."
        )
    requested = _requested_name_keys(args)
    if requested:
        settlements = [
            name for name in settlements if settlement_identity_key(name) in requested
        ]
        print(f"Filtered to {len(settlements)} requested settlements.")
    limit = getattr(args, "limit", None)
    if limit:
        settlements = settlements[:limit]
    return settlements


def _prepare_osm_caches(args) -> None:
    if getattr(args, "no_pbf", False):
        return
    try:
        from osm_pbf_loader import prepare_national_caches

        caches = prepare_national_caches(force=getattr(args, "refresh", False))
        print(f"OSM national caches ready: {', '.join(sorted(caches))}")
    except Exception as exc:
        print(
            "Warning: could not build national OSM caches "
            f"({exc}); settlement matching will use existing caches/Nominatim."
        )


def _refresh_dashboard() -> None:
    try:
        from build_no_results import main as build_no_results_main
        from generate_index import generate_index_html

        print("Refreshing no_results.json…")
        build_no_results_main()
        print("Refreshing the national dashboard (index.html)…")
        generate_index_html()
    except Exception as exc:  # dashboard refresh must never fail the batch
        print(f"Warning: dashboard refresh failed: {exc}")


def run_everything(args) -> Dict[str, Any]:
    """Single-pass orchestration: acquire -> match -> process -> report."""
    if getattr(args, "dashboard_only", False):
        _refresh_dashboard()
        return {"dashboard_only": True}

    # Laya model is loaded in-process: keep workers=1 so it is reused once.
    if getattr(args, "use_ai", False) and getattr(args, "workers", 1) > 1:
        print("WARNING: Forcing workers=1 because Laya AI is enabled.")
        args.workers = 1

    print("Fetching LAMAS data (cached after the first run)...")
    lamas_df = load_or_fetch_LAMAS(force_refresh=getattr(args, "refresh", False))
    if lamas_df.empty:
        raise ValueError("Failed to fetch LAMAS data")
    settlements = _select_settlements(lamas_df, args)
    if not settlements:
        raise ValueError("No settlements found in LAMAS data")

    _prepare_osm_caches(args)

    print(f"Resolving {len(settlements)} settlements against OSM (offline first)...")
    store = SettlementMatchStore()
    resolver = SettlementResolver(
        store=store,
        use_pbf=not getattr(args, "no_pbf", False),
        allow_nominatim=not getattr(args, "no_nominatim", False),
    )
    resolution = resolver.resolve(
        settlements,
        refresh=getattr(args, "refresh", False) or getattr(args, "refresh_matches", False),
    )
    report_path = write_match_report(
        settlements, resolution.matches, resolution.unresolved, path=MATCH_REPORT_PATH
    )
    stats = resolution.stats
    print(
        f"Settlement matching: {stats.get('matched', 0)}/{len(settlements)} matched "
        f"({stats.get('matched_offline', 0)} offline, "
        f"{stats.get('matched_nominatim', 0)} Nominatim, "
        f"{stats.get('reused_from_store', 0)} cached, "
        f"{stats.get('unresolved', 0)} unresolved, "
        f"{stats.get('rejected_non_settlement', 0)} rejected as "
        f"suburb/neighbourhood). Report: {report_path}"
    )

    if getattr(args, "match_only", False):
        return {"settlement_matching": resolution.stats, "match_report": report_path}

    processor = BatchProcessor(
        use_ai=getattr(args, "use_ai", False),
        skip_html=getattr(args, "skip_html", False),
        quiet=getattr(args, "quiet", False),
        workers=getattr(args, "workers", 1),
        use_pbf=not getattr(args, "no_pbf", False),
        save_intermediates=not getattr(args, "no_intermediates", False),
        matches=resolution.matches,
        unresolved=resolution.unresolved,
        matching_stats=resolution.stats,
        lamas_df=lamas_df,
        force_refresh=getattr(args, "refresh", False),
    )
    summary = processor.run_batch(
        settlements=settlements,
        force=getattr(args, "force", False),
        dry_run=getattr(args, "dry_run", False),
    )

    if not summary:
        summary = {
            "dry_run": True,
            "settlement_matching": resolution.stats,
        }
    if (
        not getattr(args, "dry_run", False)
        and not getattr(args, "skip_html", False)
        and not getattr(args, "skip_dashboard", False)
    ):
        _refresh_dashboard()
    return summary


def main(argv=None) -> int:
    args = build_cli_parser().parse_args(argv)
    try:
        summary = run_everything(args)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print("\n✓ Run complete!")
    return 0 if summary is not None else 1


if __name__ == "__main__":
    sys.exit(main())
