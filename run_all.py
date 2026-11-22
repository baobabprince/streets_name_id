#!/usr/bin/env python3
"""One command for the entire streets_name_id flow.

    python run_all.py                     # fetch -> match -> process -> report -> dashboard
    python run_all.py --cities --workers 4 --use-ai
    python run_all.py --non-cities --match-only --refresh-matches
    python run_all.py --only-file batch_reports/settlement_match_retry.txt --force
    python run_all.py --refresh-all       # refresh every source and reprocess
    python run_all.py --fast              # no AI/HTML/dashboard/intermediates
    python run_all.py --match-only        # rebuild the settlement match map
    python run_all.py --dashboard-only    # rebuild index.html

All per-stage work lives in :mod:`batch_process_settlements`; this script is
the single, strict entry point used by CI and recommended for local runs.
"""

from __future__ import annotations

import sys
import time
from typing import Any, Dict, Optional, Sequence

from batch_process_settlements import build_cli_parser, run_everything


def build_parser():
    parser = build_cli_parser(
        prog="run_all.py",
        description="Fetch, match and process every LAMAS settlement in one run",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Preset: no AI, no HTML/dashboard and no intermediate pickles",
    )
    parser.add_argument(
        "--refresh-all",
        action="store_true",
        help="Preset: refresh all sources and force reprocessing of every settlement",
    )
    return parser


def _apply_presets(args) -> None:
    if getattr(args, "refresh_all", False):
        args.refresh = True
        args.refresh_matches = True
        args.force = True
    if getattr(args, "fast", False):
        args.use_ai = False
        args.skip_html = True
        args.skip_dashboard = True
        args.no_intermediates = True


def _exit_code(summary: Optional[Dict[str, Any]]) -> int:
    if not summary:
        return 1
    stats = summary.get("statistics")
    if not stats:
        return 0
    total = stats.get("total_settlements", 0)
    completed = stats.get("successful", 0) + stats.get("skipped_already_processed", 0)
    if total and completed < total:
        print(
            f"\nOnly {completed}/{total} settlements completed; "
            "see batch_reports/ for details.",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _apply_presets(args)

    started = time.time()
    try:
        summary = run_everything(args)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    matching = summary.get("settlement_matching") or {}
    if matching:
        print(
            "\nSettlements: "
            f"{matching.get('matched', 0)}/{matching.get('requested', 0)} matched "
            f"({matching.get('matched_offline', 0)} offline, "
            f"{matching.get('matched_nominatim', 0)} Nominatim, "
            f"{matching.get('reused_from_store', 0)} cached, "
            f"{matching.get('unresolved', 0)} unresolved)"
        )
    if summary.get("statistics"):
        stats = summary["statistics"]
        print(
            f"Pipelines: {stats['successful']} successful, "
            f"{stats['failed_pipeline']} failed, "
            f"{stats['skipped_already_processed']} skipped"
        )
    print(f"✓ Full run finished in {time.time() - started:.1f}s")
    return _exit_code(summary)


if __name__ == "__main__":
    sys.exit(main())
