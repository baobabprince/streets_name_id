#!/usr/bin/env python3
"""Build an offline, reviewable Mazal entity registry (no street-ID changes)."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mazal import MazalClient, build_registry, name_key, write_json


def page_names(directory: Path):
    """Read structured records, not legacy tooltip strings joining two names."""
    for page in sorted(directory.glob("*.html")):
        match = re.search(r'<script id="street-records"[^>]*>(.*?)</script>',
                          page.read_text(encoding="utf-8"), re.S)
        if match:
            records = json.loads(match[1])
            for record in records:
                if isinstance(record, list) and len(record) >= 2 and record[1]:
                    yield record[1]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", action="append", default=[], help="Repeat for each street-name variant")
    parser.add_argument("--html-dir", type=Path, help="Also read names from structured settlement pages")
    parser.add_argument("--output", type=Path, default=Path("data/mazal_entities.json"))
    parser.add_argument("--cache", type=Path, default=Path("cache/mazal/lookups.json"))
    parser.add_argument("--max-requests", type=int, default=50)
    parser.add_argument("--max-names", type=int, default=50)
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args(argv)
    key = os.environ.get("MAZAL_API_KEY", "").strip()
    if not key:
        parser.error("Set MAZAL_API_KEY in the environment (never pass it on the command line)")
    if args.max_names < 1 or args.max_requests < 1 or args.delay < 0:
        parser.error("Limits must be positive and delay must be nonnegative")
    if args.html_dir and not args.html_dir.is_dir():
        parser.error("--html-dir must be an existing directory")
    names = [n.strip() for n in args.name if n.strip()]
    if args.html_dir:
        names.extend(page_names(args.html_dir))
    # Explicit --name arguments get priority over page scan. Keep all variants
    # of the first max-names canonical names, useful for the grouping report.
    selected, keys = [], set()
    for name in dict.fromkeys(names):
        nk = name_key(name)
        if nk in keys or len(keys) < args.max_names:
            keys.add(nk)
            selected.append(name)
    if not selected:
        parser.error("Supply --name or --html-dir with structured street records")
    client = MazalClient(key, args.cache, max_requests=args.max_requests, delay=args.delay)
    registry = build_registry(selected, client)
    write_json(args.output, registry)
    print(json.dumps({"names": len(registry["entries"]), "requests": client.requests_used,
                      "statistics": registry["statistics"]}, ensure_ascii=False))
    # A successful HTTP response in an unknown schema must not look like a
    # successful empty enrichment. Persist the report, but fail the pilot.
    failures = {"http_error", "api_error", "network_error", "schema_error", "budget_exhausted", "truncated"}
    return int(bool(failures.intersection(registry["statistics"])))


if __name__ == "__main__":
    raise SystemExit(main())
