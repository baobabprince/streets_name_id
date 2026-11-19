#!/usr/bin/env python3
"""Prioritise settlement-identification problems from the match report.

The 83 official cities usually resolve from the local PBF index.  The hard
cases are the long tail of LAMAS settlements: Bedouin tribes, moshavim,
small villages, camps, schools and places OSM maps as a neighbourhood or
``landuse=residential``.  This script turns the audit CSV into:

* ``settlement_match_audit.csv`` — one row per settlement with issue flags,
  priority and a suggested next action;
* ``settlement_match_audit.md`` — counts plus the top examples per issue;
* ``settlement_match_retry.txt`` — names ready for
  ``python run_all.py --only-file ... --refresh-matches``.

It does not call Nominatim/Overpass and never changes the match store.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Sequence

from rapidfuzz import fuzz

from settlement_names import canonical_key

DEFAULT_REPORT = os.path.join("batch_reports", "settlement_match_report.csv")
DEFAULT_OUT_DIR = "batch_reports"

P1_ISSUES = {
    "unresolved_non_settlement",
    "unresolved_nominatim",
    "unresolved_offline",
    "unresolved_other",
    "duplicate_osm_id",
    "inside_another_city",
    "empty_osm_type",
    "pinned_not_found",
}
P2_ISSUES = {
    "nominatim_fallback",
    "fuzzy_name",
    "suburb_candidates_ignored",
    "display_name_mismatch",
    "far_from_center",
}
P3_ISSUES = {
    "pinned_override",
    "institution_or_camp",
    "missing_center_distance",
}

ACTIONS: Dict[str, str] = {
    "unresolved_non_settlement": "add curated alias or verified osm_id_override",
    "unresolved_nominatim": "add alias / check Arabic-Hebrew spelling",
    "unresolved_offline": "add alias or rebuild national places cache",
    "unresolved_other": "manual review of rejection reason",
    "duplicate_osm_id": "split duplicate OSM ids or add alias",
    "inside_another_city": "verify Nominatim address hierarchy / boundary",
    "empty_osm_type": "rebuild places cache with typed OSM ids",
    "pinned_not_found": "fix or remove stale osm_id_override",
    "nominatim_fallback": "verify and promote to an offline alias",
    "fuzzy_name": "verify spelling and add curated alias",
    "suburb_candidates_ignored": "verify a whole-settlement boundary exists",
    "display_name_mismatch": "manual review of matched OSM object",
    "far_from_center": "verify LAMAS centre or matched coordinates",
    "pinned_override": "audit verified pin",
    "institution_or_camp": "verify LAMAS settlement type / OSM display name",
    "missing_center_distance": "add LAMAS centre for geographic validation",
}

SUSPICIOUS_DISPLAY_RE = re.compile(
    r"מחנה|בית\s*ספר|בי\"ס|חווה|מאחז|שכונת|קמפוס|כלא|בסיס|מוסד",
    re.IGNORECASE,
)


def _parse_float(value: Any):
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _duplicate_osm_ids(rows: Sequence[Dict[str, str]]) -> set[str]:
    counts = Counter(
        f"{row.get('osm_type', '')}:{row.get('osm_id', '')}"
        for row in rows
        if (row.get("status") or "").strip().lower() == "matched"
        and str(row.get("osm_id", "")).strip()
    )
    return {key for key, count in counts.items() if count > 1 and key != ":"}


def classify_row(row: Dict[str, str], duplicate_ids: set[str]) -> List[str]:
    """Return issue flags for one match-report row."""
    status = (row.get("status") or "").strip().lower()
    source = (row.get("source") or "").strip().lower()
    reason = row.get("reason") or ""
    validation = row.get("validation_message") or ""
    display_name = row.get("display_name") or ""
    settlement = row.get("settlement") or ""
    osm_type = (row.get("osm_type") or "").strip()
    osm_id = (row.get("osm_id") or "").strip()
    name_score = _parse_float(row.get("name_score"))
    distance = _parse_float(row.get("center_distance_m"))
    lower_reason = reason.lower()
    lower_validation = validation.lower()

    issues: List[str] = []
    if status != "matched":
        if "non-settlement" in lower_reason:
            issues.append("unresolved_non_settlement")
        elif "duplicate osm id" in lower_reason:
            issues.append("duplicate_osm_id")
        elif "pinned osm id" in lower_reason:
            issues.append("pinned_not_found")
        elif "nominatim" in lower_reason or "no valid" in lower_reason:
            issues.append("unresolved_nominatim")
        elif "local osm" in lower_reason or "not found" in lower_reason:
            issues.append("unresolved_offline")
        else:
            issues.append("unresolved_other")
        return issues

    if source == "nominatim":
        issues.append("nominatim_fallback")
    elif source == "pinned":
        issues.append("pinned_override")
    if name_score is not None and name_score < 100:
        issues.append("fuzzy_name")
    if not osm_type:
        issues.append("empty_osm_type")
    if distance is None:
        issues.append("missing_center_distance")
    elif distance > 25_000:
        issues.append("far_from_center")
    if "ignored" in lower_validation and "non-settlement" in lower_validation:
        issues.append("suburb_candidates_ignored")
    if "inside another" in lower_validation or "inside another" in lower_reason:
        issues.append("inside_another_city")
    if f"{osm_type}:{osm_id}" in duplicate_ids:
        issues.append("duplicate_osm_id")

    display_head = display_name.split(",")[0].strip()
    if display_head and settlement:
        head_key, settlement_key = canonical_key(display_head), canonical_key(settlement)
        if head_key and settlement_key and head_key != settlement_key:
            if fuzz.ratio(head_key, settlement_key) < 92:
                issues.append("display_name_mismatch")
    if SUSPICIOUS_DISPLAY_RE.search(display_name):
        issues.append("institution_or_camp")
    return list(dict.fromkeys(issues))


def _priority(issues: Iterable[str]) -> str:
    issues = set(issues)
    if issues & P1_ISSUES:
        return "P1"
    if issues & P2_ISSUES:
        return "P2"
    if issues & P3_ISSUES:
        return "P3"
    return "OK"


def _suggested_action(issues: Iterable[str]) -> str:
    actions = [ACTIONS.get(issue, "manual_review") for issue in dict.fromkeys(issues)]
    return "; ".join(dict.fromkeys(actions)) if actions else "ok"


def build_audit(rows: Sequence[Dict[str, str]]) -> List[Dict[str, str]]:
    duplicate_ids = _duplicate_osm_ids(rows)
    audit: List[Dict[str, str]] = []
    for row in rows:
        issues = classify_row(row, duplicate_ids)
        enriched = dict(row)
        enriched["issues"] = "|".join(issues)
        enriched["priority"] = _priority(issues)
        enriched["suggested_action"] = _suggested_action(issues)
        audit.append(enriched)
    return audit


def _write_csv(path: str, audit: Sequence[Dict[str, str]]) -> None:
    fieldnames: List[str] = []
    for row in audit:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(audit)


def _write_markdown(
    path: str,
    audit: Sequence[Dict[str, str]],
    top: int,
) -> None:
    status_counts = Counter(row.get("status", "") for row in audit)
    source_counts = Counter(row.get("source", "") or "-" for row in audit)
    priority_counts = Counter(row.get("priority", "") for row in audit)
    issue_counts: Counter[str] = Counter()
    examples: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in audit:
        for issue in filter(None, row.get("issues", "").split("|")):
            issue_counts[issue] += 1
            if len(examples[issue]) < top:
                examples[issue].append(row)

    lines = [
        "# Settlement matching audit",
        "",
        f"Rows: **{len(audit)}** · "
        f"matched: **{status_counts.get('matched', 0)}** · "
        f"unresolved: **{status_counts.get('unresolved', 0)}**",
        "",
        "## Sources",
        "",
    ]
    for source, count in sorted(source_counts.items()):
        lines.append(f"- `{source or '-'}`: {count}")
    lines += ["", "## Priority", ""]
    for priority in ("P1", "P2", "P3", "OK"):
        lines.append(f"- {priority}: {priority_counts.get(priority, 0)}")
    lines += ["", "## Issues", ""]
    if not issue_counts:
        lines.append("No issues detected.")
    for issue, count in issue_counts.most_common():
        lines += [
            "",
            f"### `{issue}` — {count}",
            "",
            f"Suggested: {ACTIONS.get(issue, 'manual_review')}",
            "",
            "| Settlement | Status | Source | Reason |",
            "|---|---|---|---|",
        ]
        for row in examples[issue]:
            reason = (row.get("reason") or row.get("validation_message") or "").replace("|", "\\|")
            lines.append(
                f"| {row.get('settlement', '')} | {row.get('status', '')} | "
                f"{row.get('source', '') or '-'} | {reason[:160]} |"
            )
    lines.append("")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def _write_retry_file(path: str, audit: Sequence[Dict[str, str]]) -> int:
    names = [
        row.get("settlement", "")
        for row in audit
        if row.get("priority") in {"P1", "P2"} and row.get("settlement")
    ]
    names = list(dict.fromkeys(names))
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("# Generated by evaluate_settlement_matching.py\n")
        handle.write("# Use with: python run_all.py --only-file <this file> --refresh-matches\n")
        for name in names:
            handle.write(f"{name}\n")
    return len(names)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Audit settlement identification problems, focusing on the long tail of LAMAS settlements."
    )
    parser.add_argument("--report", default=DEFAULT_REPORT, help="settlement_match_report.csv path")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="directory for audit outputs")
    parser.add_argument("--top", type=int, default=12, help="examples per issue in the Markdown report")
    args = parser.parse_args(argv)

    if not os.path.exists(args.report):
        print(f"Report not found: {args.report}", file=sys.stderr)
        return 1
    with open(args.report, "r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        print(f"Report is empty: {args.report}", file=sys.stderr)
        return 1

    os.makedirs(args.out_dir, exist_ok=True)
    audit = build_audit(rows)
    audit_csv = os.path.join(args.out_dir, "settlement_match_audit.csv")
    audit_md = os.path.join(args.out_dir, "settlement_match_audit.md")
    retry_txt = os.path.join(args.out_dir, "settlement_match_retry.txt")
    _write_csv(audit_csv, audit)
    _write_markdown(audit_md, audit, args.top)
    retry_count = _write_retry_file(retry_txt, audit)

    priority_counts = Counter(row["priority"] for row in audit)
    print(
        f"Audited {len(audit)} settlements: "
        f"P1={priority_counts.get('P1', 0)}, "
        f"P2={priority_counts.get('P2', 0)}, "
        f"P3={priority_counts.get('P3', 0)}, "
        f"OK={priority_counts.get('OK', 0)}"
    )
    print(f"CSV:   {audit_csv}")
    print(f"MD:    {audit_md}")
    print(f"Retry: {retry_txt} ({retry_count} names)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
